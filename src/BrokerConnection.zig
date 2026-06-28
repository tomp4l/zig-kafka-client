const std = @import("std");
const Io = std.Io;

const Self = @This();

// correlation ID to request mapping
inflight_requests_mutex: std.Io.Mutex = .init,
write_mutex: std.Io.Mutex = .init,

inflight_requests: std.AutoHashMapUnmanaged(u32, *InFlightRequest) = .empty,
correlation_id: std.atomic.Value(u32) = .init(0),
read_future: ?Io.Future(void) = null,
read_error: ?anyerror = null,

reader: *Io.Reader,
writer: *Io.Writer,

pub fn init(reader: *Io.Reader, writer: *Io.Writer) Self {
    return .{
        .reader = reader,
        .writer = writer,
    };
}

pub fn connect(self: *Self, io: Io, allocator: std.mem.Allocator) !void {
    if (self.read_future != null) return error.AlreadyConnected;

    self.read_future = try io.concurrent(readResponses, .{ self, io, allocator });
}

pub fn close(self: *Self, io: Io, allocator: std.mem.Allocator) void {
    if (self.read_future) |*f| f.cancel(io);

    self.inflight_requests_mutex.lock(io) catch {
        return;
    };
    defer self.inflight_requests_mutex.unlock(io);

    self.inflight_requests.deinit(allocator);
}

const InFlightRequest = struct {
    request_state: std.atomic.Value(RequestState) = .init(.started),
    response: []const u8 = undefined,
    response_header_flexible: bool,
};

const RequestState = enum(u32) {
    started,
    completed,
    errored,
};

pub fn KafkaResponse(comptime T: type) type {
    return struct {
        value: T,
        raw_buffer: []const u8,
        allocator: std.mem.Allocator,

        pub fn deinit(self: *@This()) void {
            self.allocator.free(self.raw_buffer);
        }
    };
}

fn cleanOutstandingRequest(self: *Self, io: Io, correlation_id: u32) void {
    self.inflight_requests_mutex.lock(io) catch {
        return;
    };
    _ = self.inflight_requests.remove(correlation_id);
    self.inflight_requests_mutex.unlock(io);
}

// why is this special :(
const API_VERSION_KEY = 18;

pub fn makeRequest(self: *Self, ResponseType: type, io: Io, allocator: std.mem.Allocator, request: anytype) !KafkaResponse(ResponseType) {
    const RequestType = @TypeOf(request);
    var in_flight: InFlightRequest = .{ .response_header_flexible = RequestType.is_flexible and RequestType.api_key != API_VERSION_KEY };
    const correlation_id = self.getNextCorrelationId();
    {
        try self.inflight_requests_mutex.lock(io);
        defer self.inflight_requests_mutex.unlock(io);
        try self.inflight_requests.put(allocator, correlation_id, &in_flight);
    }
    // Ensure we always clean up if something goes wrong
    defer cleanOutstandingRequest(self, io, correlation_id);

    var discarding: Io.Writer.Discarding = .init(&.{});
    try request.serialise(&discarding.writer);
    const bytes = discarding.fullCount();

    // would need to consider client ID if not null
    const size = bytes + if (RequestType.is_flexible) 11 else 10;

    {
        try self.write_mutex.lock(io);
        defer self.write_mutex.unlock(io);

        try self.writer.writeInt(u32, @truncate(size), .big);
        try self.writer.writeInt(u16, RequestType.api_key, .big);
        try self.writer.writeInt(u16, RequestType.version, .big);
        try self.writer.writeInt(u32, correlation_id, .big);
        //Hard code null client ID for now
        try self.writer.writeInt(i16, -1, .big);
        if (RequestType.is_flexible) {
            try self.writer.writeByte(0x00);
        }
        try request.serialise(self.writer);
        try self.writer.flush();
    }

    // this can have timeout but worry later
    while (in_flight.request_state.load(.acquire) == .started)
        try io.futexWait(RequestState, &in_flight.request_state.raw, .started);
    const new_state = in_flight.request_state.load(.acquire);
    switch (new_state) {
        .started => unreachable,
        .completed => {},
        .errored => return error.ConnectionClosed,
    }

    return .{
        .allocator = allocator,
        .raw_buffer = in_flight.response,
        .value = try ResponseType.deserialise(allocator, in_flight.response),
    };
}

fn recordReadError(self: *Self, io: Io, err: anyerror, curr_request: ?*InFlightRequest) void {
    switch (err) {
        error.EndOfStream, error.ReadFailed, error.Cancelled => {},
        else => std.log.warn("got read error: {}", .{err}),
    }
    self.read_error = err;

    if (curr_request) |request| {
        request.request_state.store(.errored, .release);

        io.futexWake(RequestState, &request.request_state.raw, 1);
    }

    self.inflight_requests_mutex.lock(io) catch {
        if (self.inflight_requests.count() == 0) return;

        std.log.err("cancellation with in flight requests", .{});
        return;
    };
    defer self.inflight_requests_mutex.unlock(io);

    var it = self.inflight_requests.valueIterator();
    while (it.next()) |request| {
        request.*.request_state.store(.errored, .release);

        io.futexWake(RequestState, &request.*.request_state.raw, 1);
    }

    self.inflight_requests.clearRetainingCapacity();
}

fn readResponses(self: *Self, io: Io, allocator: std.mem.Allocator) void {
    while (true) {

        // read next message
        var size = self.reader.takeInt(u32, .big) catch |err| return self.recordReadError(io, err, null);
        const correlation_id = self.reader.takeInt(u32, .big) catch |err| return self.recordReadError(io, err, null);
        size -= 4;

        const request = lock: {
            self.inflight_requests_mutex.lock(io) catch |err| return self.recordReadError(io, err, null);
            defer self.inflight_requests_mutex.unlock(io);
            break :lock self.inflight_requests.fetchRemove(correlation_id);
        };

        if (request) |kv| {
            const r = kv.value;

            if (r.response_header_flexible) {
                const flexible = self.reader.takeByte() catch |err| return self.recordReadError(io, err, r);
                if (flexible != 0) return self.recordReadError(io, error.Unimplemented, r);
                size -= 1;
            }

            const response_body = allocator.alloc(u8, size) catch |err| return self.recordReadError(io, err, r);
            var finished = false;
            defer if (!finished) allocator.free(response_body);

            self.reader.readSliceAll(response_body) catch |err| return self.recordReadError(io, err, r);

            r.response = response_body;
            r.request_state.store(.completed, .release);
            io.futexWake(RequestState, &r.request_state.raw, 1);
            finished = true;
        } else {
            self.reader.discardAll(size) catch |err| return self.recordReadError(io, err, null);
        }
    }
}

fn getNextCorrelationId(self: *Self) u32 {
    return self.correlation_id.fetchAdd(1, .monotonic);
}

test "fake request / response" {
    const Pipe = @import("testing/Pipe.zig");
    const io = std.testing.io;

    var input_buffers: Pipe.DefaultPipeBuffers = .init;
    var input_pipe: Pipe = .initWithBuffers(io, &input_buffers);
    defer input_pipe.close();

    var output_buffers: Pipe.DefaultPipeBuffers = .init;
    var output_pipe: Pipe = .initWithBuffers(io, &output_buffers);
    defer output_pipe.close();

    const FakeRequest = struct {
        pub const version = 2;
        pub const api_key = 0x69;
        pub const is_flexible = false;

        pub fn serialise(self: *const @This(), writer: *std.Io.Writer) !void {
            _ = self;
            try writer.writeAll("hello");
        }
    };

    const FakeResponse = struct {
        pub fn deserialise(_: std.mem.Allocator, bytes: []const u8) !@This() {
            if (bytes.len == 6) {
                return .{};
            } else {
                return error.UnexpectedInput;
            }
        }
    };

    const allocator = std.testing.allocator;

    var client: Self = .init(&input_pipe.reader, &output_pipe.writer);
    defer client.close(io, allocator);

    try client.connect(io, allocator);

    const fake_request: FakeRequest = .{};

    const RequestError = anyerror;
    const Wrapper = struct {
        fn makeRequest(c: *Self, io_: Io, allocator_: std.mem.Allocator, request: FakeRequest) RequestError!KafkaResponse(FakeResponse) {
            return c.makeRequest(FakeResponse, io_, allocator_, request);
        }
    };

    var future_response = try io.concurrent(Wrapper.makeRequest, .{ &client, io, allocator, fake_request });

    _ = try output_pipe.reader.peek(1);

    try input_pipe.sendData(&.{
        0x00, 0x00, 0x00, 0x0a, // 10 bytes
        0x00, 0x00, 0x00, 0x00, // correlation id 0
        0x00, 0x00, 0x00, 0x00, 0x00, 0x00, // 6 junk bytes
    });

    var response = try future_response.await(io);
    defer response.deinit();

    try std.testing.expectEqual(FakeResponse{}, response.value);

    try std.testing.expectEqualSlices(u8, &.{
        0x00, 0x00, 0x00, 0x0F, // 15 bytes
        0x00, 0x69, // api key
        0x00, 0x02, // version
        0x00, 0x00, 0x00, 0x00, // correlation id 0
        0xFF, 0xFF, // null client id
        'h', 'e', 'l', 'l', 'o', // dummy payload
    }, try output_pipe.reader.take(19));
}

test "it propogates errors" {
    const Pipe = @import("testing/Pipe.zig");
    const io = std.testing.io;

    var input_buffers: Pipe.DefaultPipeBuffers = .init;
    var input_pipe: Pipe = .initWithBuffers(io, &input_buffers);
    defer input_pipe.close();

    var output_buffers: Pipe.DefaultPipeBuffers = .init;
    var output_pipe: Pipe = .initWithBuffers(io, &output_buffers);
    defer output_pipe.close();

    const FakeRequest = struct {
        pub const version = 1;
        pub const api_key = 1;
        pub const is_flexible = false;

        pub fn serialise(self: *const @This(), writer: *std.Io.Writer) !void {
            _ = self;
            _ = writer;
        }
    };

    const FakeResponse = struct {
        pub fn deserialise(_: std.mem.Allocator, bytes: []const u8) !@This() {
            _ = bytes;
            return .{};
        }
    };

    const allocator = std.testing.allocator;

    var client: Self = .init(&input_pipe.reader, &output_pipe.writer);
    defer client.close(io, allocator);

    try client.connect(io, allocator);

    const fake_request: FakeRequest = .{};

    const RequestError = anyerror;
    const Wrapper = struct {
        fn makeRequest(c: *Self, io_: Io, allocator_: std.mem.Allocator, request: FakeRequest) RequestError!KafkaResponse(FakeResponse) {
            return c.makeRequest(FakeResponse, io_, allocator_, request);
        }
    };

    var future_response = try io.concurrent(Wrapper.makeRequest, .{ &client, io, allocator, fake_request });

    _ = try output_pipe.reader.peek(1);

    try input_pipe.sendData(&.{
        0x00, 0x00, 0x00, 0x0a, // 10 bytes
        0x00, 0x00, 0x00, 0x00, // correlation id 0
        0x00, // incomplete body
    });

    input_pipe.close();

    try std.testing.expectError(error.ConnectionClosed, future_response.await(io));
}
