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
    self.inflight_requests_mutex.lock(io) catch {
        return;
    };
    defer self.inflight_requests_mutex.unlock(io);

    if (self.read_future) |*f| f.cancel(io);

    self.inflight_requests.deinit(allocator);
}

const InFlightRequest = struct {
    request_state: std.atomic.Value(RequestState) = .init(.started),
    response: []const u8 = undefined,
    is_flexible: bool,
};

const RequestState = enum(u32) {
    started,
    completed,
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

fn makeRequest(self: *Self, ResponseType: type, io: Io, allocator: std.mem.Allocator, request: anytype) !KafkaResponse(ResponseType) {
    const RequestType = @TypeOf(request);
    var in_flight: InFlightRequest = .{ .is_flexible = RequestType.is_flexible };
    const correlation_id = self.getNextCorrelationId();
    {
        try self.inflight_requests_mutex.lock(io);
        defer self.inflight_requests_mutex.unlock(io);
        try self.inflight_requests.put(allocator, correlation_id, &in_flight);
    }
    // Ensure we always clean up if something goes wrong
    defer {
        self.inflight_requests_mutex.lock(io) catch {};
        _ = self.inflight_requests.remove(correlation_id);
        self.inflight_requests_mutex.unlock(io);
    }

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
    try io.futexWait(RequestState, &in_flight.request_state.raw, .started);
    const new_state = in_flight.request_state.load(.acquire);
    _ = new_state;

    return .{
        .allocator = allocator,
        .raw_buffer = in_flight.response,
        .value = try ResponseType.deserialise(in_flight.response),
    };
}

fn recordReadError(self: *Self, err: anyerror) void {
    switch (err) {
        error.EndOfStream => {},
        else => std.log.warn("got read error: {}", .{err}),
    }
    self.read_error = err;
}

fn readResponses(self: *Self, io: Io, allocator: std.mem.Allocator) void {
    while (true) {

        // read next message
        var size = self.reader.takeInt(u32, .big) catch |err| return self.recordReadError(err);
        const correlation_id = self.reader.takeInt(u32, .big) catch |err| return self.recordReadError(err);
        size -= 4;

        const request = lock: {
            self.inflight_requests_mutex.lock(io) catch |err| return self.recordReadError(err);
            defer self.inflight_requests_mutex.unlock(io);
            break :lock self.inflight_requests.fetchRemove(correlation_id);
        };

        if (request) |kv| {
            const r = kv.value;

            if (r.is_flexible) {
                const flexible = self.reader.takeByte() catch |err| return self.recordReadError(err);
                if (flexible != 0) return self.recordReadError(error.Unimplemented);
                size -= 1;
            }

            const response_body = allocator.alloc(u8, size) catch |err| return self.recordReadError(err);

            self.reader.readSliceAll(response_body) catch |err| return self.recordReadError(err);

            r.response = response_body;
            r.request_state.store(.completed, .release);
            io.futexWake(RequestState, &r.request_state.raw, 1);
        } else {
            self.reader.discardAll(size) catch |err| return self.recordReadError(err);
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
        pub const version = 1;
        pub const api_key = 1;
        pub const is_flexible = false;

        pub fn serialise(self: *const @This(), writer: *std.Io.Writer) !void {
            _ = self;
            try writer.writeAll("hello");
        }
    };

    const FakeResponse = struct {
        pub fn deserialise(bytes: []const u8) !@This() {
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

    const RequestError = error{ WriteFailed, Canceled, OutOfMemory, UnexpectedInput };
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
}
