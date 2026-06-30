const std = @import("std");
const protocol = @import("protocol");
const Io = std.Io;

const Self = @This();

inflight_requests_mutex: std.Io.Mutex = .init,
write_mutex: std.Io.Mutex = .init,

// correlation ID to request mapping
inflight_requests: std.AutoHashMapUnmanaged(u32, *InFlightRequest) = .empty,
valid_versions: std.AutoHashMapUnmanaged(i16, VersionRange) = .empty,

correlation_id: std.atomic.Value(u32) = .init(0),
read_future: ?Io.Future(void) = null,
read_error: ?anyerror = null,

client_id: ?[]const u8 = null,

//TODO Pull from build.zig.zon?
client_software_name: []const u8 = "zig-kafka-client",
client_software_version: []const u8 = "0.0.1",

reader: *Io.Reader,
writer: *Io.Writer,

const VersionRange = struct {
    min: i16,
    max: i16,
};

pub fn init(reader: *Io.Reader, writer: *Io.Writer) Self {
    return .{
        .reader = reader,
        .writer = writer,
    };
}

pub fn connect(self: *Self, io: Io, allocator: std.mem.Allocator, client_id: ?[]const u8) !void {
    if (self.read_future != null) return error.AlreadyConnected;

    self.client_id = client_id;
    self.read_future = try io.concurrent(readResponses, .{ self, io, allocator });

    const api_version_request: protocol.ApiVersionsRequestV4 = .{
        .client_software_name = self.client_software_name,
        .client_software_version = self.client_software_version,
    };
    // todo version downgrade
    var versions = try self.makeRequestInternal(
        protocol.ApiVersionsResponseV4,
        io,
        allocator,
        api_version_request,
        false,
    );
    defer versions.deinit();

    if (versions.value.error_code != .NONE) {
        std.log.err("Failed to get versions: {}", .{versions.value.error_code});
        return error.VersionDiscoveryFailed;
    }

    {
        try self.write_mutex.lock(io);
        defer self.write_mutex.unlock(io);

        for (versions.value.api_keys) |api_key| {
            try self.valid_versions.put(
                allocator,
                api_key.api_key,
                .{ .min = api_key.min_version, .max = api_key.max_version },
            );
        }
    }
}

pub fn deinit(self: *Self, io: Io, allocator: std.mem.Allocator) void {
    if (self.read_future) |*f| f.cancel(io);

    self.inflight_requests_mutex.lock(io) catch {
        return;
    };
    defer self.inflight_requests_mutex.unlock(io);

    self.inflight_requests.deinit(allocator);

    self.write_mutex.lock(io) catch {
        return;
    };

    self.valid_versions.deinit(allocator);
    self.read_error = null;
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
        arena: std.heap.ArenaAllocator,

        pub fn deinit(self: *@This()) void {
            self.arena.child_allocator.free(self.raw_buffer);
            self.arena.deinit();
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

pub fn makeRequest(
    self: *Self,
    ResponseType: type,
    io: Io,
    allocator: std.mem.Allocator,
    request: anytype,
) !KafkaResponse(ResponseType) {
    return makeRequestInternal(
        self,
        ResponseType,
        io,
        allocator,
        request,
        true,
    );
}

// Makes a kafka request, if validate_version is true also validates against this connections supported versions.
fn makeRequestInternal(
    self: *Self,
    ResponseType: type,
    io: Io,
    allocator: std.mem.Allocator,
    request: anytype,
    comptime validate_version: bool,
) !KafkaResponse(ResponseType) {
    const RequestType = @TypeOf(request);
    if (validate_version) {
        try self.write_mutex.lock(io);
        defer self.write_mutex.unlock(io);
        if (self.valid_versions.count() == 0 or self.read_future == null) {
            return error.ConnectionClosed;
        }

        if (self.valid_versions.get(RequestType.api_key)) |versions| {
            if (versions.min > RequestType.version or versions.max < RequestType.version) {
                return error.UnsupportedVersion;
            }
        } else {
            return error.UnsupportedVersion;
        }
    }

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

    const client_id_length =
        if (self.client_id) |client_id|
            client_id.len
        else
            0;

    const size = bytes + client_id_length + if (RequestType.is_flexible) 11 else 10;

    {
        try self.write_mutex.lock(io);
        self.write_mutex.unlock(io);

        if (validate_version and self.valid_versions.count() == 0 or self.read_future == null) {
            return error.ConnectionClosed;
        }

        try self.writer.writeInt(u32, @truncate(size), .big);
        try self.writer.writeInt(u16, RequestType.api_key, .big);
        try self.writer.writeInt(u16, RequestType.version, .big);
        try self.writer.writeInt(u32, correlation_id, .big);
        if (self.client_id) |client_id| {
            try self.writer.writeInt(i16, @intCast(@as(u15, @truncate(client_id.len))), .big);
            try self.writer.writeAll(client_id);
        } else {
            try self.writer.writeInt(i16, -1, .big);
        }
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

    errdefer allocator.free(in_flight.response);
    var value_arena: std.heap.ArenaAllocator = .init(allocator);
    errdefer value_arena.deinit();
    const value = try ResponseType.deserialise(value_arena.allocator(), in_flight.response);
    return .{
        .arena = value_arena,
        .raw_buffer = in_flight.response,
        .value = value,
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

    var connection: Self = .init(&input_pipe.reader, &output_pipe.writer);
    defer connection.deinit(io, allocator);
    connection.client_software_name = "";
    connection.client_software_version = "";

    var connect_future = try io.concurrent(connect, .{ &connection, io, allocator, null });

    const bytes = try output_pipe.reader.take(18);
    try std.testing.expectEqualSlices(u8, &.{
        0x00, 0x00, 0x00, 0x0E, // 14 bytes
        0x00, 0x12, // api key 18
        0x00, 0x04, // version 4
        0x00, 0x00, 0x00, 0x00, // correlation_id 1
        0xFF, 0xFF, // null client id
        0x00, // 0 flex headers
        0x01, // empty client software name (this is invalid but whatever)
        0x01, // empty client software version
        0x00, // 0 flex fields
    }, bytes);

    try input_pipe.sendData(&.{
        0x00, 0x00, 0x00, 19, // 19 bytes
        0x00, 0x00, 0x00, 0x00, // correlation id 0

        0x00, 0x00, // 1. error_code: i16 (0)

        0x02, // 2. api_keys compact array: (1 item -> encoded as length + 1 = 2)
        // --- ApiVersion[0] ---
        0x00, 0x69, // api_key (105)
        0x00, 0x00, // min_version (0)
        0x00, 0x05, // max_version (5)
        0x00, // TAG BUFFER (0 tags) for the ApiVersion struct
        // --- End ApiVersion[0] ---

        0x00, 0x00, 0x00, 0x00, // 3. throttle_time_ms: i32 (0)

        0x00, // Num Tags: 0
    });

    try connect_future.await(io);

    const fake_request: FakeRequest = .{};

    const RequestError = anyerror;
    const Wrapper = struct {
        fn makeRequest(c: *Self, io_: Io, allocator_: std.mem.Allocator, request: FakeRequest) RequestError!KafkaResponse(FakeResponse) {
            return c.makeRequest(FakeResponse, io_, allocator_, request);
        }
    };

    var future_response = try io.concurrent(Wrapper.makeRequest, .{ &connection, io, allocator, fake_request });

    _ = try output_pipe.reader.peek(1);

    try input_pipe.sendData(&.{
        0x00, 0x00, 0x00, 0x0a, // 10 bytes
        0x00, 0x00, 0x00, 0x01, // correlation id 1
        0x00, 0x00, 0x00, 0x00, 0x00, 0x00, // 6 junk bytes
    });

    var response = try future_response.await(io);
    defer response.deinit();

    try std.testing.expectEqual(FakeResponse{}, response.value);

    try std.testing.expectEqualSlices(u8, &.{
        0x00, 0x00, 0x00, 0x0F, // 15 bytes
        0x00, 0x69, // api key
        0x00, 0x02, // version
        0x00, 0x00, 0x00, 0x01, // correlation id 1
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

    var connection: Self = .init(&input_pipe.reader, &output_pipe.writer);
    defer connection.deinit(io, allocator);
    connection.client_software_name = "";
    connection.client_software_version = "";

    var connect_future = try io.concurrent(connect, .{ &connection, io, allocator, null });

    const bytes = try output_pipe.reader.take(18);

    try std.testing.expectEqualSlices(u8, &.{
        0x00, 0x00, 0x00, 0x0E, // 14 bytes
        0x00, 0x12, // api key 18
        0x00, 0x04, // version 4
        0x00, 0x00, 0x00, 0x00, // correlation_id 1
        0xFF, 0xFF, // null client id
        0x00, // 0 flex headers
        0x01, // empty client software name
        0x01, // empty client software version
        0x00, // 0 flex fields
    }, bytes);

    try input_pipe.sendData(&.{
        0x00, 0x00, 0x00, 12, // 12 bytes
        0x00, 0x00, 0x00, 0x00, // correlation id 0
        0x00, 0x00, // 1. error_code: i16 (0)
        0x01, // 2. api_keys compact array: (0 items)
        0x00, 0x00, 0x00, 0x00, // 3. throttle_time_ms: i32 (0)
        0x00, // Num Tags: 0
    });

    try connect_future.await(io);

    try connection.valid_versions.put(allocator, FakeRequest.api_key, .{ .min = 0, .max = 10 });

    const fake_request: FakeRequest = .{};

    const RequestError = anyerror;
    const Wrapper = struct {
        fn makeRequest(c: *Self, io_: Io, allocator_: std.mem.Allocator, request: FakeRequest) RequestError!KafkaResponse(FakeResponse) {
            return c.makeRequest(FakeResponse, io_, allocator_, request);
        }
    };

    var future_response = try io.concurrent(Wrapper.makeRequest, .{ &connection, io, allocator, fake_request });

    _ = try output_pipe.reader.peek(1);

    try input_pipe.sendData(&.{
        0x00, 0x00, 0x00, 0x0a, // 10 bytes
        0x00, 0x00, 0x00, 0x01, // correlation id 0
        0x00, // incomplete body
    });

    input_pipe.close();

    try std.testing.expectError(error.ConnectionClosed, future_response.await(io));
}
