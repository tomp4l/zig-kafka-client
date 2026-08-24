const std = @import("std");
const Io = std.Io;
const HostName = Io.net.HostName;

const BrokerConnection = @import("BrokerConnection.zig");

// todo config?
const socket_read_buffer_size = 128 * 1024;
const socket_write_buffer_size = 128 * 1024;

node_id: ?i32 = null,
broker_connection: BrokerConnection,
connection: Io.net.Stream,

socket_read_buffer: [socket_read_buffer_size]u8,
socket_write_buffer: [socket_write_buffer_size]u8,

socket_reader: Io.net.Stream.Reader,
socket_writer: Io.net.Stream.Writer,

ref_count: std.atomic.Value(usize) = .init(1),

pub fn init(io: Io, allocator: std.mem.Allocator, host_name: HostName, port: u16) !*@This() {
    const self = try allocator.create(@This());
    errdefer allocator.destroy(self);
    self.node_id = null;
    self.ref_count = .init(1);
    self.connection = try host_name.connect(io, port, .{
        .mode = .stream,
        .timeout = .none,
    });
    errdefer self.connection.close(io);

    self.socket_reader = self.connection.reader(io, &self.socket_read_buffer);
    self.socket_writer = self.connection.writer(io, &self.socket_write_buffer);

    self.broker_connection = .init(&self.socket_reader.interface, &self.socket_writer.interface, allocator);
    errdefer self.broker_connection.deinit(io);
    // todo client id
    try self.broker_connection.connect(io, allocator, null);

    return self;
}

pub fn retain(self: *@This()) void {
    _ = self.ref_count.fetchAdd(1, .monotonic);
}

pub fn release(self: *@This(), io: Io, allocator: std.mem.Allocator) void {
    if (self.ref_count.fetchSub(1, .monotonic) == 1) {
        self.deinit(io, allocator);
    }
}

pub fn makeRequest(self: *@This(), ResponseType: type, io: Io, allocator: std.mem.Allocator, request: anytype) !BrokerConnection.KafkaResponse(ResponseType) {
    return self.broker_connection.makeRequest(ResponseType, io, allocator, request) catch |err| switch (err) {
        error.Timeout => return err,
        error.WriteFailed => return self.socket_writer.err orelse err,
        error.ConcurrencyUnavailable => return err,
        error.Canceled => return err,
        error.OutOfMemory => return err,
        error.ConnectionClosed => return self.socket_reader.err orelse (self.broker_connection.read_error orelse err),
        // should probably do something to bundle these up as serde errors
        error.TooShort => return err,
        error.UnsupportedVersion => return err,
        error.VarIntTooBig => return err,
        error.NonNullableField => return err,
    };
}

pub fn deinit(self: *@This(), io: Io, allocator: std.mem.Allocator) void {
    self.broker_connection.deinit(io);
    self.connection.close(io);
    self.* = undefined;
    allocator.destroy(self);
}
