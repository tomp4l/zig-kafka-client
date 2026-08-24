const std = @import("std");
const Io = std.Io;

const BrokerConnection = @import("../BrokerConnection.zig");

const HostName = std.Io.net.HostName;

pub fn FakeConnection(mockFn: anytype) type {
    return struct {
        var global_id: std.atomic.Value(usize) = .init(0);

        host_name: []const u8,
        port: u16,
        id: usize,
        call_count: std.atomic.Value(usize) = .init(0),

        ref_count: std.atomic.Value(usize) = .init(1),

        pub fn init(io_: Io, allocator: std.mem.Allocator, host_name: HostName, port: u16) !*@This() {
            _ = io_;

            const self = try allocator.create(@This());

            self.* = .{
                .host_name = try allocator.dupe(u8, host_name.bytes),
                .port = port,
                .id = global_id.fetchAdd(1, .monotonic),
            };

            return self;
        }

        pub fn makeRequest(
            self: *@This(),
            ResponseType: type,
            io: Io,
            allocator: std.mem.Allocator,
            request: anytype,
        ) !BrokerConnection.KafkaResponse(ResponseType) {
            _ = io;

            _ = self.call_count.fetchAdd(1, .monotonic);

            var arena = std.heap.ArenaAllocator.init(allocator);

            const response_raw = try mockFn.mock(arena.allocator(), request);
            const response_cast: *ResponseType = @ptrCast(@alignCast(response_raw));

            return BrokerConnection.KafkaResponse(ResponseType){ .arena = arena, .raw_buffer = &.{}, .value = response_cast.* };
        }

        pub fn retain(self: *@This()) void {
            _ = self.ref_count.fetchAdd(1, .monotonic);
        }

        pub fn release(self: *@This(), io: Io, allocator: std.mem.Allocator) void {
            _ = io;

            if (self.ref_count.fetchSub(1, .monotonic) == 1) {
                allocator.free(self.host_name);
                allocator.destroy(self);
            }
        }
    };
}
