const std = @import("std");
const Io = std.Io;
const HostName = Io.net.HostName;

const BrokerConnection = @import("BrokerConnection.zig");
const protocol = @import("protocol");

// todo config?
const socket_read_buffer_size = 128 * 1024;
const socket_write_buffer_size = 128 * 1024;

const ConnectedNode = struct {
    node_id: ?i32 = null,
    broker_connection: BrokerConnection = undefined,
    connection: Io.net.Stream = undefined,

    socket_read_buffer: [socket_read_buffer_size]u8 = undefined,
    socket_write_buffer: [socket_write_buffer_size]u8 = undefined,

    socket_reader: Io.net.Stream.Reader = undefined,
    socket_writer: Io.net.Stream.Writer = undefined,

    fn init(io: Io, allocator: std.mem.Allocator, host_name: HostName, port: u16) !*@This() {
        const self = try allocator.create(@This());
        self.node_id = null;
        self.connection = try host_name.connect(io, port, .{
            .mode = .stream,
            .timeout = .none,
        });
        errdefer self.connection.close(io);

        self.socket_reader = self.connection.reader(io, &self.socket_read_buffer);
        self.socket_writer = self.connection.writer(io, &self.socket_write_buffer);

        self.broker_connection = .init(&self.socket_reader.interface, &self.socket_writer.interface);
        errdefer self.broker_connection.deinit(io, allocator);
        // todo client id
        try self.broker_connection.connect(io, allocator, null);

        return self;
    }

    fn makeRequest(self: *@This(), ResponseType: type, io: Io, allocator: std.mem.Allocator, request: anytype) !BrokerConnection.KafkaResponse(ResponseType) {
        return self.broker_connection.makeRequest(ResponseType, io, allocator, request);
    }

    fn deinit(self: *@This(), io: Io, allocator: std.mem.Allocator) void {
        self.broker_connection.deinit(io, allocator);
        self.connection.close(io);
        allocator.destroy(self);
    }
};

const BrokerConfig = struct {
    host: []const u8,
    port: u16,
};

pub const BootstrapConfig = struct {
    broker_config: BrokerConfig,
    pub fn single(host: []const u8, port: u16) @This() {
        return .{ .broker_config = .{
            .host = host,
            .port = port,
        } };
    }
};

const TopicConfig = struct {};

const TopicMap = std.AutoHashMapUnmanaged([]const u8, TopicConfig);

pub const Cluster = GenericCluster(ConnectedNode);

fn GenericCluster(NodeType: type) type {
    return struct {
        const Self = @This();

        connection_mutex: Io.Mutex = .init,
        node_map: std.AutoHashMapUnmanaged(i32, *NodeType) = .empty,

        // lazily populated on write
        topic_config_mutex: std.Io.Mutex = .init,
        topic_config: TopicMap = .empty,

        pub fn init() Self {
            return .{};
        }

        pub fn deinit(self: *Self, io: Io, allocator: std.mem.Allocator) void {
            self.connection_mutex.lockUncancelable(io);
            var it = self.node_map.iterator();

            while (it.next()) |entry| {
                entry.value_ptr.*.deinit(io, allocator);
            }

            self.node_map.deinit(allocator);

            self.topic_config_mutex.lockUncancelable(io);
            self.topic_config.deinit(allocator);
        }

        pub fn bootstrap(self: *Self, io: Io, allocator: std.mem.Allocator, config: BootstrapConfig) !void {
            try self.connection_mutex.lock(io);
            defer self.connection_mutex.unlock(io);
            if (self.node_map.count() > 0) return error.AlreadyBootstrapped;

            // if we provide more we can try them first here

            const last_server = config.broker_config;

            const host_name = try HostName.init(last_server.host);

            var connected_node = try NodeType.init(io, allocator, host_name, last_server.port);
            errdefer connected_node.deinit(io, allocator);

            const req = protocol.MetadataRequestV13{
                .topics = &.{},
                .include_topic_authorized_operations = false,
            };

            const Response = BrokerConnection.KafkaResponse(protocol.MetadataResponseV13);
            var response: Response = try connected_node.makeRequest(protocol.MetadataResponseV13, io, allocator, req);
            defer response.deinit();

            if (response.value.error_code != .NONE) {
                return error.FailedMetadataRequest;
            }

            var reused_connection = false;

            for (response.value.brokers) |broker| {
                if (std.mem.eql(u8, last_server.host, broker.host) and
                    @as(i32, @intCast(last_server.port)) == broker.port)
                {
                    reused_connection = true;
                    try self.node_map.put(allocator, broker.node_id, connected_node);
                } else {
                    const node_host_name = try HostName.init(broker.host);

                    if (broker.port > std.math.maxInt(u16) or broker.port < 0) {
                        return error.InvalidPort;
                    }

                    var new_connected_node = try NodeType.init(io, allocator, node_host_name, @intCast(broker.port));
                    errdefer new_connected_node.deinit(io, allocator);
                    try self.node_map.put(allocator, broker.node_id, new_connected_node);
                }
            }

            if (!reused_connection) {
                connected_node.deinit(io, allocator);
            }
        }
    };
}

fn FakeConnection(mockFn: anytype) type {
    return struct {
        var global_id: std.atomic.Value(usize) = .init(0);

        host_name: []const u8,
        port: u16,
        id: usize,

        fn init(io_: Io, allocator: std.mem.Allocator, host_name: HostName, port: u16) !*@This() {
            _ = io_;

            const self = try allocator.create(@This());
            self.host_name = try allocator.dupe(u8, host_name.bytes);
            self.port = port;
            self.id = global_id.fetchAdd(1, .monotonic);
            return self;
        }

        fn makeRequest(self: *@This(), ResponseType: type, io: Io, allocator: std.mem.Allocator, request: anytype) !BrokerConnection.KafkaResponse(ResponseType) {
            _ = io;
            _ = self;

            var arena = std.heap.ArenaAllocator.init(allocator);

            const response_raw = try mockFn.mock(arena.allocator(), &request);
            const response_cast: *ResponseType = @ptrCast(@alignCast(response_raw));

            return BrokerConnection.KafkaResponse(ResponseType){ .arena = arena, .raw_buffer = &.{}, .value = response_cast.* };
        }

        fn deinit(self: *@This(), io: Io, allocator: std.mem.Allocator) void {
            _ = io;
            allocator.free(self.host_name);
            allocator.destroy(self);
        }
    };
}

test "cluster bootstrap" {
    const TestCluster = GenericCluster(FakeConnection(struct {
        fn mock(arena: std.mem.Allocator, request: anytype) !*anyopaque {
            _ = request;

            const response = try arena.create(protocol.MetadataResponseV13);

            response.error_code = .NONE;
            response.brokers = try arena.alloc(protocol.MetadataResponseV13.MetadataResponseBroker, 2);

            response.brokers[0] = .{
                .node_id = 1,
                .host = "localhost",
                .port = 1234,
            };

            response.brokers[1] = .{
                .node_id = 2,
                .host = "localhost",
                .port = 2345,
            };

            return response;
        }
    }));

    const io = std.testing.io;
    const allocator = std.testing.allocator;

    var cluster: TestCluster = .init();
    defer cluster.deinit(io, allocator);

    try cluster.bootstrap(io, allocator, .single("localhost", 1234));

    const node1 = cluster.node_map.get(1) orelse return error.MissingNode;
    const node2 = cluster.node_map.get(2) orelse return error.MissingNode;
    const node3 = cluster.node_map.get(3);

    try std.testing.expectEqual(0, node1.id);
    try std.testing.expectEqualStrings("localhost", node1.host_name);
    try std.testing.expectEqual(1234, node1.port);

    try std.testing.expectEqual(1, node2.id);
    try std.testing.expectEqualStrings("localhost", node2.host_name);
    try std.testing.expectEqual(2345, node2.port);

    try std.testing.expectEqual(null, node3);
}
