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
    broker_connection: BrokerConnection,
    connection: Io.net.Stream,

    socket_read_buffer: [socket_read_buffer_size]u8,
    socket_write_buffer: [socket_write_buffer_size]u8,

    socket_reader: Io.net.Stream.Reader,
    socket_writer: Io.net.Stream.Writer,

    ref_count: std.atomic.Value(usize) = .init(1),

    fn init(io: Io, allocator: std.mem.Allocator, host_name: HostName, port: u16) !*@This() {
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

        self.broker_connection = .init(&self.socket_reader.interface, &self.socket_writer.interface);
        errdefer self.broker_connection.deinit(io, allocator);
        // todo client id
        try self.broker_connection.connect(io, allocator, null);

        return self;
    }

    fn retain(self: *@This()) void {
        _ = self.ref_count.fetchAdd(1, .monotonic);
    }

    fn release(self: *@This(), io: Io, allocator: std.mem.Allocator) void {
        if (self.ref_count.fetchSub(1, .monotonic) == 1) {
            self.deinit(io, allocator);
        }
    }

    fn makeRequest(self: *@This(), ResponseType: type, io: Io, allocator: std.mem.Allocator, request: anytype) !BrokerConnection.KafkaResponse(ResponseType) {
        return self.broker_connection.makeRequest(ResponseType, io, allocator, request);
    }

    fn deinit(self: *@This(), io: Io, allocator: std.mem.Allocator) void {
        self.broker_connection.deinit(io, allocator);
        self.connection.close(io);
        self.* = undefined;
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

const PartitionConfig = struct {
    leader_id: i32,
};

const TopicConfig = struct {
    topic_id: [16]u8,
    partitions: std.AutoHashMapUnmanaged(i32, PartitionConfig) = .empty,

    fn deinit(self: *@This(), allocator: std.mem.Allocator) void {
        self.partitions.deinit(allocator);
    }
};

const TopicMap = std.StringHashMapUnmanaged(TopicConfig);

pub const Cluster = GenericCluster(ConnectedNode);

const TopicIdAndLeader = struct {
    leader_id: i32,
    topic_id: [16]u8,
};

fn GenericCluster(NodeType: type) type {
    return struct {
        const Self = @This();

        connection_mutex: Io.Mutex = .init,
        connection_node_map: std.AutoHashMapUnmanaged(i32, *NodeType) = .empty,
        connections: std.ArrayList(*NodeType) = .empty,
        connection_last_used: usize = 0,

        // lazily populated on write
        topic_config_mutex: std.Io.Mutex = .init,
        topic_config: TopicMap = .empty,

        pub fn init() Self {
            return .{};
        }

        pub fn deinit(self: *Self, io: Io, allocator: std.mem.Allocator) void {
            self.connection_mutex.lockUncancelable(io);

            for (self.connections.items) |conn| {
                conn.release(io, allocator);
            }

            self.connection_node_map.deinit(allocator);
            self.connections.deinit(allocator);

            self.topic_config_mutex.lockUncancelable(io);

            var topic_config_it = self.topic_config.iterator();
            while (topic_config_it.next()) |kv| {
                allocator.free(kv.key_ptr.*);
                kv.value_ptr.deinit(allocator);
            }

            self.topic_config.deinit(allocator);

            self.* = undefined;
        }

        fn nextUnusedConnecton(self: *Self, io: Io) !*NodeType {
            try self.connection_mutex.lock(io);
            defer self.connection_mutex.unlock(io);
            if (self.connections.items.len == 0) return error.NotBootstrapped;
            self.connection_last_used += 1;
            self.connection_last_used %= self.connections.items.len;
            const conn = self.connections.items[self.connection_last_used];
            conn.retain();

            return conn;
        }

        pub fn makeRequestAny(
            self: *@This(),
            ResponseType: type,
            io: Io,
            allocator: std.mem.Allocator,
            request: anytype,
        ) !BrokerConnection.KafkaResponse(ResponseType) {
            const connection = try self.nextUnusedConnecton(io);
            defer connection.release(io, allocator);

            return connection.makeRequest(ResponseType, io, allocator, request);
        }

        fn connectionForNode(self: *@This(), io: Io, node_id: i32) !*NodeType {
            try self.connection_mutex.lock(io);
            defer self.connection_mutex.unlock(io);
            var connection = self.connection_node_map.get(node_id) orelse return error.MissingNode;
            connection.retain();

            return connection;
        }

        pub fn makeRequestNode(
            self: *@This(),
            ResponseType: type,
            io: Io,
            allocator: std.mem.Allocator,
            node_id: i32,
            request: anytype,
        ) !BrokerConnection.KafkaResponse(ResponseType) {
            var connection: *NodeType = try self.connectionForNode(io, node_id);
            defer connection.release(io, allocator);

            return connection.makeRequest(ResponseType, io, allocator, request);
        }

        pub fn leaderForTopicPartition(
            self: *@This(),
            io: Io,
            allocator: std.mem.Allocator,
            topic_name: []const u8,
            partition_index: i32,
        ) !TopicIdAndLeader {
            {
                try self.topic_config_mutex.lock(io);
                defer self.topic_config_mutex.unlock(io);
                if (self.topic_config.get(topic_name)) |config| {
                    if (config.partitions.get(partition_index)) |partition| {
                        return .{
                            .leader_id = partition.leader_id,
                            .topic_id = config.topic_id,
                        };
                    }
                }
            }

            const metadata_request: protocol.MetadataRequestV13 = .{
                .topics = &.{.{
                    .topic_id = @splat(0),
                    .name = topic_name,
                }},
                .allow_auto_topic_creation = false, // could be configured
                .include_topic_authorized_operations = false, // could use this to respect ACLs later
            };

            var metadata_response = try self.makeRequestAny(
                protocol.MetadataResponseV13,
                io,
                allocator,
                metadata_request,
            );
            defer metadata_response.deinit();

            const response_value = metadata_response.value;

            try self.topic_config_mutex.lock(io);
            defer self.topic_config_mutex.unlock(io);

            for (response_value.topics) |topic| {
                if (topic.name) |name| {
                    const config = try self.topic_config.getOrPut(allocator, name);

                    if (!config.found_existing) {
                        config.key_ptr.* = try allocator.dupe(u8, name);
                        config.value_ptr.* = .{
                            .topic_id = topic.topic_id,
                        };
                    } else {
                        config.value_ptr.*.partitions.clearRetainingCapacity();
                    }

                    const topic_config: *TopicConfig = config.value_ptr;

                    const is_topic = std.mem.eql(u8, name, topic_name);
                    if (topic.error_code != .NONE) {
                        std.log.warn("Got topic error {s}: {any}", .{ name, topic.error_code });

                        if (is_topic) {
                            return error.TopicError;
                        } else {
                            continue;
                        }
                    }

                    for (topic.partitions) |partition| {
                        if (partition.error_code != .NONE) {
                            std.log.warn("Got partition error {s}-{}: {any}", .{ name, partition.partition_index, partition.error_code });
                            if (is_topic and partition.partition_index == partition_index) {
                                return error.PartitionError;
                            } else {
                                continue;
                            }
                        }

                        try topic_config.partitions.put(
                            allocator,
                            partition.partition_index,
                            .{ .leader_id = partition.leader_id },
                        );
                    }
                }
            }

            if (self.topic_config.get(topic_name)) |config| {
                if (config.partitions.get(partition_index)) |partition| {
                    return .{
                        .leader_id = partition.leader_id,
                        .topic_id = config.topic_id,
                    };
                }
            }
            return error.NotFound;
        }

        pub fn bootstrap(self: *Self, io: Io, allocator: std.mem.Allocator, config: BootstrapConfig) !void {
            try self.connection_mutex.lock(io);
            defer self.connection_mutex.unlock(io);
            if (self.connection_node_map.count() > 0) return error.AlreadyBootstrapped;
            errdefer {
                for (self.connections.items) |conn| {
                    conn.release(io, allocator);
                }
                self.connections.clearAndFree(allocator);
                self.connection_node_map.clearAndFree(allocator);
            }

            // if we provide more we can try them first here

            const last_server = config.broker_config;

            const host_name = try HostName.init(last_server.host);

            var connected_node = try NodeType.init(io, allocator, host_name, last_server.port);
            errdefer connected_node.release(io, allocator);

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
                    try self.connection_node_map.put(allocator, broker.node_id, connected_node);
                    try self.connections.append(allocator, connected_node);
                } else {
                    const node_host_name = try HostName.init(broker.host);

                    if (broker.port > std.math.maxInt(u16) or broker.port < 0) {
                        return error.InvalidPort;
                    }

                    var new_connected_node = try NodeType.init(io, allocator, node_host_name, @intCast(broker.port));
                    errdefer new_connected_node.release(io, allocator);
                    try self.connection_node_map.put(allocator, broker.node_id, new_connected_node);
                    try self.connections.append(allocator, new_connected_node);
                }
            }

            if (!reused_connection) {
                connected_node.release(io, allocator);
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
        call_count: std.atomic.Value(usize) = .init(0),

        ref_count: std.atomic.Value(usize) = .init(1),

        fn init(io_: Io, allocator: std.mem.Allocator, host_name: HostName, port: u16) !*@This() {
            _ = io_;

            const self = try allocator.create(@This());

            self.* = .{
                .host_name = try allocator.dupe(u8, host_name.bytes),
                .port = port,
                .id = global_id.fetchAdd(1, .monotonic),
            };

            return self;
        }

        fn makeRequest(
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

        fn retain(self: *@This()) void {
            _ = self.ref_count.fetchAdd(1, .monotonic);
        }

        fn release(self: *@This(), io: Io, allocator: std.mem.Allocator) void {
            _ = io;

            if (self.ref_count.fetchSub(1, .monotonic) == 1) {
                allocator.free(self.host_name);
                allocator.destroy(self);
            }
        }
    };
}

test "cluster bootstrap and partition" {
    const TestCluster = GenericCluster(FakeConnection(struct {
        fn mock(arena: std.mem.Allocator, request: anytype) !*anyopaque {
            if (@TypeOf(request) == protocol.MetadataRequestV13) {
                const request_typed: protocol.MetadataRequestV13 = request;

                const response = try arena.create(protocol.MetadataResponseV13);

                response.error_code = .NONE;
                var brokers = try arena.alloc(protocol.MetadataResponseV13.MetadataResponseBroker, 2);
                brokers[0] = .{
                    .node_id = 1,
                    .host = "localhost",
                    .port = 1234,
                };
                brokers[1] = .{
                    .node_id = 2,
                    .host = "localhost",
                    .port = 2345,
                };

                response.brokers = brokers;

                if (request_typed.topics) |topics| {
                    var response_topics: std.ArrayList(protocol.MetadataResponseV13.MetadataResponseTopic) = .empty;
                    for (topics) |topic| {
                        try response_topics.append(arena, .{
                            .error_code = .NONE,
                            .name = if (topic.name) |name| try arena.dupe(u8, name) else null,
                            .topic_id = @splat(0),
                            .partitions = &.{
                                .{
                                    .error_code = .NONE,
                                    .partition_index = 0,
                                    .leader_id = 123,
                                    .replica_nodes = &.{},
                                    .isr_nodes = &.{},
                                    .offline_replicas = &.{},
                                },
                            },
                        });
                    }

                    response.topics = try response_topics.toOwnedSlice(arena);
                }

                return response;
            }

            std.debug.print("Unexpected type {any}\n", .{@TypeOf(request)});

            return error.Unmatched;
        }
    }));

    const io = std.testing.io;
    const allocator = std.testing.allocator;

    var cluster: TestCluster = .init();
    defer cluster.deinit(io, allocator);

    try cluster.bootstrap(io, allocator, .single("localhost", 1234));

    const node1 = cluster.connection_node_map.get(1) orelse return error.MissingNode;
    const node2 = cluster.connection_node_map.get(2) orelse return error.MissingNode;
    const node3 = cluster.connection_node_map.get(3);

    try std.testing.expectEqual(0, node1.id);
    try std.testing.expectEqualStrings("localhost", node1.host_name);
    try std.testing.expectEqual(1234, node1.port);

    try std.testing.expectEqual(1, node2.id);
    try std.testing.expectEqualStrings("localhost", node2.host_name);
    try std.testing.expectEqual(2345, node2.port);

    try std.testing.expectEqual(null, node3);

    const topic_partition_leader = try cluster.leaderForTopicPartition(io, allocator, "test-topic", 0);
    try std.testing.expectEqual(123, topic_partition_leader.leader_id);

    var call_counts: [2]usize = undefined;
    for (cluster.connections.items, 0..) |c, i| {
        call_counts[i] = c.call_count.load(.acquire);
    }

    const topic_partition_leader_cached = try cluster.leaderForTopicPartition(io, allocator, "test-topic", 0);
    try std.testing.expectEqual(123, topic_partition_leader_cached.leader_id);

    var call_counts_after: [2]usize = undefined;
    for (cluster.connections.items, 0..) |c, i| {
        call_counts_after[i] = c.call_count.load(.acquire);
    }
    try std.testing.expectEqualSlices(usize, &call_counts, &call_counts_after);
}

test {
    _ = @import("./protocol/RecordSet.zig");
}
