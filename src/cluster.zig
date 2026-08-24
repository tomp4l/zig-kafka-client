const std = @import("std");
const Io = std.Io;
const HostName = Io.net.HostName;

const protocol = @import("protocol");

const FakeConnection = @import("./testing/connection.zig").FakeConnection;
const BrokerConnection = @import("BrokerConnection.zig");
const ConnectedNode = @import("ConnectedNode.zig");

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

pub const TopicIdAndLeader = struct {
    leader_id: i32,
    topic_id: [16]u8,
    topic_name: []const u8,
    partition_index: i32,
};

pub const TopicNameAndPartition = struct {
    topic_name: []const u8,
    partition_index: i32,
};

pub fn GenericCluster(ConnectionType: type) type {
    return struct {
        const Self = @This();

        connection_mutex: Io.Mutex = .init,
        connection_node_map: std.AutoHashMapUnmanaged(i32, *ConnectionType) = .empty,
        connections: std.ArrayList(*ConnectionType) = .empty,
        connection_last_used: usize = 0,

        // lazily populated on write
        topic_config_mutex: std.Io.Mutex = .init,
        topic_config: TopicMap = .empty,
        cluster_allocator: std.mem.Allocator,

        pub fn init(metadata_allocator: std.mem.Allocator) Self {
            return .{ .cluster_allocator = metadata_allocator };
        }

        pub fn deinit(self: *Self, io: Io) void {
            self.connection_mutex.lockUncancelable(io);

            for (self.connections.items) |conn| {
                conn.release(io, self.cluster_allocator);
            }

            self.connection_node_map.deinit(self.cluster_allocator);
            self.connections.deinit(self.cluster_allocator);

            self.topic_config_mutex.lockUncancelable(io);

            var topic_config_it = self.topic_config.iterator();
            while (topic_config_it.next()) |kv| {
                self.cluster_allocator.free(kv.key_ptr.*);
                kv.value_ptr.deinit(self.cluster_allocator);
            }

            self.topic_config.deinit(self.cluster_allocator);

            self.* = undefined;
        }

        fn nextUnusedConnecton(self: *Self, io: Io) !*ConnectionType {
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

        fn connectionForNode(self: *@This(), io: Io, node_id: i32) !*ConnectionType {
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
            var connection: *ConnectionType = try self.connectionForNode(io, node_id);
            defer connection.release(io, allocator);

            return connection.makeRequest(ResponseType, io, allocator, request);
        }

        pub fn leadersForTopicsAndPartitions(
            self: *@This(),
            io: Io,
            request_allocator: std.mem.Allocator,
            topics: []const TopicNameAndPartition,
            output: []TopicIdAndLeader,
        ) !void {
            std.debug.assert(topics.len == output.len);

            var has_all_topics = true;
            {
                try self.topic_config_mutex.lock(io);
                defer self.topic_config_mutex.unlock(io);
                for (topics, output) |topic, *out| {
                    if (self.topic_config.get(topic.topic_name)) |topic_config| {
                        if (topic_config.partitions.get(topic.partition_index)) |partition| {
                            out.* = .{
                                .leader_id = partition.leader_id,
                                .topic_id = topic_config.topic_id,
                                .topic_name = topic.topic_name,
                                .partition_index = topic.partition_index,
                            };
                        } else {
                            has_all_topics = false;
                            break;
                        }
                    } else {
                        has_all_topics = false;
                        break;
                    }
                }
            }

            if (has_all_topics) {
                return;
            }

            var unique_topics: std.StringHashMapUnmanaged(void) = .empty;
            defer unique_topics.deinit(request_allocator);
            for (topics) |topic| {
                try unique_topics.put(request_allocator, topic.topic_name, {});
            }

            var topics_request: std.ArrayList(protocol.MetadataRequestV13.MetadataRequestTopic) = .empty;
            defer topics_request.deinit(request_allocator);
            var unique_topic_iter = unique_topics.keyIterator();
            while (unique_topic_iter.next()) |topic_name| {
                try topics_request.append(request_allocator, .{
                    .topic_id = @splat(0),
                    .name = topic_name.*,
                });
            }

            const metadata_request: protocol.MetadataRequestV13 = .{
                .topics = topics_request.items,
                .allow_auto_topic_creation = false, // could be configured
                .include_topic_authorized_operations = false, // could use this to respect ACLs later
            };

            var metadata_response = try self.makeRequestAny(
                protocol.MetadataResponseV13,
                io,
                request_allocator,
                metadata_request,
            );
            defer metadata_response.deinit();

            const response_value = metadata_response.value;

            try self.topic_config_mutex.lock(io);
            defer self.topic_config_mutex.unlock(io);

            for (response_value.topics) |topic| {
                if (topic.name) |name| {
                    const config = try self.topic_config.getOrPut(self.cluster_allocator, name);

                    if (!config.found_existing) {
                        config.key_ptr.* = try self.cluster_allocator.dupe(u8, name);
                        config.value_ptr.* = .{
                            .topic_id = topic.topic_id,
                        };
                    } else {
                        config.value_ptr.*.partitions.clearRetainingCapacity();
                    }

                    const topic_config: *TopicConfig = config.value_ptr;

                    if (topic.error_code != .NONE) {
                        std.log.warn("Got topic error {s}: {any}", .{ name, topic.error_code });

                        return error.TopicError;
                    }

                    for (topic.partitions) |partition| {
                        if (partition.error_code != .NONE) {
                            std.log.warn("Got partition error {s}-{}: {any}", .{ name, partition.partition_index, partition.error_code });

                            return error.PartitionError;
                        }

                        try topic_config.partitions.put(
                            self.cluster_allocator,
                            partition.partition_index,
                            .{ .leader_id = partition.leader_id },
                        );
                    }
                }
            }

            for (topics, output) |topic, *out| {
                if (self.topic_config.get(topic.topic_name)) |topic_config| {
                    if (topic_config.partitions.get(topic.partition_index)) |partition| {
                        out.* = .{
                            .leader_id = partition.leader_id,
                            .topic_id = topic_config.topic_id,
                            .topic_name = topic.topic_name,
                            .partition_index = topic.partition_index,
                        };
                    } else {
                        return error.MissingTopicPartition;
                    }
                } else {
                    return error.MissingTopicPartition;
                }
            }
        }

        pub fn leaderForTopicPartition(
            self: *@This(),
            io: Io,
            allocator: std.mem.Allocator,
            topic_name: []const u8,
            partition_index: i32,
        ) !TopicIdAndLeader {
            var output: [1]TopicIdAndLeader = undefined;
            try self.leadersForTopicsAndPartitions(io, allocator, &.{.{
                .topic_name = topic_name,
                .partition_index = partition_index,
            }}, &output);

            return output[0];
        }

        pub fn bootstrap(self: *Self, io: Io, allocator: std.mem.Allocator, config: BootstrapConfig) !void {
            try self.connection_mutex.lock(io);
            defer self.connection_mutex.unlock(io);
            if (self.connection_node_map.count() > 0) return error.AlreadyBootstrapped;
            errdefer {
                for (self.connections.items) |conn| {
                    conn.release(io, self.cluster_allocator);
                }
                self.connections.clearAndFree(self.cluster_allocator);
                self.connection_node_map.clearAndFree(self.cluster_allocator);
            }

            // if we provide more we can try them first here

            const last_server = config.broker_config;

            const host_name = try HostName.init(last_server.host);

            var connected_node = try ConnectionType.init(io, self.cluster_allocator, host_name, last_server.port);
            errdefer connected_node.release(io, self.cluster_allocator);

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
                    try self.connection_node_map.put(self.cluster_allocator, broker.node_id, connected_node);
                    try self.connections.append(self.cluster_allocator, connected_node);
                } else {
                    const node_host_name = try HostName.init(broker.host);

                    if (broker.port > std.math.maxInt(u16) or broker.port < 0) {
                        return error.InvalidPort;
                    }

                    var new_connected_node = try ConnectionType.init(io, self.cluster_allocator, node_host_name, @intCast(broker.port));
                    errdefer new_connected_node.release(io, self.cluster_allocator);
                    try self.connection_node_map.put(self.cluster_allocator, broker.node_id, new_connected_node);
                    try self.connections.append(self.cluster_allocator, new_connected_node);
                }
            }

            if (!reused_connection) {
                connected_node.release(io, self.cluster_allocator);
            }
        }
    };
}

test "cluster bootstrap and partition" {
    const TestCluster = GenericCluster(FakeConnection(struct {
        pub fn mock(arena: std.mem.Allocator, request: anytype) !*anyopaque {
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

    var cluster_allocator_instance: std.heap.DebugAllocator(.{}) = .init;
    const cluster_allocator = cluster_allocator_instance.allocator();
    var cluster: TestCluster = .init(cluster_allocator);
    defer cluster.deinit(io);

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
