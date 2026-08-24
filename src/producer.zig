const std = @import("std");
const Io = std.Io;

const protocol = @import("protocol");

const RecordSet = @import("./protocol/RecordSet.zig");
const FakeConnection = @import("./testing/connection.zig").FakeConnection;
const cluster_module = @import("cluster.zig");
const GenericCluster = cluster_module.GenericCluster;

pub const Producer = GenericProducer(@import("ConnectedNode.zig"));

pub const ProducerRecord = struct {
    key: ?[]const u8,
    value: ?[]const u8,
    headers: []const RecordSet.Header,
};

pub const ProducerRecords = struct {
    topic: []const u8,
    partition_id: i32,
    records: []const ProducerRecord,
};

pub const QueuedRecords = struct {
    producer_records: ProducerRecords,
    semaphore: *Io.Semaphore,
};

const ProducerConfig = struct {
    max_batch_size: usize = 100,
};

const PartitionedMessages = std.StringHashMapUnmanaged(std.AutoHashMapUnmanaged(i32, std.ArrayList(RecordSet.Record)));

fn GenericProducer(ConnectionType: type) type {
    return struct {
        cluster: *GenericCluster(ConnectionType),

        message_queue: Io.Queue(QueuedRecords),
        config: ProducerConfig,

        connection_mutex: std.Io.Mutex = .init,
        run_future: ?std.Io.Future(void) = null,
        produce_error: ?anyerror = null,

        pub fn init(
            cluster: *GenericCluster(ConnectionType),
            message_queue_buffer: []QueuedRecords,
            config: ProducerConfig,
        ) @This() {
            return .{
                .cluster = cluster,
                .message_queue = .init(message_queue_buffer),
                .config = config,
            };
        }

        pub fn deinit(self: *@This(), io: Io) void {
            if (self.run_future) |*f| f.cancel(io);
            self.message_queue.close(io);
        }

        pub fn connect(self: *@This(), io: Io, allocator: std.mem.Allocator) !void {
            try self.connection_mutex.lock(io);
            defer self.connection_mutex.unlock(io);

            self.run_future = try io.concurrent(runProduceRequests, .{
                self,
                io,
                allocator,
            });
        }

        pub fn produce(self: *@This(), io: Io, producer_records: ProducerRecords) !void {
            {
                try self.connection_mutex.lock(io);
                defer self.connection_mutex.unlock(io);

                if (self.run_future == null) {
                    return error.ConnectionClosed;
                }
            }
            var semaphore: Io.Semaphore = .{};
            self.message_queue.putOne(io, .{
                .producer_records = producer_records,
                .semaphore = &semaphore,
            }) catch |err| switch (err) {
                error.Closed => return error.ConnectionClosed,
                error.Canceled => return error.Canceled,
            };

            try semaphore.wait(io);
        }

        const TimeoutOrMessages = union(enum) {
            timeout,
            messages: error{ Canceled, Closed }!usize,
        };

        fn handleProduceError(self: *@This(), io: Io, err: anyerror) void {
            self.produce_error = err;
            self.message_queue.close(io);

            switch (err) {
                error.Canceled => return,
                else => std.log.err("Unexpected producer error: {any}", .{err}),
            }
        }

        fn runProduceRequests(self: *@This(), io: Io, allocator: std.mem.Allocator) void {
            var buffer: [32]QueuedRecords = undefined;
            var message_count: usize = 0;
            _ = &message_count; // autofix
            var partitioned_messages: PartitionedMessages = undefined;
            var select_buffer: [2]TimeoutOrMessages = undefined;
            var select: Io.Select(TimeoutOrMessages) = .init(io, &select_buffer);
            defer select.cancelDiscard();

            var produce_arena: std.heap.ArenaAllocator = .init(allocator);
            defer produce_arena.deinit();
            const arena_allocator = produce_arena.allocator();

            while (true) {
                partitioned_messages = .empty;

                select.concurrent(.messages, @TypeOf(self.message_queue).get, .{ &self.message_queue, io, &buffer, 1 }) catch |err| return self.handleProduceError(io, err);

                const next = select.await() catch |err| return self.handleProduceError(io, err);

                switch (next) {
                    .timeout => @panic("TODO"),
                    .messages => |maybe_read_count| {
                        const read_count = maybe_read_count catch |err| return self.handleProduceError(io, err);
                        const messages = buffer[0..read_count];

                        for (messages) |message| {
                            const topic_messages = partitioned_messages.getOrPut(arena_allocator, message.producer_records.topic) catch |err| return self.handleProduceError(io, err);
                            if (!topic_messages.found_existing) {
                                topic_messages.value_ptr.* = .empty;
                            }

                            const partition_messages = topic_messages.value_ptr.getOrPut(arena_allocator, message.producer_records.partition_id) catch |err| return self.handleProduceError(io, err);
                            if (!partition_messages.found_existing) {
                                partition_messages.value_ptr.* = .empty;
                            }

                            const new_records = partition_messages.value_ptr.addManyAsSlice(arena_allocator, message.producer_records.records.len) catch |err| return self.handleProduceError(io, err);

                            for (message.producer_records.records, new_records, 0..) |record, *new_record, i| {
                                new_record.* = .{
                                    .timestamp_delta = 0,
                                    .offset_delta = @intCast(i),
                                    .key = record.key,
                                    .value = record.value,
                                    .headers = record.headers,
                                };
                            }
                        }

                        self.sendProduceRequests(io, &produce_arena, &partitioned_messages) catch |err| return self.handleProduceError(io, err);

                        for (messages) |message| {
                            message.semaphore.post(io);
                        }
                    },
                }
            }
        }

        fn sendProduceRequests(self: @This(), io: Io, arena: *std.heap.ArenaAllocator, partitioned_messages: *PartitionedMessages) !void {
            const arena_allocator = arena.allocator();
            defer _ = arena.reset(.retain_capacity);

            var topics: std.ArrayList(cluster_module.TopicNameAndPartition) = .empty;

            var topic_iterator = partitioned_messages.iterator();

            while (topic_iterator.next()) |topic_messages| {
                var partition_iterator = topic_messages.value_ptr.iterator();
                while (partition_iterator.next()) |partition_messages| {
                    try topics.append(arena_allocator, .{
                        .topic_name = topic_messages.key_ptr.*,
                        .partition_index = partition_messages.key_ptr.*,
                    });
                }
            }

            const topics_with_leader = try arena_allocator.alloc(cluster_module.TopicIdAndLeader, topics.items.len);
            defer arena_allocator.free(topics_with_leader);

            try self.cluster.leadersForTopicsAndPartitions(
                io,
                arena_allocator,
                topics.items,
                topics_with_leader,
            );

            var paritioned_record_sets: std.AutoHashMapUnmanaged(i32, std.AutoHashMapUnmanaged([16]u8, std.ArrayList(protocol.ProduceRequestV13.PartitionProduceData))) = .empty;

            topic_iterator = partitioned_messages.iterator();
            var index: usize = 0;

            const timestamp = Io.Timestamp.now(io, .real);
            while (topic_iterator.next()) |topic_messages| {
                var partition_iterator = topic_messages.value_ptr.iterator();
                while (partition_iterator.next()) |partition_messages| {
                    const leader = topics_with_leader[index];
                    index += 1;

                    std.debug.assert(leader.partition_index == partition_messages.key_ptr.*);
                    std.debug.assert(std.mem.eql(u8, leader.topic_name, topic_messages.key_ptr.*));

                    const broker_data = try paritioned_record_sets.getOrPut(arena_allocator, leader.leader_id);
                    if (!broker_data.found_existing) {
                        broker_data.value_ptr.* = .empty;
                    }

                    const topic_data = try broker_data.value_ptr.getOrPut(arena_allocator, leader.topic_id);

                    if (!topic_data.found_existing) {
                        topic_data.value_ptr.* = .empty;
                    }

                    var record_set: RecordSet = .{
                        .attributes = .{},
                        .base_sequence = -1,
                        .base_timestamp = timestamp.toMilliseconds(),
                        .last_offset_delta = @intCast(partition_messages.value_ptr.items.len - 1),
                        .max_timestamp = timestamp.toMilliseconds(),
                        .partition_leader_epoch = -1,
                        .producer_epoch = -1,
                        .producer_id = -1,
                        .records = partition_messages.value_ptr.items,
                    };

                    const data: protocol.ProduceRequestV13.PartitionProduceData = .{
                        .index = partition_messages.key_ptr.*,
                        .records = try record_set.serialise(arena_allocator),
                    };

                    try topic_data.value_ptr.append(arena_allocator, data);
                }
            }

            var group: std.Io.Group = .init;

            var request_iterator = paritioned_record_sets.iterator();

            while (request_iterator.next()) |broker_data| {
                var topic_data: std.ArrayList(protocol.ProduceRequestV13.TopicProduceData) = .empty;

                var data_iterator = broker_data.value_ptr.iterator();

                while (data_iterator.next()) |partition_data| {
                    try topic_data.append(arena_allocator, .{
                        .topic_id = partition_data.key_ptr.*,
                        .partition_data = partition_data.value_ptr.items,
                    });
                }

                const request: protocol.ProduceRequestV13 = .{
                    .acks = -1,
                    .timeout_ms = 20_000,
                    .topic_data = topic_data.items,
                };

                try group.concurrent(io, produceRequest, .{
                    self.cluster, io, arena_allocator, broker_data.key_ptr.*, request,
                });
            }

            try group.await(io);
        }

        fn produceRequest(
            cluster: *GenericCluster(ConnectionType),
            io: Io,
            allocator: std.mem.Allocator,
            leader_id: i32,
            request: protocol.ProduceRequestV13,
        ) error{Canceled}!void {
            const response = cluster.makeRequestNode(protocol.ProduceResponseV13, io, allocator, leader_id, request) catch |err| switch (err) {
                error.Canceled => return error.Canceled,
                else =>
                // todo errors
                return,
            };
            _ = response; // autofix
        }
    };
}

test "produce single message" {
    const topic_name = "test_topic";
    const partition_id = 11;
    const leader_id = 12;

    const Connection = FakeConnection(struct {
        fn mockMetadataRequest(
            arena: std.mem.Allocator,
        ) !*protocol.MetadataResponseV13 {
            const result = try arena.create(protocol.MetadataResponseV13);

            result.topics = &.{.{
                .error_code = .NONE,
                .name = topic_name,
                .partitions = &.{.{
                    .error_code = .NONE,
                    .partition_index = partition_id,
                    .replica_nodes = &.{},
                    .isr_nodes = &.{},
                    .offline_replicas = &.{},
                    .leader_id = leader_id,
                }},
                .topic_id = @splat(0),
            }};

            return result;
        }

        fn mockProduceRequest(arena: std.mem.Allocator) !*protocol.ProduceResponseV13 {
            const result = try arena.create(protocol.ProduceResponseV13);

            result.responses = &.{};

            return result;
        }

        pub fn mock(arena: std.mem.Allocator, request: anytype) !*anyopaque {
            const RequestType = @TypeOf(request);

            switch (RequestType) {
                protocol.MetadataRequestV13 => {
                    return @ptrCast(try mockMetadataRequest(arena));
                },
                protocol.ProduceRequestV13 => {
                    return @ptrCast(try mockProduceRequest(arena));
                },
                else => {
                    std.log.err("Unhandled request {any}", .{RequestType});
                    return error.Unimplemented;
                },
            }
        }
    });
    const Cluster = GenericCluster(Connection);
    const io = std.testing.io;
    const allocator = std.testing.allocator;

    var message_queue: [16]QueuedRecords = undefined;
    var cluster: Cluster = .init(allocator);
    defer cluster.deinit(io);
    const connection = try Connection.init(io, allocator, try Io.net.HostName.init("localhost"), 123);
    try cluster.connections.append(allocator, connection);
    try cluster.connection_node_map.put(allocator, leader_id, connection);
    var producer: GenericProducer(Connection) = .init(
        &cluster,
        &message_queue,
        .{},
    );
    defer producer.deinit(io);

    try producer.connect(io, allocator);

    const records: ProducerRecords = .{
        .topic = topic_name,
        .partition_id = partition_id,
        .records = &.{.{
            .key = "hello",
            .value = "world",
            .headers = &.{},
        }},
    };

    const SelectType = union(enum) {
        produce: error{ Canceled, ConnectionClosed }!void,
        timeout: error{Canceled}!void,
    };
    var select_buffer: [1]SelectType = undefined;
    var select: Io.Select(SelectType) = .init(io, &select_buffer);

    try select.concurrent(.produce, GenericProducer(Connection).produce, .{ &producer, io, records });
    try select.concurrent(.timeout, Io.sleep, .{ io, .fromSeconds(1), .real });

    defer select.cancelDiscard();

    switch (try select.await()) {
        .produce => {},
        .timeout => return error.Timeout,
    }
}

test "produce multiple messages" {
    return error.SkipZigTest;
}

test "produce batch" {
    return error.SkipZigTest;
}
