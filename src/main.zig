const std = @import("std");
const Io = std.Io;

const kafka_client = @import("kafka_client");

const protocol = kafka_client.protocol;

pub fn main(init: std.process.Init) !void {
    const arena = init.arena.allocator();
    const io = init.io;

    var cluster = kafka_client.Cluster.init();

    try cluster.bootstrap(io, arena, .single("localhost", 9092));
    defer cluster.deinit(io, arena);

    try testProduce(io, arena, &cluster);
}

fn testProduce(io: Io, arena: std.mem.Allocator, cluster: *kafka_client.Cluster) !void {
    var topics: [1]protocol.MetadataRequestV13.MetadataRequestTopic = .{.{
        .name = "test",
        .topic_id = @splat(0),
    }};

    const req = protocol.MetadataRequestV13{
        .topics = &topics,
        .include_topic_authorized_operations = false,
    };

    var metadata_response = try cluster.connections.items[0].broker_connection.makeRequest(protocol.MetadataResponseV13, io, arena, req);
    defer metadata_response.deinit();

    const value = metadata_response.value;

    std.debug.print("cluster id: {?s}, controller id: {}, throttle time: {}\n\n", .{ value.cluster_id, value.controller_id, value.throttle_time_ms });

    for (value.brokers) |broker| {
        std.debug.print("broker (node_id:{}) (rack:{?s}): {s}:{}\n", .{ broker.node_id, broker.rack, broker.host, broker.port });
    }

    std.debug.print("\n", .{});

    var leader_id: i32 = undefined;
    var topic_id: [16]u8 = undefined;
    var partition_index: i32 = undefined;

    for (value.topics) |topic| {
        std.debug.print("Error: {any}, ID: {x} - {?s}\n", .{ topic.error_code, topic.topic_id, topic.name });
        topic_id = topic.topic_id;

        for (topic.partitions) |partition| {
            std.debug.print("Partition {}: leader: {} in-sync: {any} replicas: {any}\n", .{
                partition.partition_index,
                partition.leader_id,
                partition.isr_nodes,
                partition.replica_nodes,
            });

            leader_id = partition.leader_id;
            partition_index = partition.partition_index;
        }

        std.debug.print("\n", .{});
    }

    const records: kafka_client.RecordSet = .{
        .partition_leader_epoch = 0,
        .attributes = .{},
        .last_offset_delta = 0,
        .base_timestamp = 0,
        .max_timestamp = 0,
        .producer_id = 0,
        .producer_epoch = 0,
        .base_sequence = 0,
        .records = &.{.{
            .timestamp_delta = 0,
            .offset_delta = 0,
            .key = "hello",
            .value = "world",
            .headers = &.{},
        }},
    };
    var partition_data: [1]protocol.ProduceRequestV13.PartitionProduceData = .{
        .{
            .index = partition_index,
            .records = try records.serialise(arena),
        },
    };
    var topic_data: [1]protocol.ProduceRequestV13.TopicProduceData = .{
        .{
            .topic_id = topic_id,
            .partition_data = &partition_data,
        },
    };
    const produce_request: protocol.ProduceRequestV13 = .{
        .acks = -1,
        .topic_data = &topic_data,
        .timeout_ms = 30_000,
    };

    const connection = cluster.connection_node_map.get(leader_id) orelse return error.NoConnection;
    var produce_response = try connection.broker_connection.makeRequest(protocol.ProduceResponseV13, io, arena, produce_request);
    defer produce_response.deinit();

    for (produce_response.value.responses) |response| {
        for (response.partition_responses) |pr| {
            std.debug.print("error: {any} - {?s} - leader: {any} \n", .{ pr.error_code, pr.error_message, pr.current_leader });
        }
    }
}

test {
    _ = @import("protocol_tests.zig");
}
