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
    const topic_and_leader = try cluster.leaderForTopicPartition(io, arena, "test", 0);

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

    const produce_request: protocol.ProduceRequestV13 = .{
        .acks = -1,
        .topic_data = &.{.{
            .topic_id = topic_and_leader.topic_id,
            .partition_data = &.{.{
                .index = 0,
                .records = try records.serialise(arena),
            }},
        }},
        .timeout_ms = 30_000,
    };

    var produce_response = try cluster.makeRequestNode(
        protocol.ProduceResponseV13,
        io,
        arena,
        topic_and_leader.leader_id,
        produce_request,
    );
    defer produce_response.deinit();

    for (produce_response.value.responses) |response| {
        for (response.partition_responses) |pr| {
            std.debug.print("error: {any} - {?s}\nleader: {any} - offset {}\n", .{ pr.error_code, pr.error_message, pr.current_leader, pr.base_offset });
        }
    }
}

test {
    _ = @import("protocol_tests.zig");
}
