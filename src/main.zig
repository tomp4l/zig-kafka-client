const std = @import("std");
const Io = std.Io;

const kafka_client = @import("kafka_client");
const protocol = kafka_client.protocol;

pub fn main(init: std.process.Init) !void {
    const arena = init.arena.allocator();
    const io = init.io;

    var cluster = kafka_client.Cluster.init(arena);

    try cluster.bootstrap(io, arena, .single("localhost", 9092));
    defer cluster.deinit(io);

    try testProduce(io, arena, &cluster);
}

fn testProduce(io: Io, arena: std.mem.Allocator, cluster: *kafka_client.Cluster) !void {
    var message_queue_buffer: [128]kafka_client.QueuedRecords = undefined;
    var producer: kafka_client.Producer = .init(cluster, &message_queue_buffer, .{});
    defer producer.deinit(io);
    try producer.connect(io, arena);
    const records: kafka_client.ProducerRecords = .{
        .topic = "test",
        .partition_id = 0,
        .records = &.{.{
            .key = "hello",
            .value = "world",
            .headers = &.{},
        }},
    };

    try producer.produce(io, records);

    // for (produce_response.value.responses) |response| {
    //     for (response.partition_responses) |pr| {
    //         std.debug.print("error: {any} - {?s}\nleader: {any} - offset {}\n", .{ pr.error_code, pr.error_message, pr.current_leader, pr.base_offset });
    //     }
    // }
}

test {
    _ = @import("protocol_tests.zig");
}
