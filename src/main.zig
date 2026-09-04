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
    try testConsume(io, arena, &cluster);
}

fn testConsume(io: Io, arena: std.mem.Allocator, cluster: *kafka_client.Cluster) !void {
    std.debug.print("raw consuming test\n", .{});

    const request: kafka_client.protocol.FindCoordinatorRequestV6 = .{
        .coordinator_keys = &.{"test-group-3"},
    };

    outer: while (true) {
        var response = try cluster.makeRequestAny(kafka_client.protocol.FindCoordinatorResponseV6, io, arena, request);
        defer response.deinit();

        for (response.value.coordinators) |c| {
            // First topic needs to create some stuff
            // Will need some back-off/max retry, it could also be a misconfigured topic (too many brokers required)
            if (c.error_code == .COORDINATOR_NOT_AVAILABLE) {
                std.debug.print("sleeping for a bit while the group creates...\n", .{});
                try io.sleep(.fromMilliseconds(250), .real);
                continue :outer;
            }
        }
        std.debug.print("Coordinator response: {any}\n", .{response.value});
        break;
    }
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
}

test {
    _ = @import("protocol_tests.zig");
}
