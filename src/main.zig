const std = @import("std");
const Io = std.Io;

const kafka_client = @import("kafka_client");

const protocol = kafka_client.protocol;

pub fn main(init: std.process.Init) !void {
    const arena = init.arena.allocator();
    const io = init.io;

    // const host_name = try Io.net.HostName.init("localhost");

    // const socket = try host_name.connect(init.io, 9092, .{ .mode = .stream });
    // defer socket.close(init.io);

    // var read_buf: [4096]u8 = undefined;
    // var write_buf: [4096]u8 = undefined;

    // var reader = socket.reader(init.io, &read_buf);
    // var writer = socket.writer(init.io, &write_buf);

    // var connection = kafka_client.BrokerConnection.init(&reader.interface, &writer.interface);
    // defer connection.deinit(io, arena);
    // try connection.connect(io, arena, "client_id");
    // try testMetadataRequest(io, arena, &connection);

    var cluster = kafka_client.Cluster.init();

    try cluster.bootstrap(io, arena, .single("localhost", 9092));
    defer cluster.deinit(io, arena);

    std.debug.print("made {} connections\n", .{cluster.node_map.count()});
}

fn testMetadataRequest(io: Io, arena: std.mem.Allocator, connection: *kafka_client.BrokerConnection) !void {
    var topics: [1]protocol.MetadataRequestV13.MetadataRequestTopic = .{.{
        .name = "test",
        .topic_id = @splat(0),
    }};

    const req = protocol.MetadataRequestV13{
        .topics = &topics,
        .include_topic_authorized_operations = false,
    };

    var response = try connection.makeRequest(protocol.MetadataResponseV13, io, arena, req);
    defer response.deinit();

    const value = response.value;

    std.debug.print("cluster id: {?s}, controller id: {}, throttle time: {}\n\n", .{ value.cluster_id, value.controller_id, value.throttle_time_ms });

    for (value.brokers) |broker| {
        std.debug.print("broker (node_id:{}) (rack:{?s}): {s}:{}\n", .{ broker.node_id, broker.rack, broker.host, broker.port });
    }

    std.debug.print("\n", .{});

    for (value.topics) |topic| {
        std.debug.print("Error: {any}, ID: {x} - {?s}\n", .{ topic.error_code, topic.topic_id, topic.name });

        for (topic.partitions) |partition| {
            std.debug.print("Partition {}: leader: {} in-sync: {any} replicas: {any}\n", .{
                partition.partition_index,
                partition.leader_id,
                partition.isr_nodes,
                partition.replica_nodes,
            });
        }

        std.debug.print("\n", .{});
    }
}

test {
    _ = @import("protocol_tests.zig");
}
