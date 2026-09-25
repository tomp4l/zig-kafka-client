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

    const group_id = "test-group";

    var coordinator_node_id: i32 = undefined;
    outer: while (true) {
        const request: kafka_client.protocol.FindCoordinatorRequestV6 = .{
            .coordinator_keys = &.{group_id},
        };

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

            if (std.mem.eql(u8, c.key, group_id)) {
                if (c.error_code == .NONE) {
                    coordinator_node_id = c.node_id;
                    break :outer;
                } else {
                    std.debug.print("bad response {} - {?s}", .{ c.error_code, c.error_message });
                    return error.BadResponse;
                }
            }
        }
        std.debug.print("Coordinator response: {any}\n", .{response.value});
        return error.GroupNotFound;
    }

    var member_id: []const u8 = undefined;
    var generation_id: i32 = undefined;
    var is_leader = false;

    {
        var request: kafka_client.protocol.JoinGroupRequestV9 = .{
            .group_id = group_id,
            .session_timeout_ms = 45_000, // This is Java default
            .rebalance_timeout_ms = 300_000, // de
            .member_id = "", // inital request sends empty
            .protocol_type = "consumer_zig",
            .protocols = &.{.{
                .name = "test",
                .metadata = "",
            }},
        };

        var response_no_member_id = try cluster.makeRequestNode(kafka_client.protocol.JoinGroupResponseV9, io, arena, coordinator_node_id, request);
        defer response_no_member_id.deinit();

        if (response_no_member_id.value.error_code != .MEMBER_ID_REQUIRED) {
            std.debug.print("Join group response: {any}\n", .{response_no_member_id.value});
            return error.UnexpectedError;
        }

        member_id = try arena.dupe(u8, response_no_member_id.value.member_id);
        request.member_id = member_id;

        var response = try cluster.makeRequestNode(kafka_client.protocol.JoinGroupResponseV9, io, arena, coordinator_node_id, request);
        defer response.deinit();

        if (response.value.error_code != .NONE) {
            std.debug.print("Join group response: {any}\n", .{response_no_member_id.value});
            return error.UnexpectedError;
        }

        generation_id = response.value.generation_id;
        is_leader = std.mem.eql(u8, member_id, response.value.leader);
    }

    {
        const request: kafka_client.protocol.SyncGroupRequestV5 = .{
            .group_id = group_id,
            .generation_id = generation_id,
            .member_id = member_id,
            .protocol_type = "consumer_zig",
            .protocol_name = "test",
            .assignments = if (is_leader) &.{.{
                .member_id = member_id,
                .assignment = "",
            }} else &.{},
        };

        var response = try cluster.makeRequestNode(kafka_client.protocol.SyncGroupResponseV5, io, arena, coordinator_node_id, request);
        defer response.deinit();

        std.debug.print("Sync response:\n", .{});
        inline for (@typeInfo(@TypeOf(response.value)).@"struct".fields) |field| {
            if (comptime std.mem.eql(u8, "assignment", field.name)) {
                std.debug.print("{s}: {s}\n", .{ field.name, @field(response.value, field.name) });
            } else if (comptime std.mem.eql(u8, "protocol_type", field.name) or std.mem.eql(u8, "protocol_name", field.name)) {
                std.debug.print("{s}: {?s}\n", .{ field.name, @field(response.value, field.name) });
            } else {
                std.debug.print("{s}: {any}\n", .{ field.name, @field(response.value, field.name) });
            }
        }

        if (response.value.error_code != .NONE) {
            return error.SyncIssue;
        }
    }
    const topic = cluster.topic_config.get("test") orelse return error.BadTopic;
    const partition = topic.partitions.get(0) orelse return error.BadTopic;
    var fetch_offset: i64 = undefined;
    {
        const request: kafka_client.protocol.OffsetFetchRequestV10 =
            .{
                .groups = &.{.{
                    .group_id = group_id,
                    .topics = &.{
                        .{
                            .topic_id = topic.topic_id,
                            .partition_indexes = &.{0},
                        },
                    },
                }},
            };

        var response = try cluster.makeRequestNode(kafka_client.protocol.OffsetFetchResponseV10, io, arena, coordinator_node_id, request);
        defer response.deinit();

        std.debug.print("\nOffet Response:\n", .{});
        for (response.value.groups) |group| {
            std.debug.print("group: {s}\n", .{group.group_id});
            for (group.topics) |offset_topic| {
                std.debug.print("Topic: {s}\n", .{std.fmt.bytesToHex(offset_topic.topic_id, .lower)});
                for (offset_topic.partitions) |p| {
                    std.debug.print("Partition: {} - {} - {}\n", .{ p.partition_index, p.committed_leader_epoch, p.committed_offset });

                    if (p.committed_offset > 0) {
                        fetch_offset = p.committed_offset;
                    } else {
                        fetch_offset = 0;
                    }
                }
            }
        }
    }

    var offset_to_commit: i64 = undefined;
    {
        const leader_id = partition.leader_id;
        const request: kafka_client.protocol.FetchRequestV18 = .{
            .max_wait_ms = 500,
            .min_bytes = 1,
            .topics = &.{.{
                .topic_id = topic.topic_id,
                .partitions = &.{.{
                    .partition = 0,
                    .fetch_offset = fetch_offset,
                    .partition_max_bytes = 1_000_000,
                    .replica_directory_id = @splat(0),
                }},
            }},
            .forgotten_topics_data = &.{},
            .replica_state = .{},
        };

        var response = try cluster.makeRequestNode(kafka_client.protocol.FetchResponseV18, io, arena, leader_id, request);
        defer response.deinit();

        std.debug.print("\n{any}\n", .{response.value});

        for (response.value.responses) |r| {
            for (r.partitions) |p| {
                std.debug.print("Record error: {any}\n", .{p.error_code});

                const records = try kafka_client.RecordSet.deserialiseAll(arena, p.records orelse &.{});

                for (records) |record_set| {
                    std.debug.print("Record data: {any}\n", .{record_set});

                    for (record_set.records) |record| {
                        offset_to_commit = record_set.base_offset + record.offset_delta;
                    }
                }
            }
        }
    }

    {
        const request: kafka_client.protocol.HeartbeatRequestV4 = .{
            .generation_id = generation_id,
            .member_id = member_id,
            .group_id = group_id,
        };

        var response = try cluster.makeRequestNode(kafka_client.protocol.HeartbeatResponseV4, io, arena, coordinator_node_id, request);
        defer response.deinit();

        std.debug.print("\nheartbeat {any}\n", .{response.value});
    }

    {
        const request: kafka_client.protocol.OffsetCommitRequestV10 = .{
            .member_id = member_id,
            .group_id = group_id,
            .generation_id_or_member_epoch = generation_id,
            .topics = &.{
                .{
                    .topic_id = topic.topic_id,
                    .partitions = &.{
                        .{
                            .partition_index = 0,
                            .committed_offset = offset_to_commit,
                            .committed_metadata = null,
                        },
                    },
                },
            },
        };

        var response = try cluster.makeRequestNode(kafka_client.protocol.OffsetCommitResponseV10, io, arena, coordinator_node_id, request);
        defer response.deinit();

        std.debug.print("\ncommit\n", .{});

        for (response.value.topics) |offset_commit_topic| {
            std.debug.print("topic {any}\n", .{offset_commit_topic});
        }
    }

    {
        const request: kafka_client.protocol.LeaveGroupRequestV5 = .{
            .group_id = group_id,
            .members = &.{
                .{
                    .member_id = member_id,
                },
            },
        };

        var response = try cluster.makeRequestNode(kafka_client.protocol.LeaveGroupResponseV5, io, arena, coordinator_node_id, request);
        defer response.deinit();

        std.debug.print("\nleft group {any}\n", .{response.value});
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
