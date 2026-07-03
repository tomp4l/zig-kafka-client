const std = @import("std");
const Io = std.Io;
const Writer = Io.Writer;
const AllocatingWriter = Writer.Allocating;

const protocol = @import("kafka_client").protocol;

test {
    std.testing.refAllDecls(protocol);
}

test "ApiVersionsRequestV5" {
    const request = protocol.ApiVersionsRequestV5{
        .client_software_name = "abc",
        .client_software_version = "1",
        .cluster_id = null,
    };
    const allocator = std.testing.allocator;
    var allocating: AllocatingWriter = .init(allocator);
    defer allocating.deinit();

    try request.serialise(&allocating.writer);

    const bytes = allocating.written();

    const expected_bytes = &[_]u8{
        0x04, 0x61, 0x62, 0x63, // "abc"
        0x02, 0x31, // "1"
        0x00, // null (cluster_id)
        0xFF, 0xFF, 0xFF, 0xFF, // -1 (node_id)
        0x00, // empty tagged fields buffer
    };

    try std.testing.expectEqualSlices(u8, expected_bytes, bytes);

    try std.testing.expectEqual(18, protocol.ApiVersionsRequestV5.api_key);
    try std.testing.expectEqual(true, protocol.ApiVersionsRequestV5.is_flexible);
    try std.testing.expectEqual(5, protocol.ApiVersionsRequestV5.version);
}

test "ApiVersionsResponseV0" {
    const allocator = std.testing.allocator;
    const mock_payload = [_]u8{
        0x00, 0x00, // 1. error_code: i16
        0x00, 0x00, 0x00, 0x01, // 2. api_keys array length: i32 (1 item)

        // --- ApiVersion[0] ---
        // 3. api_key: i16 (18 = ApiVersions API)
        0x00, 0x12,
        // 4. min_version: i16 (0)
        0x00, 0x00,
        // 5. max_version: i16 (5)
        0x00, 0x05,
    };

    // Parse the bytes
    const response = try protocol.ApiVersionsResponseV0.deserialise(allocator, &mock_payload);
    defer allocator.free(response.api_keys);

    // Verify the scalar field
    try std.testing.expectEqual(protocol.ResponseError.NONE, response.error_code);

    // Verify the array allocation and lengths
    const keys = response.api_keys;
    try std.testing.expectEqual(@as(usize, 1), keys.len);

    // Verify the deeply nested struct parsing
    try std.testing.expectEqual(@as(i16, 18), keys[0].api_key);
    try std.testing.expectEqual(@as(i16, 0), keys[0].min_version);
    try std.testing.expectEqual(@as(i16, 5), keys[0].max_version);
}

test "ApiVersionsResponseV3" {
    const allocator = std.testing.allocator;
    const mock_payload = [_]u8{
        0x00, 0x00, // 1. error_code: i16 (0)

        0x02, // 2. api_keys compact array: (1 item -> encoded as length + 1 = 2)
        // --- ApiVersion[0] ---
        0x00, 0x12, // api_key (18)
        0x00, 0x00, // min_version (0)
        0x00, 0x05, // max_version (5)
        0x00, // TAG BUFFER (0 tags) for the ApiVersion struct
        // --- End ApiVersion[0] ---

        0x00, 0x00, 0x00, 0x00, // 3. throttle_time_ms: i32 (0)

        // ==========================================
        // MAIN TAG BUFFER
        // We will send EXACTLY ONE tag: zk_migration_ready (Tag ID = 3)
        // ==========================================

        0x01, // Num Tags: 1

        0x03, // Tag ID: 3 (zk_migration_ready)
        0x01, // Tag Length: 1 byte
        0x01, // Tag Data: 0x01 (true)
    };

    // Parse the bytes
    const response = try protocol.ApiVersionsResponseV3.deserialise(allocator, &mock_payload);
    defer allocator.free(response.api_keys);

    // Verify the scalar field
    try std.testing.expectEqual(protocol.ResponseError.NONE, response.error_code);

    // Verify the array allocation and lengths
    const keys = response.api_keys;
    try std.testing.expectEqual(@as(usize, 1), keys.len);

    // Verify the deeply nested struct parsing
    try std.testing.expectEqual(@as(i16, 18), keys[0].api_key);
    try std.testing.expectEqual(@as(i16, 0), keys[0].min_version);
    try std.testing.expectEqual(@as(i16, 5), keys[0].max_version);

    try std.testing.expectEqual(0, response.finalized_features.len);
    try std.testing.expectEqual(true, response.zk_migration_ready);
}

test "MetadataRequestV13" {
    var topics: [1]protocol.MetadataRequestV13.MetadataRequestTopic = .{.{
        .topic_id = @splat(0),
        .name = "topic",
    }};
    const request = protocol.MetadataRequestV13{
        .topics = &topics,
        .allow_auto_topic_creation = false,
        .include_topic_authorized_operations = true,
    };

    const allocator = std.testing.allocator;
    var allocating: AllocatingWriter = .init(allocator);
    defer allocating.deinit();

    try request.serialise(&allocating.writer);

    const bytes = allocating.written();

    const expected_bytes = &[_]u8{
        0x02, // length 1 + 1
        0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, // 1st 8 bytes uuid
        0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, // 2nd 8 bytes uuid
        0x06, // length 5 + 1
        't', 'o', 'p', 'i', 'c', // topic name
        0x00, // no tagged fields
        0x00, // false
        0x01, // true
        0x00, // empty tagged fields buffer
    };

    try std.testing.expectEqualSlices(u8, expected_bytes, bytes);
}

test "ProduceRequestV13" {
    var partition_data: [1]protocol.ProduceRequestV13.PartitionProduceData = .{
        .{
            .index = 1,
            .records = "records",
        },
    };

    var topic_data: [1]protocol.ProduceRequestV13.TopicProduceData = .{
        .{
            .topic_id = @splat(0),
            .partition_data = &partition_data,
        },
    };

    const request: protocol.ProduceRequestV13 = .{
        .acks = -1,
        .timeout_ms = 1000,
        .topic_data = &topic_data,
    };

    const allocator = std.testing.allocator;
    var allocating: AllocatingWriter = .init(allocator);
    defer allocating.deinit();

    try request.serialise(&allocating.writer);

    const bytes = allocating.written();

    const expected_bytes = &[_]u8{
        0x00, // null transaction ID
        0xFF, 0xFF, // -1 acks (all)
        0x00, 0x00, 0x03, 0xE8, // 1000ms timeout
        0x02, // 1 producer record (1+1)
        0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, // uuid
        0x02, // 1 partition record (1+1)
        0x00, 0x00, 0x00, 0x01, // id 1
        0x08, // record length
        'r', 'e', 'c', 'o', 'r', 'd', 's', // data
        0x00, // 0 tags (partition)
        0x00, // 0 tags (producer)
        0x00, // 0 tags
    };

    try std.testing.expectEqualSlices(u8, expected_bytes, bytes);
}

test "ProduceResponseV13" {
    const allocator = std.testing.allocator;
    const mock_payload = [_]u8{
        0x01, // empty responses
        0x00, 0x00, 0x00, 0x10, // 16 throttle time
        0x00, // no tags
    };

    // Parse the bytes
    const response = try protocol.ProduceResponseV13.deserialise(allocator, &mock_payload);

    // Verify the scalar field
    try std.testing.expectEqual(16, response.throttle_time_ms);

    try std.testing.expectEqualSlices(protocol.ProduceResponseV13.TopicProduceResponse, &.{}, response.responses);
    try std.testing.expectEqualSlices(protocol.ProduceResponseV13.NodeEndpoint, &.{}, response.node_endpoints);
}
