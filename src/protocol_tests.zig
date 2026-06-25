const std = @import("std");
const Io = std.Io;
const Writer = Io.Writer;
const AllocatingWriter = Writer.Allocating;

const protocol = @import("protocol");

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
