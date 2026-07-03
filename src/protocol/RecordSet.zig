const std = @import("std");

const protocol = @import("protocol");

const Io = std.Io;
const Crc = std.hash.crc.Crc32Iscsi;

pub const Compression = enum(u3) {
    none = 0,
    gzip = 1,
    snappy = 2,
    lz4 = 3,
    zstd = 4,
    _,
};

// bit 0~2:
//     0: no compression
//     1: gzip
//     2: snappy
//     3: lz4
//     4: zstd
// bit 3: timestampType
// bit 4: isTransactional (0 means not transactional)
// bit 5: isControlBatch (0 means not a control batch)
// bit 6: hasDeleteHorizonMs (0 means baseTimestamp is not set as the delete horizon for compaction)
// bit 7~15: unused
pub const Attributes = packed struct {
    compression: Compression = .none,
    brokerTimestamp: bool = false,
    isTransactional: bool = false,
    isControlBatch: bool = false,
    hasDeleteHorizonMs: bool = false,
    _padding: u9 = 0,

    fn toU16(self: Attributes) u16 {
        return @bitCast(self);
    }

    fn fromU16(value: u16) Attributes {
        return @bitCast(value);
    }
};

// baseOffset: int64
// batchLength: int32
// partitionLeaderEpoch: int32
// magic: int8 (current magic value is 2)
// crc: uint32
// attributes: int16
// lastOffsetDelta: int32
// baseTimestamp: int64
// maxTimestamp: int64
// producerId: int64
// producerEpoch: int16
// baseSequence: int32
// recordsCount: int32
// records: [Record]

partition_leader_epoch: i32,
attributes: Attributes,
last_offset_delta: i32,
base_timestamp: i64,
max_timestamp: i64,
producer_id: i64,
producer_epoch: i16,
base_sequence: i32,
records: []const Record,

fn writeVarLengthBytes(writer: *Io.Writer, maybe_value: ?[]const u8) !void {
    if (maybe_value) |value| {
        if (value.len > std.math.maxInt(u31)) return error.TooBig;
        try protocol.writeUnsignedVarInt(writer, zigZagEncode(@intCast(value.len)));
        try writer.writeAll(value);
    } else {
        try writer.writeByte(1); // -1
    }
}

// length: varint
// attributes: int8
//     bit 0~7: unused
// timestampDelta: varlong
// offsetDelta: varint
// keyLength: varint
// key: byte[]
// valueLength: varint
// value: byte[]
// headersCount: varint
// Headers => [Header]
pub const Record = struct {
    attributes: u8 = 0,
    timestamp_delta: i64,
    offset_delta: i32,
    key: ?[]const u8,
    value: ?[]const u8,
    headers: []const Header,

    // this does not encode the length!
    fn serialise(self: *const @This(), writer: *Io.Writer) !void {
        try writer.writeByte(self.attributes);
        try protocol.writeUnsignedVarInt(writer, zigZagEncode(self.timestamp_delta));
        try protocol.writeUnsignedVarInt(writer, zigZagEncode(self.offset_delta));

        try writeVarLengthBytes(writer, self.key);
        try writeVarLengthBytes(writer, self.value);
        if (self.headers.len > std.math.maxInt(u31)) return error.TooBig;
        try protocol.writeUnsignedVarInt(writer, zigZagEncode(@intCast(self.headers.len)));

        for (self.headers) |header| {
            try header.serialise(writer);
        }
    }
};

// headerKeyLength: varint
// headerKey: String
// headerValueLength: varint
// Value: byte[]
pub const Header = struct {
    header_key: []const u8,
    value: ?[]const u8,

    fn serialise(self: *const @This(), writer: *Io.Writer) !void {
        try writeVarLengthBytes(writer, self.header_key);
        try writeVarLengthBytes(writer, self.value);
    }
};

// This is so hidden in the docs FML
fn zigZagEncode(val: i64) u64 {
    return @bitCast((val << 1) ^ (val >> 63));
}

const PRE_LENGTH_OFFSET = 8;
const LENGTH_OFFSET = 12;
const PRE_CRC_HEADER_OFFSET = (64 + 32 + 32 + 8) / 8;
const CRC_HEADER_OFFSET = PRE_CRC_HEADER_OFFSET + 4;
const MAGIC_BYTE = 2;

// This has a different signature to the generated serialisers
// I could marry them up but:
//   - This needs to do CRC checksum so needs to allocate
//   - I'm storing this as a u8 on the generated types
//   - I haven't found any need to directly allocate for the other ones
pub fn serialise(self: *const @This(), allocator: std.mem.Allocator) ![]const u8 {
    var allocating: Io.Writer.Allocating = .init(allocator);
    defer allocating.deinit();
    var writer = &allocating.writer;

    try writer.writeInt(i64, 0, .big); // base offset
    try writer.writeInt(i32, 0, .big); // placeholder size
    try writer.writeInt(i32, self.partition_leader_epoch, .big);
    try writer.writeInt(i8, MAGIC_BYTE, .big);
    try writer.writeInt(u32, 0, .big); // placeholder crc
    try writer.writeInt(u16, self.attributes.toU16(), .big);
    try writer.writeInt(i32, self.last_offset_delta, .big);
    try writer.writeInt(i64, self.base_timestamp, .big);
    try writer.writeInt(i64, self.max_timestamp, .big);
    try writer.writeInt(i64, self.producer_id, .big);
    try writer.writeInt(i16, self.producer_epoch, .big);
    try writer.writeInt(i32, self.base_sequence, .big);
    if (self.records.len > std.math.maxInt(u31)) return error.TooBig;

    try writer.writeInt(i32, @intCast(self.records.len), .big);

    for (self.records) |record| {
        var discarding: Io.Writer.Discarding = .init(&.{});
        try record.serialise(&discarding.writer);
        const record_length = discarding.fullCount();
        if (record_length > std.math.maxInt(u31)) return error.TooBig;
        try protocol.writeUnsignedVarInt(writer, zigZagEncode(@intCast(record_length)));
        try record.serialise(writer);
    }

    const result = try allocating.toOwnedSlice();
    const length = result.len - LENGTH_OFFSET;
    if (length > std.math.maxInt(u31)) return error.TooBig;
    std.mem.writeInt(i32, result[PRE_LENGTH_OFFSET..LENGTH_OFFSET], @intCast(length), .big);
    const crc = Crc.hash(result[CRC_HEADER_OFFSET..]);
    std.mem.writeInt(u32, result[PRE_CRC_HEADER_OFFSET..CRC_HEADER_OFFSET], crc, .big);
    return result;
}

test "Attributes" {
    try std.testing.expectEqual(16, @bitSizeOf(Attributes));
}

test zigZagEncode {
    try std.testing.expectEqual(0, zigZagEncode(0));
    try std.testing.expectEqual(1, zigZagEncode(-1));
    try std.testing.expectEqual(2, zigZagEncode(1));
    try std.testing.expectEqual(3, zigZagEncode(-2));
    try std.testing.expectEqual(4, zigZagEncode(2));
}

test "serialise" {
    const set: @This() = .{
        .partition_leader_epoch = 1,
        .attributes = .{},
        .last_offset_delta = 2,
        .base_timestamp = 3,
        .max_timestamp = 4,
        .producer_id = 5,
        .producer_epoch = 6,
        .base_sequence = 7,
        .records = &.{.{
            .timestamp_delta = 8,
            .offset_delta = 9,
            .key = "a",
            .value = "b",
            .headers = &.{
                .{
                    .header_key = "c",
                    .value = "d",
                },
            },
        }},
    };

    const allocator = std.testing.allocator;

    const bytes = try set.serialise(allocator);
    defer allocator.free(bytes);

    const expected_bytes: []const u8 = &.{
        0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, // 8 bytes for crap
        0x00, 0x00, 0x00, 0x3E, // 62 (74-12) bytes
        0x00, 0x00, 0x00, 0x01, // 1 partition leader id
        0x02, // magic byte
        0xC8, 0xB7, 0xE8, 0x7D, // crc - I hope this is right
        0x00, 0x00, // attributes
        0x00, 0x00, 0x00, 0x02, // last offset delta
        0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x03, // base timestamp
        0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x04, // max timestamp
        0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x05, // producer id
        0x00, 0x06, // producer epoch
        0x00, 0x00, 0x00, 0x07, // base sequence
        0x00, 0x00, 0x00, 0x01, // number of records
        0x18, // size of record
        0x00, // record attributes
        0x10, // timestamp delta (8 ziggy zaggy)
        0x12, // offset delta (9 zaggy ziggy)
        0x02, // length 1 zigzag
        'a',
        0x02, // length 1 zigzag
        'b',
        0x02, // count 1 header zigzag
        0x02, // length 1 zigzag
        'c',
        0x02, // length 1 zigzag
        'd',
    };

    try std.testing.expectEqualSlices(u8, expected_bytes, bytes);
}
