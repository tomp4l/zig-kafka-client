const std = @import("std");
const Io = std.Io;
const Crc = std.hash.crc.Crc32Iscsi;

const protocol = @import("protocol");

const Self = @This();

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

base_offset: i64,
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

pub fn readUnsignedVarInt(reader: *Io.Reader) !usize {
    var value: usize = 0;
    var shift: u6 = 0;

    while (reader.takeByte() catch null) |byte| {
        value |= @as(usize, byte & 0x7F) << shift;

        if ((byte & 0x80) == 0) {
            return value;
        }

        if (shift >= 63) return error.VarIntTooBig;
        shift += 7;
    }

    return error.TooShort;
}

fn readVarLengthBytes(allocator: std.mem.Allocator, reader: *Io.Reader) !?[]const u8 {
    const length = zigZagDecode(@intCast(try readUnsignedVarInt(reader)));

    if (length == -1) {
        return null;
    }
    if (length < 0) {
        return error.InvalidLength;
    }

    return try reader.readAlloc(allocator, @intCast(length));
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

    // decodes including skipping the length!
    fn deserialise(allocator: std.mem.Allocator, reader: *Io.Reader) !Record {
        const record_len: i32 = @intCast(zigZagDecode(@intCast(try readUnsignedVarInt(reader))));
        _ = record_len;

        const attributes = try reader.takeByte();
        const timestamp_delta = zigZagDecode(@intCast(try readUnsignedVarInt(reader)));
        const offset_delta: i32 = @intCast(zigZagDecode(@intCast(try readUnsignedVarInt(reader))));
        const key = try readVarLengthBytes(allocator, reader);
        errdefer if (key) |k| allocator.free(k);
        const value = try readVarLengthBytes(allocator, reader);
        errdefer if (value) |v| allocator.free(v);

        const header_len: i32 = @intCast(zigZagDecode(@intCast(try readUnsignedVarInt(reader))));
        if (header_len < 0) return error.InvalidHeaderLength;
        var headers: std.ArrayList(Header) = try .initCapacity(allocator, @intCast(header_len));
        errdefer {
            for (headers.items) |h| {
                allocator.free(h.header_key);
                if (h.value) |v| allocator.free(v);
            }
            headers.deinit(allocator);
        }

        for (0..@intCast(header_len)) |i| {
            _ = i;

            const header_key = try readVarLengthBytes(allocator, reader);
            errdefer if (header_key) |k| allocator.free(k);
            const header_value = try readVarLengthBytes(allocator, reader);
            errdefer if (header_value) |v| allocator.free(v);

            try headers.appendBounded(.{
                .header_key = header_key orelse return error.NullHeaderKey,
                .value = header_value,
            });
        }

        return .{
            .attributes = attributes,
            .timestamp_delta = timestamp_delta,
            .offset_delta = offset_delta,
            .key = key,
            .value = value,
            .headers = try headers.toOwnedSlice(allocator),
        };
    }

    fn deinit(self: Record, allocator: std.mem.Allocator) void {
        if (self.key) |k| allocator.free(k);
        if (self.value) |k| allocator.free(k);
        for (self.headers) |h| {
            allocator.free(h.header_key);
            if (h.value) |v| allocator.free(v);
        }
        allocator.free(self.headers);
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

fn zigZagDecode(val: u64) i64 {
    return @bitCast((val >> 1) ^ (~(val & 1) +% 1));
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

    try writer.writeInt(i64, self.base_offset, .big); // base offset
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

pub fn deserialise(allocator: std.mem.Allocator, bytes: []const u8, consumed: *usize) !Self {
    var reader = std.Io.Reader.fixed(bytes);
    var self: Self = undefined;

    self.base_offset = try reader.takeInt(i64, .big);
    const message_size = try reader.takeInt(i32, .big);
    consumed.* = LENGTH_OFFSET + @as(usize, @intCast(message_size));
    self.partition_leader_epoch = try reader.takeInt(i32, .big);

    if (try reader.takeInt(i8, .big) != MAGIC_BYTE) return error.InvalidMagic;
    const crc = try reader.takeInt(u32, .big);
    _ = crc; // assume correct for now
    self.attributes = Attributes.fromU16(try reader.takeInt(u16, .big));
    self.last_offset_delta = try reader.takeInt(i32, .big);
    self.base_timestamp = try reader.takeInt(i64, .big);
    self.max_timestamp = try reader.takeInt(i64, .big);
    self.producer_id = try reader.takeInt(i64, .big);
    self.producer_epoch = try reader.takeInt(i16, .big);
    self.base_sequence = try reader.takeInt(i32, .big);

    const records_len = try reader.takeInt(i32, .big);

    var records: std.ArrayList(Record) = try .initCapacity(allocator, @intCast(records_len));
    errdefer {
        for (records.items) |r| {
            r.deinit(allocator);
        }
        records.deinit(allocator);
    }

    for (0..@intCast(records_len)) |i| {
        _ = i;

        const record = try Record.deserialise(allocator, &reader);
        errdefer record.deinit(allocator);
        try records.appendBounded(record);
    }

    self.records = try records.toOwnedSlice(allocator);
    return self;
}

pub fn deinit(self: *Self, allocator: std.mem.Allocator) void {
    for (self.records) |record| {
        record.deinit(allocator);
    }
    allocator.free(self.records);
}

pub fn deserialiseAll(allocator: std.mem.Allocator, bytes: []const u8) ![]const Self {
    var records: std.ArrayList(Self) = .empty;
    errdefer {
        for (records.items) |*r| {
            r.deinit(allocator);
        }
        records.deinit(allocator);
    }

    var offset: usize = 0;

    while (offset < bytes.len) {
        var consumed: usize = undefined;
        var record = try deserialise(allocator, bytes[offset..], &consumed);
        errdefer record.deinit(allocator);
        offset += consumed;
        try records.append(allocator, record);
    }

    return try records.toOwnedSlice(allocator);
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

test zigZagDecode {
    try std.testing.expectEqual(0, zigZagDecode(0));
    try std.testing.expectEqual(-1, zigZagDecode(1));
    try std.testing.expectEqual(1, zigZagDecode(2));
    try std.testing.expectEqual(-2, zigZagDecode(3));
    try std.testing.expectEqual(2, zigZagDecode(4));
}

test "serialise" {
    const set: @This() = .{
        .base_offset = 0,
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

test "deserialise" {
    var arena = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena.deinit();
    const allocator = arena.allocator();

    const bytes_hex = "0000000000000000000000420000000002eae98244000000000000000001a0a4b7f0d4000001a0a4b7f0d4ffffffffffffffffffffffffffff00000001200000000a68656c6c6f0a776f726c640000000000000000010000004200000000026ec12411000000000000000001a0a4b99a21000001a0a4b99a21ffffffffffffffffffffffffffff00000001200000000a68656c6c6f0a776f726c640000000000000000020000004200000000024696f7af000000000000000001a0a4bdc3b6000001a0a4bdc3b6ffffffffffffffffffffffffffff00000001200000000a68656c6c6f0a776f726c64000000000000000003000000420000000002ea75161a000000000000000001a0a4c0528d000001a0a4c0528dffffffffffffffffffffffffffff00000001200000000a68656c6c6f0a776f726c640000000000000000040000004200000000029467593c000000000000000001a0a4c0f75a000001a0a4c0f75affffffffffffffffffffffffffff00000001200000000a68656c6c6f0a776f726c6400";
    var bytes_buffer: [bytes_hex.len / 2]u8 = undefined;
    const bytes = try std.fmt.hexToBytes(&bytes_buffer, bytes_hex);

    var first_message_size: usize = undefined;
    const first_record = try deserialise(allocator, bytes, &first_message_size);

    try std.testing.expectEqual(first_record.partition_leader_epoch, 0);

    try std.testing.expectEqual(5, (try deserialiseAll(allocator, bytes)).len);
}
