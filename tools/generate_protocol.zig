const std = @import("std");
const Io = std.Io;

pub fn main(init: std.process.Init) !void {
    const io = init.io;
    const arena = init.arena.allocator();

    const args = try init.minimal.args.toSlice(arena);

    if (args.len != 2) fatal("wrong number of arguments", .{});

    const output_file_path = args[1];

    var output_file = Io.Dir.cwd().createFile(io, output_file_path, .{}) catch |err| {
        fatal("unable to open '{s}': {s}", .{ output_file_path, @errorName(err) });
    };

    std.log.debug("protocol out file: {s}", .{output_file_path});

    generateAllStructs(io, arena, output_file) catch |err| {
        fatal("unable to generate files '{s}': {s}", .{ output_file_path, @errorName(err) });
    };

    output_file.close(io);

    formatFile(io, output_file_path) catch |err| {
        fatal("unable to format files '{s}': {s}", .{ output_file_path, @errorName(err) });
    };

    return std.process.cleanExit(io);
}

fn formatFile(io: Io, output_file_path: []const u8) !void {
    std.log.debug("Formatting generated file: {s}", .{output_file_path});

    var fmt_result = try std.process.spawn(io, .{
        .argv = &.{ "zig", "fmt", output_file_path },
    });

    const term = try fmt_result.wait(io);

    switch (term) {
        .exited => |code| {
            if (code != 0) {
                std.log.debug("zig fmt exited with code {}", .{code});
                return error.NonZeroExit;
            }
        },
        else => return error.FmtCrashed,
    }

    std.log.debug("Successfully formatted protocol file.", .{});
}

const ProtocolField = struct {
    name: []const u8,
    type: []const u8,
    versions: []const u8,
    ignorable: bool = false,
    about: []const u8,
    fields: []ProtocolField = &.{},
    tag: ?usize = null,
    taggedVersions: ?[]const u8 = null,
    nullableVersions: ?[]const u8 = null,
    mapKey: bool = false,
    default: ?std.json.Value = null,

    // generated field
    snake_name: []const u8 = undefined,

    fn populateGeneratedFields(self: *ProtocolField, arena: std.mem.Allocator) !void {
        self.snake_name = try toSnakeCase(arena, self.name);

        for (self.fields) |*field| {
            try field.populateGeneratedFields(arena);
        }
    }
};

const VersionRange = union(enum) {
    none,
    single: usize,
    range: struct { start: usize, end: usize },
    open: usize,

    fn parse(input: []const u8) !@This() {
        if (std.mem.eql(u8, "none", input)) {
            return .none;
        }
        if (std.mem.find(u8, input, "+")) |i| {
            const parsed = try std.fmt.parseInt(usize, input[0..i], 10);
            return .{ .open = parsed };
        }
        var split = std.mem.splitScalar(u8, input, '-');

        const start = if (split.next()) |n|
            try std.fmt.parseInt(usize, n, 10)
        else
            return error.EmptyInput;
        // no range so use start
        if (split.next()) |n| {
            const end = try std.fmt.parseInt(usize, n, 10);
            return .{ .range = .{ .start = start, .end = end } };
        } else return .{ .single = start };
    }

    fn contains(self: @This(), version: usize) bool {
        return switch (self) {
            .none => false,
            .single => |v| v == version,
            .range => |r| version >= r.start and version <= r.end,
            .open => |start| version >= start,
        };
    }
};

const ProtocolJson = struct {
    apiKey: usize,
    type: []const u8,
    listeners: []const []const u8 = &.{},
    name: []const u8,
    validVersions: []const u8,
    flexibleVersions: []const u8,
    latestVersionUnstable: bool = false,
    fields: []ProtocolField,

    const VersionsIterator = struct {
        next_version: usize,
        max_version: usize,
        fn next(self: *@This()) ?usize {
            if (self.next_version > self.max_version) {
                self.next_version = std.math.maxInt(usize);
                return null;
            }
            const ret = self.next_version;
            self.next_version += 1;
            return ret;
        }
    };

    fn validVersionsIterator(self: *const @This()) !VersionsIterator {
        switch (try VersionRange.parse(self.validVersions)) {
            .none => return .{
                .next_version = std.math.maxInt(usize),
                .max_version = 0,
            },
            .open => return error.UnexpectedOpenRange,
            .range => |r| return .{ .next_version = r.start, .max_version = r.end },
            .single => |v| return .{ .next_version = v, .max_version = v },
        }
    }

    fn populateGeneratedFields(self: *ProtocolJson, arena: std.mem.Allocator) !void {
        for (self.fields) |*field| {
            try field.populateGeneratedFields(arena);
        }
    }
};

fn generateAllStructs(io: Io, arena: std.mem.Allocator, output_file: Io.File) !void {
    try output_file.writeStreamingAll(io, "const std = @import(\"std\");");

    try output_file.writeStreamingAll(io,
        \\ fn writeUnsignedVarInt(writer: *std.Io.Writer, value: usize) !void {
        \\     var temp = value;
        \\     while (true) {
        \\         var byte: u8 = @intCast(temp & 0x7F);
        \\ 
        \\         temp >>= 7;
        \\         if (temp != 0) {
        \\             byte |= 0x80;
        \\             try writer.writeByte(byte);
        \\         } else {
        \\             try writer.writeByte(byte);
        \\             break;
        \\         }
        \\     }
        \\ } 
    );

    var inputs = try Io.Dir.cwd().openDir(io, "protocol", .{ .iterate = true });
    std.log.debug("protocol input dir: {s}", .{try inputs.realPathFileAlloc(io, ".", arena)});

    var iter = inputs.iterate();

    var buffer: [1024]u8 = undefined;

    while (try iter.next(io)) |file| {
        std.log.debug("protocol file: {s}", .{file.name});

        const input = try inputs.openFile(io, file.name, .{ .allow_directory = false });
        var reader = input.readerStreaming(io, &buffer);

        var file_content: std.ArrayList(u8) = .empty;

        while (true) {
            const line = reader.interface.takeDelimiterExclusive('\n') catch |err| switch (err) {
                error.EndOfStream => break,
                else => return err,
            };
            reader.interface.toss(1);

            var is_comment = false;
            for (line) |c| {
                if (c == '/') {
                    if (is_comment) {
                        break;
                    }
                    is_comment = true;
                } else {
                    is_comment = false;
                    if (c != ' ') {
                        break;
                    }
                }
            }

            if (is_comment) {
                continue;
            }

            try file_content.appendSlice(arena, line);
        }

        var scanner = std.json.Scanner.initCompleteInput(arena, file_content.items);
        var diagnostics = std.json.Scanner.Diagnostics{};
        scanner.enableDiagnostics(&diagnostics);
        var json = std.json.parseFromTokenSourceLeaky(ProtocolJson, arena, &scanner, .{
            .ignore_unknown_fields = false,
            .parse_numbers = true,
        }) catch |err| {
            std.log.err("failed to parse json '{s}': {}", .{ file_content.items[diagnostics.getByteOffset() -| 20 .. diagnostics.getByteOffset() + 20], err });
            std.log.err("                                          ^", .{});

            return err;
        };

        try json.populateGeneratedFields(arena);

        try generateStructVersions(io, arena, json, output_file);
    }
}

fn generateStructVersions(io: Io, arena: std.mem.Allocator, protocol_json: ProtocolJson, output_file: Io.File) !void {
    var version_it = try protocol_json.validVersionsIterator();
    while (version_it.next()) |version| {
        try generateStructVersion(io, arena, protocol_json, version, output_file);
    }
}

fn mapKafkaType(kafka_type: []const u8) []const u8 {
    if (std.mem.eql(u8, kafka_type, "int8")) return "i8";
    if (std.mem.eql(u8, kafka_type, "int16")) return "i16";
    if (std.mem.eql(u8, kafka_type, "int32")) return "i32";
    if (std.mem.eql(u8, kafka_type, "int64")) return "i64";
    if (std.mem.eql(u8, kafka_type, "bool")) return "bool";
    if (std.mem.eql(u8, kafka_type, "string")) return "[]const u8";
    if (std.mem.eql(u8, kafka_type, "bytes")) return "[]const u8";
    if (std.mem.eql(u8, kafka_type, "records")) return "[]const u8";
    if (std.mem.eql(u8, kafka_type, "uuid")) return "[16]u8";
    if (std.mem.eql(u8, kafka_type, "float64")) return "f64";

    // Fallback for custom nested arrays (e.g. "[]Topic")
    return kafka_type;
}

fn toSnakeCase(arena: std.mem.Allocator, input: []const u8) ![]const u8 {
    var out: std.ArrayList(u8) = .empty;

    for (input, 0..) |c, i| {
        if (std.ascii.isUpper(c)) {
            if (i > 0) try out.append(arena, '_');
            try out.append(arena, std.ascii.toLower(c));
        } else {
            try out.append(arena, c);
        }
    }
    return out.toOwnedSlice(arena);
}

fn generateStructVersion(
    io: Io,
    arena: std.mem.Allocator,
    protocol_json: ProtocolJson,
    version: usize,
    output_file: Io.File,
) !void {

    // shared buffer for all formatting
    var print_buffer: [256]u8 = undefined;
    const name = try std.fmt.bufPrint(&print_buffer, "{s}V{}", .{ protocol_json.name, version });

    std.log.debug("generating {s}", .{name});

    const flexible_versions = try VersionRange.parse(protocol_json.flexibleVersions);
    const is_flexible = flexible_versions.contains(version);
    const is_request = std.mem.eql(u8, "request", protocol_json.type);

    try output_file.writeStreamingAll(io, "pub const ");
    try output_file.writeStreamingAll(io, name);
    try output_file.writeStreamingAll(io, "= struct {");

    try output_file.writeStreamingAll(io, "pub const api_key = ");
    const api_key_length = std.fmt.printInt(&print_buffer, protocol_json.apiKey, 10, .lower, .{});
    try output_file.writeStreamingAll(io, print_buffer[0..api_key_length]);

    try output_file.writeStreamingAll(io, ";");

    try output_file.writeStreamingAll(io, "pub const is_request = ");
    try output_file.writeStreamingAll(io, if (is_request) "true;" else "false;");

    try output_file.writeStreamingAll(io, "pub const is_flexible = ");
    try output_file.writeStreamingAll(io, if (is_flexible) "true;" else "false;");

    try output_file.writeStreamingAll(io, "pub const version = ");
    const version_length = std.fmt.printInt(&print_buffer, version, 10, .lower, .{});
    try output_file.writeStreamingAll(io, print_buffer[0..version_length]);

    try output_file.writeStreamingAll(io, ";");

    for (protocol_json.fields) |field| {
        try mapSubtype(io, arena, field, version, output_file);
    }

    for (protocol_json.fields) |field| {
        try mapField(io, arena, field, version, output_file);
    }

    if (is_request) {
        try createSerialise(io, arena, protocol_json, version, is_flexible, output_file);
    } else {
        // try createDeserialise(io, arena, protocol_json, version, is_flexible, output_file);
    }

    try output_file.writeStreamingAll(io, "};");
}

fn createDeserialise(
    io: Io,
    arena: std.mem.Allocator,
    protocol_json: ProtocolJson,
    version: usize,
    is_flexible: bool,
    output_file: Io.File,
) !void {
    try output_file.writeStreamingAll(io,
        \\ pub fn deserialise(self: *const @This(), allocator: std.mem.Allocator, bytes: [] const u8) !void {
        \\
    );

    for (protocol_json.fields) |field| {
        const field_versions = try VersionRange.parse(field.versions);
        if (field_versions.contains(version)) {
            _ = is_flexible;
            _ = arena;
            // try createDeserialiseField(io, arena, field, version, is_flexible, output_file);
        }
    }

    try output_file.writeStreamingAll(io,
        \\ }
        \\
    );
}

fn createDeserialiseField(
    io: Io,
    arena: std.mem.Allocator,
    field: ProtocolField,
    version: usize,
    is_flexible: bool,
    output_file: Io.File,
) !void {
    _ = arena;
    _ = io;
    _ = version;
    _ = is_flexible;
    _ = output_file;

    const kafka_type = field.type;
    if (std.mem.eql(u8, kafka_type, "int8")) @panic("int");
    if (std.mem.eql(u8, kafka_type, "int16")) @panic("int");
    if (std.mem.eql(u8, kafka_type, "int32")) @panic("int");
    if (std.mem.eql(u8, kafka_type, "int64")) @panic("int");
    if (std.mem.eql(u8, kafka_type, "bool")) @panic("bool");
    if (std.mem.eql(u8, kafka_type, "string")) @panic("string");
    if (std.mem.eql(u8, kafka_type, "bytes")) @panic("bytes");
    if (std.mem.eql(u8, kafka_type, "records")) @panic("records");
    if (std.mem.eql(u8, kafka_type, "uuid")) @panic("uuid");
    if (std.mem.eql(u8, kafka_type, "float64")) @panic("float64");

    // Fallback for custom nested arrays (e.g. "[]Topic")
    @panic(kafka_type);
}

fn createSerialise(
    io: Io,
    arena: std.mem.Allocator,
    protocol_json: ProtocolJson,
    version: usize,
    is_flexible: bool,
    output_file: Io.File,
) !void {
    try output_file.writeStreamingAll(io,
        \\ pub fn serialise(self: *const @This(), writer: *std.Io.Writer) !void {
        \\
    );
    var has_field = false;
    for (protocol_json.fields) |field| {
        const field_versions = try VersionRange.parse(field.versions);
        if (field_versions.contains(version)) {
            has_field = true;
            try createSerialiseField(io, arena, field, version, is_flexible, output_file);
        }
    }

    if (!has_field) {
        try output_file.writeStreamingAll(io,
            \\   _ = self;
        );
    }

    if (is_flexible) {
        // todo proper flexible handling
        try output_file.writeStreamingAll(io, "try writer.writeByte(0x00);");
    }

    try output_file.writeStreamingAll(io,
        \\   try writer.flush();
        \\ }
    );
}

fn createSerialiseField(
    io: Io,
    arena: std.mem.Allocator,
    field: ProtocolField,
    version: usize,
    is_flexible: bool,
    output_file: Io.File,
) !void {
    _ = arena;

    const kafka_type = field.type;
    if (std.mem.eql(u8, kafka_type, "int8"))
        return createSerialiseInt(i8, io, field, output_file);
    if (std.mem.eql(u8, kafka_type, "int16"))
        return createSerialiseInt(i16, io, field, output_file);
    if (std.mem.eql(u8, kafka_type, "int32"))
        return createSerialiseInt(i32, io, field, output_file);
    if (std.mem.eql(u8, kafka_type, "int64"))
        return createSerialiseInt(i64, io, field, output_file);
    if (std.mem.eql(u8, kafka_type, "bool")) @panic("bool");
    if (std.mem.eql(u8, kafka_type, "string"))
        return createSerialiseBytes(io, field, version, is_flexible, output_file);
    if (std.mem.eql(u8, kafka_type, "bytes"))
        return createSerialiseBytes(io, field, version, is_flexible, output_file);
    if (std.mem.eql(u8, kafka_type, "records"))
        return createSerialiseBytes(io, field, version, is_flexible, output_file);
    if (std.mem.eql(u8, kafka_type, "uuid")) @panic("uuid");
    if (std.mem.eql(u8, kafka_type, "float64")) @panic("float64");

    // Fallback for custom nested arrays (e.g. "[]Topic")
    @panic(kafka_type);
}

fn createSerialiseInt(
    T: type,
    io: Io,
    field: ProtocolField,
    output_file: Io.File,
) !void {
    try output_file.writeStreamingAll(io,
        \\ {
        \\   var buf: [
    );
    const size = switch (@sizeOf(T)) {
        1 => "1",
        2 => "2",
        4 => "4",
        8 => "8",
        else => @compileError(std.fmt.comptimePrint("Unsupported size: {}", .{@sizeOf(T)})),
    };
    try output_file.writeStreamingAll(io, size);
    try output_file.writeStreamingAll(io,
        \\]u8 = undefined;
        \\  std.mem.writeInt(
    );

    var type_buffer: [3]u8 = undefined;
    const type_name = try std.fmt.bufPrint(&type_buffer, "{}", .{T});

    try output_file.writeStreamingAll(io, type_name);

    try output_file.writeStreamingAll(io, ", &buf, self.");

    try output_file.writeStreamingAll(io, field.snake_name);

    try output_file.writeStreamingAll(io,
        \\, .big);
        \\   try writer.writeAll(&buf);
        \\ }
    );
}

fn createSerialiseBytes(
    io: Io,
    field: ProtocolField,
    version: usize,
    is_flexible: bool,
    output_file: Io.File,
) !void {
    const is_nullable = if (field.nullableVersions) |n|
        (try VersionRange.parse(n)).contains(version)
    else
        false;

    if (is_nullable) {
        try output_file.writeStreamingAll(io, "if(self.");
    } else {
        try output_file.writeStreamingAll(io, "{const field = self.");
    }
    try output_file.writeStreamingAll(io, field.snake_name);
    if (is_nullable) {
        try output_file.writeStreamingAll(io, ") |field| {");
    } else {
        try output_file.writeStreamingAll(io, ";");
    }

    if (is_flexible) {
        try output_file.writeStreamingAll(io, "try writeUnsignedVarInt(writer, field.len + 1);");
    } else {
        // Legacy needs to differentiate between string vs bytes/records
        if (std.mem.eql(u8, field.type, "string")) {
            try output_file.writeStreamingAll(io, "try writer.writeInt(i16, @intCast(field.len), .big);");
        } else {
            // "bytes" and "records"
            try output_file.writeStreamingAll(io, "try writer.writeInt(i32, @intCast(field.len), .big);");
        }
    }

    try output_file.writeStreamingAll(io, "try writer.writeAll(field);}");

    if (is_nullable) {
        try output_file.writeStreamingAll(io, "else {");

        if (is_flexible) {
            try output_file.writeStreamingAll(io, "try writeUnsignedVarInt(writer, 0);");
        } else {
            // Legacy needs to differentiate between string vs bytes/records
            if (std.mem.eql(u8, field.type, "string")) {
                try output_file.writeStreamingAll(io, "try writer.writeInt(i16, -1, .big);");
            } else {
                // "bytes" and "records"
                try output_file.writeStreamingAll(io, "try writer.writeInt(i32, -1, .big);");
            }
        }

        try output_file.writeStreamingAll(io, "}");
    }
}

fn mapSubtype(io: Io, arena: std.mem.Allocator, field: ProtocolField, version: usize, output_file: Io.File) !void {
    const field_versions = try VersionRange.parse(field.versions);
    if (!field_versions.contains(version)) return;

    if (field.type[0] == '[' and field.type[1] == ']') {
        for (field.fields) |sub_field| {
            try mapSubtype(io, arena, sub_field, version, output_file);
        }

        try output_file.writeStreamingAll(io, "pub const ");
        try output_file.writeStreamingAll(io, field.type[2..]);
        try output_file.writeStreamingAll(io, "= struct {");

        for (field.fields) |sub_field| {
            try mapField(io, arena, sub_field, version, output_file);
        }

        try output_file.writeStreamingAll(io, "};");
    }
}

fn mapField(io: Io, arena: std.mem.Allocator, field: ProtocolField, version: usize, output_file: Io.File) !void {
    const field_versions = try VersionRange.parse(field.versions);
    if (!field_versions.contains(version)) return;

    const nullable_versions = if (field.nullableVersions) |v| try VersionRange.parse(v) else VersionRange.none;

    const snake_name = field.snake_name;
    const zig_type = mapKafkaType(field.type);

    const nullable = if (nullable_versions.contains(version)) "?" else "";

    var default_buffer: [128]u8 = undefined;

    const default = if (field.default) |value|
        switch (value) {
            .null => " = null",
            .string => |s| blk: {
                if (std.mem.eql(u8, "null", s)) {
                    break :blk " = null";
                }
                break :blk try std.fmt.bufPrint(&default_buffer, " = {s}", .{s});
            },
            .integer => |i| try std.fmt.bufPrint(&default_buffer, " = {}", .{i}),
            else => {
                std.log.err("Bad default {any}", .{value});
                return error.UnsupportedType;
            },
        }
    else
        "";

    // A quick allocPrint makes this way cleaner than manual writeStreamingAll
    const field_str = try std.fmt.allocPrint(arena, "{s}: {s}{s}{s},", .{ snake_name, nullable, zig_type, default });
    try output_file.writeStreamingAll(io, field_str);
}

fn fatal(comptime format: []const u8, args: anytype) noreturn {
    std.debug.print(format, args);
    std.process.exit(1);
}

pub const std_options: std.Options = .{ .log_level = .err };
