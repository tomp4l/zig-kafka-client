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

    var buffer: [1024]u8 = undefined;
    var file_writer = output_file.writer(io, &buffer);
    const writer = &file_writer.interface;
    std.log.debug("protocol out file: {s}", .{output_file_path});

    generateAllStructs(io, arena, writer) catch |err| {
        fatal("unable to generate files '{s}': {s}", .{ output_file_path, @errorName(err) });
    };

    try writer.flush();

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

fn generateAllStructs(io: Io, arena: std.mem.Allocator, writer: *Io.Writer) !void {
    try writer.writeAll("const std = @import(\"std\");");

    try writer.writeAll(
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
        \\
        \\ fn readUnsignedVarInt(bytes: []const u8) !struct { value: usize, bytes_read: usize } {
        \\   var value: usize = 0;
        \\   var shift: u6 = 0;
        \\   var bytes_read: usize = 0;
        \\   
        \\   while (bytes_read < bytes.len) {
        \\       const byte = bytes[bytes_read];
        \\       bytes_read += 1;
        \\
        \\       value |= @as(usize, byte & 0x7F) << shift;
        \\   
        \\       if ((byte & 0x80) == 0) {
        \\           return .{ .value = value, .bytes_read = bytes_read };
        \\       }
        \\   
        \\       if (shift >= 63) return error.VarIntTooBig;
        \\           shift += 7;
        \\       }
        \\   
        \\       return error.TooShort;
        \\   }
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

        try generateStructVersions(io, arena, json, writer);
    }
}

fn generateStructVersions(io: Io, arena: std.mem.Allocator, protocol_json: ProtocolJson, writer: *Io.Writer) !void {
    var version_it = try protocol_json.validVersionsIterator();
    while (version_it.next()) |version| {
        try generateStructVersion(io, arena, protocol_json, version, writer);
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
    writer: *Io.Writer,
) !void {

    // shared buffer for all formatting
    var print_buffer: [256]u8 = undefined;
    const name = try std.fmt.bufPrint(&print_buffer, "{s}V{}", .{ protocol_json.name, version });

    std.log.debug("generating {s}", .{name});

    const flexible_versions = try VersionRange.parse(protocol_json.flexibleVersions);
    const is_flexible = flexible_versions.contains(version);
    const is_request = std.mem.eql(u8, "request", protocol_json.type);

    try writer.print(
        \\ pub const {s} = struct {{
        \\   pub const api_key = {};
        \\   pub const is_request = {};
        \\   pub const is_flexible = {};
        \\   pub const version = {};
    , .{
        name,
        protocol_json.apiKey,
        is_request,
        is_flexible,
        version,
    });

    for (protocol_json.fields) |field| {
        try mapSubtype(io, arena, field, version, is_request, is_flexible, writer);
    }

    for (protocol_json.fields) |field| {
        try mapField(field, version, writer);
    }

    if (is_request) {
        try createSerialise(arena, protocol_json, version, is_flexible, writer);
    } else {
        try createDeserialise(protocol_json, version, is_flexible, writer);
    }

    try writer.writeAll("};");
}

fn createDeserialise(
    protocol_json: ProtocolJson,
    version: usize,
    is_flexible: bool,
    writer: *Io.Writer,
) !void {
    try writer.writeAll(
        \\ pub fn deserialise(allocator: std.mem.Allocator, bytes: [] const u8) !@This() {
        \\   var current_offset: usize = 0;
        \\   var self: @This() = .{
    );

    for (protocol_json.fields) |field| {
        const field_versions = try VersionRange.parse(field.versions);
        if (field_versions.contains(version)) {
            try createFieldInitialisation(field, writer);
        }
    }

    try writer.writeAll("};");

    try createSerialiseFields(protocol_json.fields, version, is_flexible, writer);

    try writer.writeAll(
        \\ return self;
        \\ }
    );
}

fn getImplicitDefault(field: ProtocolField, version: usize) ![]const u8 {
    if (field.nullableVersions) |nv| {
        if ((try VersionRange.parse(nv)).contains(version)) {
            return "null";
        }
    }
    if (std.mem.eql(u8, field.type, "bool")) return "false";
    if (std.mem.startsWith(u8, field.type, "int")) return "0";
    if (std.mem.eql(u8, field.type, "float64")) return "0.0";
    if (std.mem.eql(u8, field.type, "uuid")) return "@splat(0)";

    // Strings, bytes, records, and arrays defaults to empty slices
    return "&.{}";
}

fn createDefaultInit(field: ProtocolField, version: usize, writer: *Io.Writer) !void {
    const has_default = field.default != null;
    if (has_default) return;

    try writer.print(
        \\ self.{s} = {s};
    , .{ field.snake_name, try getImplicitDefault(field, version) });
}

fn createSerialiseFields(
    fields: []const ProtocolField,
    version: usize,
    is_flexible: bool,
    writer: *Io.Writer,
) !void {
    var allocated = false;
    for (fields) |field| {
        const field_versions = try VersionRange.parse(field.versions);
        if (field_versions.contains(version)) {
            const is_tagged = if (field.taggedVersions) |v| (try VersionRange.parse(v)).contains(version) else false;
            if (is_tagged) {
                try createDefaultInit(field, version, writer);
            } else {
                allocated = try createDeserialiseField(field, version, is_flexible, writer) or allocated;
            }
        }
    }

    if (is_flexible) {
        try writer.writeAll(
            \\ const var_int_tags = try readUnsignedVarInt(bytes[current_offset..]);
            \\ current_offset += var_int_tags.bytes_read;
            \\ var number_tags = var_int_tags.value;
            \\ while(true) {
            \\   if (number_tags == 0) break;
            \\   number_tags -= 1;
            \\   const current_tag = try readUnsignedVarInt(bytes[current_offset..]);
            \\   current_offset += current_tag.bytes_read;
            \\   const current_size = try readUnsignedVarInt(bytes[current_offset..]);
            \\   current_offset += current_size.bytes_read;
            \\   switch(current_tag.value) {
        );
    }

    for (fields) |field| {
        const field_versions = try VersionRange.parse(field.versions);
        if (field_versions.contains(version)) {
            const is_tagged = if (field.taggedVersions) |v| (try VersionRange.parse(v)).contains(version) else false;
            if (!is_tagged) continue;
            const tag = field.tag orelse return error.ExpectingTag;

            try writer.print("{} => ", .{tag});
            allocated = try createDeserialiseField(field, version, is_flexible, writer) or allocated;
            try writer.writeAll(",");
        }
    }

    if (is_flexible) {
        try writer.writeAll(
            \\      else => {
            \\        current_offset += current_size.value;
            \\      },
            \\    }
            \\  }
        );
    }

    if (!allocated) {
        try writer.writeAll("_=allocator;");
    }
}

fn createDeserialiseSubType(
    protocol_field: ProtocolField,
    version: usize,
    is_flexible: bool,
    writer: *Io.Writer,
) !void {
    try writer.writeAll(
        \\ pub fn deserialise(self: *@This(), allocator: std.mem.Allocator, bytes: [] const u8) !usize {
        \\   var current_offset: usize = 0;
    );
    try createSerialiseFields(protocol_field.fields, version, is_flexible, writer);

    try writer.writeAll(
        \\   return current_offset; 
        \\ }
        \\
    );
}

fn createFieldInitialisation(
    field: ProtocolField,
    writer: *Io.Writer,
) !void {
    if (field.default == null) {
        try writer.print(".{s} = undefined,", .{field.snake_name});
    }
}

fn createDeserialiseField(
    field: ProtocolField,
    version: usize,
    is_flexible: bool,
    writer: *Io.Writer,
) !bool {
    const kafka_type = field.type;
    if (std.mem.eql(u8, kafka_type, "int8")) return createDeserialiseInt(i8, field, writer);
    if (std.mem.eql(u8, kafka_type, "int16")) return createDeserialiseInt(i16, field, writer);
    if (std.mem.eql(u8, kafka_type, "int32")) return createDeserialiseInt(i32, field, writer);
    if (std.mem.eql(u8, kafka_type, "int64")) return createDeserialiseInt(i64, field, writer);
    if (std.mem.eql(u8, kafka_type, "bool")) return createDeserialiseBool(field, writer);
    if (std.mem.eql(u8, kafka_type, "string")) return createDeserialiseBytes(field, version, is_flexible, writer);
    if (std.mem.eql(u8, kafka_type, "bytes")) return createDeserialiseBytes(field, version, is_flexible, writer);
    if (std.mem.eql(u8, kafka_type, "records")) return createDeserialiseBytes(field, version, is_flexible, writer);
    if (std.mem.eql(u8, kafka_type, "uuid")) @panic("uuid");
    if (std.mem.eql(u8, kafka_type, "float64")) @panic("float64");

    // Fallback for custom nested arrays (e.g. "[]Topic")
    try createDeserialiseArray(field, is_flexible, writer);
    return true;
}

fn createDeserialiseArray(
    field: ProtocolField,
    is_flexible: bool,
    writer: *Io.Writer,
) !void {
    if (!std.mem.eql(u8, "[]", field.type[0..2])) return error.ExpectingArray;
    const type_name = field.type[2..];

    try writer.writeAll("{");
    if (is_flexible) {
        try writer.writeAll(
            \\ const var_int = try readUnsignedVarInt(bytes[current_offset..]);
            \\ current_offset += var_int.bytes_read;
            \\ const is_null = var_int.value == 0;
            \\ const length = var_int.value -| 1;
        );
    } else {
        try writer.writeAll(
            \\    const int_size = 4;
            \\    if (current_offset + int_size > bytes.len) return error.TooShort;
            \\    const length = std.mem.readInt(i32, bytes[current_offset .. current_offset + int_size][0..int_size], .big);
            \\    const is_null = length == -1;
            \\    current_offset += int_size;
        );
    }

    // allocate
    try writer.print(
        \\ if (is_null) {{
        \\     const FieldType = @TypeOf(self.{s});
        \\     const type_info = @typeInfo(FieldType);
        \\     switch (type_info) {{
        \\       .optional => self.{s} = null,
        \\       else => return error.NonNullableField,
        \\     }}
        \\ }} else {{
        \\     const values = try allocator.alloc({s}, @intCast(length));
        \\     errdefer allocator.free(values);
        \\     self.{s} = values;
        \\     for (values) |*value| {{
        \\         current_offset += try value.deserialise(allocator, bytes[current_offset..]);
        \\     }}
        \\ }}
    , .{ field.snake_name, field.snake_name, type_name, field.snake_name });

    try writer.writeAll("}");
}

fn createDeserialiseBytes(
    field: ProtocolField,
    version: usize,
    is_flexible: bool,
    writer: *Io.Writer,
) !bool {
    _ = version;
    try writer.writeAll("{");
    if (is_flexible) {
        try writer.writeAll(
            \\ const var_int = try readUnsignedVarInt(bytes[current_offset..]);
            \\ current_offset += var_int.bytes_read;
            \\ const is_null = var_int.value == 0;
            \\ const length = var_int.value -| 1;
        );
    } else {
        const int_type = if (std.mem.eql(u8, field.type, "string")) "i16" else "i32";
        const int_type_size: usize = if (std.mem.eql(u8, field.type, "string")) 2 else 4;

        try writer.print(
            \\    const int_size = {};
            \\    if (current_offset + int_size > bytes.len) return error.TooShort;
            \\    const length = std.mem.readInt({s}, bytes[current_offset .. current_offset + int_size][0..int_size], .big);
            \\    const is_null = length == -1;
            \\    current_offset += int_size;
        , .{ int_type_size, int_type });
    }

    try writer.print(
        \\if (!is_null) {{
        \\   if (current_offset + length > bytes.len) return error.TooShort;
        \\   self.{s} = bytes[current_offset..current_offset+length];
        \\   current_offset+=length;
        \\}} else {{
        \\   const FieldType = @TypeOf(self.{s});
        \\   const type_info = @typeInfo(FieldType);
        \\   switch (type_info) {{
        \\     .optional => self.{s} = null,
        \\     else => return error.NonNullableField,
        \\   }}
        \\}}
    , .{ field.snake_name, field.snake_name, field.snake_name });

    try writer.writeAll("}");

    return false;
}

fn createDeserialiseInt(
    T: type,
    field: ProtocolField,
    writer: *Io.Writer,
) !bool {
    try writer.print(
        \\{{
        \\    const size = {};
        \\    if (current_offset + size > bytes.len) return error.TooShort;
        \\    self.{s} = std.mem.readInt({}, bytes[current_offset .. current_offset + size][0..size], .big);
        \\    current_offset += size;
        \\}}
    , .{ @sizeOf(T), field.snake_name, T });

    return false;
}

fn createDeserialiseBool(
    field: ProtocolField,
    writer: *Io.Writer,
) !bool {
    try writer.print(
        \\{{
        \\    if (current_offset + 1 > bytes.len) {{
        \\       return error.TooShort;
        \\    }}
        \\    self.{s} = bytes[current_offset] != 0;
        \\    current_offset += 1;
        \\}}
    , .{field.snake_name});

    return false;
}

fn createSerialise(
    arena: std.mem.Allocator,
    protocol_json: ProtocolJson,
    version: usize,
    is_flexible: bool,
    writer: *Io.Writer,
) !void {
    try writer.writeAll(
        \\ pub fn serialise(self: *const @This(), writer: *std.Io.Writer) !void {
        \\
    );
    var has_field = false;
    for (protocol_json.fields) |field| {
        const field_versions = try VersionRange.parse(field.versions);
        if (field_versions.contains(version)) {
            has_field = true;
            try createSerialiseField(arena, field, version, is_flexible, writer);
        }
    }

    if (!has_field) {
        try writer.writeAll(
            \\   _ = self;
        );
    }

    if (is_flexible) {
        // todo proper flexible handling
        try writer.writeAll("try writer.writeByte(0x00);");
    }

    try writer.writeAll(
        \\   try writer.flush();
        \\ }
    );
}

fn createSerialiseField(
    arena: std.mem.Allocator,
    field: ProtocolField,
    version: usize,
    is_flexible: bool,
    writer: *Io.Writer,
) !void {
    _ = arena;

    const kafka_type = field.type;
    if (std.mem.eql(u8, kafka_type, "int8"))
        return createSerialiseInt(i8, field, writer);
    if (std.mem.eql(u8, kafka_type, "int16"))
        return createSerialiseInt(i16, field, writer);
    if (std.mem.eql(u8, kafka_type, "int32"))
        return createSerialiseInt(i32, field, writer);
    if (std.mem.eql(u8, kafka_type, "int64"))
        return createSerialiseInt(i64, field, writer);
    if (std.mem.eql(u8, kafka_type, "bool")) @panic("bool");
    if (std.mem.eql(u8, kafka_type, "string"))
        return createSerialiseBytes(field, version, is_flexible, writer);
    if (std.mem.eql(u8, kafka_type, "bytes"))
        return createSerialiseBytes(field, version, is_flexible, writer);
    if (std.mem.eql(u8, kafka_type, "records"))
        return createSerialiseBytes(field, version, is_flexible, writer);
    if (std.mem.eql(u8, kafka_type, "uuid")) @panic("uuid");
    if (std.mem.eql(u8, kafka_type, "float64")) @panic("float64");

    // Fallback for custom nested arrays (e.g. "[]Topic")
    @panic(kafka_type);
}

fn createSerialiseInt(
    T: type,
    field: ProtocolField,
    writer: *Io.Writer,
) !void {
    try writer.print(
        \\ {{
        \\   var buf: [{}]u8 = undefined;
        \\   std.mem.writeInt({}, &buf, self.{s} , .big);
        \\   try writer.writeAll(&buf);
        \\ }}
    , .{ @sizeOf(T), T, field.snake_name });
}

fn createSerialiseBytes(
    field: ProtocolField,
    version: usize,
    is_flexible: bool,
    writer: *Io.Writer,
) !void {
    const is_nullable = if (field.nullableVersions) |n|
        (try VersionRange.parse(n)).contains(version)
    else
        false;

    if (is_nullable) {
        try writer.print("if(self.{s}) |field| {{", .{field.snake_name});
    } else {
        try writer.print(
            \\{{
            \\   const field = self.{s};
        , .{field.snake_name});
    }

    if (is_flexible) {
        try writer.writeAll("try writeUnsignedVarInt(writer, field.len + 1);");
    } else {
        // Legacy needs to differentiate between string vs bytes/records
        if (std.mem.eql(u8, field.type, "string")) {
            try writer.writeAll("try writer.writeInt(i16, @intCast(field.len), .big);");
        } else {
            // "bytes" and "records"
            try writer.writeAll("try writer.writeInt(i32, @intCast(field.len), .big);");
        }
    }

    try writer.writeAll("try writer.writeAll(field);}");

    if (is_nullable) {
        try writer.writeAll("else {");

        if (is_flexible) {
            try writer.writeAll("try writeUnsignedVarInt(writer, 0);");
        } else {
            // Legacy needs to differentiate between string vs bytes/records
            if (std.mem.eql(u8, field.type, "string")) {
                try writer.writeAll("try writer.writeInt(i16, -1, .big);");
            } else {
                // "bytes" and "records"
                try writer.writeAll("try writer.writeInt(i32, -1, .big);");
            }
        }

        try writer.writeAll("}");
    }
}

fn mapSubtype(io: Io, arena: std.mem.Allocator, field: ProtocolField, version: usize, is_request: bool, is_flexible: bool, writer: *Io.Writer) !void {
    const field_versions = try VersionRange.parse(field.versions);
    if (!field_versions.contains(version)) return;

    if (field.type[0] == '[' and field.type[1] == ']') {
        for (field.fields) |sub_field| {
            try mapSubtype(io, arena, sub_field, version, is_request, is_flexible, writer);
        }

        try writer.print("pub const {s} = struct {{", .{field.type[2..]});

        for (field.fields) |sub_field| {
            try mapField(sub_field, version, writer);
        }

        if (is_request) {
            // serialise?
        } else {
            try createDeserialiseSubType(field, version, is_flexible, writer);
        }

        try writer.writeAll("};");
    }
}

fn mapField(field: ProtocolField, version: usize, writer: *Io.Writer) !void {
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

    try writer.print("{s}: {s}{s}{s},", .{ snake_name, nullable, zig_type, default });
}

fn fatal(comptime format: []const u8, args: anytype) noreturn {
    std.debug.print(format, args);
    std.process.exit(1);
}

pub const std_options: std.Options = .{ .log_level = .err };
