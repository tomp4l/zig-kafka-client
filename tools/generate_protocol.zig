const std = @import("std");
const Io = std.Io;

pub fn main(init: std.process.Init) !void {
    const io = init.io;
    const arena = init.arena.allocator();

    const args = try init.minimal.args.toSlice(arena);

    if (args.len < 3) fatal("wrong number of arguments", .{});

    const output_file_path = args[1];
    const input_files = args[2..];

    var output_file = Io.Dir.cwd().createFile(io, output_file_path, .{}) catch |err| {
        fatal("unable to open '{s}': {s}", .{ output_file_path, @errorName(err) });
    };

    var buffer: [1024]u8 = undefined;
    var file_writer = output_file.writer(io, &buffer);
    const writer = &file_writer.interface;
    std.log.debug("protocol out file: {s}", .{output_file_path});

    generateAllStructs(io, arena, writer, input_files) catch |err| {
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
    entityType: ?[]const u8 = null,

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

fn writeErrorEnum(writer: *Io.Writer) !void {
    try writer.writeAll(
        \\ pub const ResponseError = enum(i16) {
        \\ UNKNOWN_SERVER_ERROR = -1, // The server experienced an unexpected error when processing the request.
        \\ NONE =  0,  
        \\ OFFSET_OUT_OF_RANGE =  1,  //The requested offset is not within the range of offsets maintained by the server.
        \\ CORRUPT_MESSAGE =  2,  //This message has failed its CRC checksum, exceeds the valid size, has a null key for a compacted topic, or is otherwise corrupt.
        \\ UNKNOWN_TOPIC_OR_PARTITION =  3,  //This server does not host this topic-partition.
        \\ INVALID_FETCH_SIZE =  4,  //The requested fetch size is invalid.
        \\ LEADER_NOT_AVAILABLE =  5,  //There is no leader for this topic-partition as we are in the middle of a leadership election.
        \\ NOT_LEADER_OR_FOLLOWER =  6,  //For requests intended only for the leader, this error indicates that the broker is not the current leader. For requests intended for any replica, this error indicates that the broker is not a replica of the topic partition.
        \\ REQUEST_TIMED_OUT =  7,  //The request timed out.
        \\ BROKER_NOT_AVAILABLE =  8,  //The broker is not available.
        \\ REPLICA_NOT_AVAILABLE =  9,  //The replica is not available for the requested topic-partition. Produce/Fetch requests and other requests intended only for the leader or follower return NOT_LEADER_OR_FOLLOWER if the broker is not a replica of the topic-partition.
        \\ MESSAGE_TOO_LARGE =  10,  //The request included a message larger than the max message size the server will accept.
        \\ STALE_CONTROLLER_EPOCH =  11,  //The controller moved to another broker.
        \\ OFFSET_METADATA_TOO_LARGE =  12,  //The metadata field of the offset request was too large.
        \\ NETWORK_EXCEPTION =  13,  //The server disconnected before a response was received.
        \\ COORDINATOR_LOAD_IN_PROGRESS =  14,  //The coordinator is loading and hence can't process requests.
        \\ COORDINATOR_NOT_AVAILABLE =  15,  //The coordinator is not available.
        \\ NOT_COORDINATOR =  16,  //This is not the correct coordinator.
        \\ INVALID_TOPIC_EXCEPTION =  17,  //The request attempted to perform an operation on an invalid topic.
        \\ RECORD_LIST_TOO_LARGE =  18,  //The request included message batch larger than the configured segment size on the server.
        \\ NOT_ENOUGH_REPLICAS =  19,  //Messages are rejected since there are fewer in-sync replicas than required.
        \\ NOT_ENOUGH_REPLICAS_AFTER_APPEND =  20,  //Messages are written to the log, but to fewer in-sync replicas than required.
        \\ INVALID_REQUIRED_ACKS =  21,  //Produce request specified an invalid value for required acks.
        \\ ILLEGAL_GENERATION =  22,  //Specified group generation id is not valid.
        \\ INCONSISTENT_GROUP_PROTOCOL =  23,  //The group member's supported protocols are incompatible with those of existing members or first group member tried to join with empty protocol type or empty protocol list.
        \\ INVALID_GROUP_ID =  24,  //The group id is invalid.
        \\ UNKNOWN_MEMBER_ID =  25,  //The coordinator is not aware of this member.
        \\ INVALID_SESSION_TIMEOUT =  26,  //The session timeout is not within the range allowed by the broker (as configured by group.min.session.timeout.ms and group.max.session.timeout.ms).
        \\ REBALANCE_IN_PROGRESS =  27,  //The group is rebalancing, so a rejoin is needed.
        \\ INVALID_COMMIT_OFFSET_SIZE =  28,  //The committing offset data size is not valid.
        \\ TOPIC_AUTHORIZATION_FAILED =  29,  //Topic authorization failed.
        \\ GROUP_AUTHORIZATION_FAILED =  30,  //Group authorization failed.
        \\ CLUSTER_AUTHORIZATION_FAILED =  31,  //Cluster authorization failed.
        \\ INVALID_TIMESTAMP =  32,  //The timestamp of the message is out of acceptable range.
        \\ UNSUPPORTED_SASL_MECHANISM =  33,  //The broker does not support the requested SASL mechanism.
        \\ ILLEGAL_SASL_STATE =  34,  //Request is not valid given the current SASL state.
        \\ UNSUPPORTED_VERSION =  35,  //The version of API is not supported.
        \\ TOPIC_ALREADY_EXISTS =  36,  //Topic with this name already exists.
        \\ INVALID_PARTITIONS =  37,  //Number of partitions is below 1.
        \\ INVALID_REPLICATION_FACTOR =  38,  //Replication factor is below 1 or larger than the number of available brokers.
        \\ INVALID_REPLICA_ASSIGNMENT =  39,  //Replica assignment is invalid.
        \\ INVALID_CONFIG =  40,  //Configuration is invalid.
        \\ NOT_CONTROLLER =  41,  //This is not the correct controller for this cluster.
        \\ INVALID_REQUEST =  42,  //This most likely occurs because of a request being malformed by the client library or the message was sent to an incompatible broker. See the broker logs for more details.
        \\ UNSUPPORTED_FOR_MESSAGE_FORMAT =  43,  //The message format version on the broker does not support the request.
        \\ POLICY_VIOLATION =  44,  //Request parameters do not satisfy the configured policy.
        \\ OUT_OF_ORDER_SEQUENCE_NUMBER =  45,  //The broker received an out of order sequence number.
        \\ DUPLICATE_SEQUENCE_NUMBER =  46,  //The broker received a duplicate sequence number.
        \\ INVALID_PRODUCER_EPOCH =  47,  //Producer attempted to produce with an old epoch.
        \\ INVALID_TXN_STATE =  48,  //The producer attempted a transactional operation in an invalid state.
        \\ INVALID_PRODUCER_ID_MAPPING =  49,  //The producer attempted to use a producer id which is not currently assigned to its transactional id.
        \\ INVALID_TRANSACTION_TIMEOUT =  50,  //The transaction timeout is larger than the maximum value allowed by the broker (as configured by transaction.max.timeout.ms).
        \\ CONCURRENT_TRANSACTIONS =  51,  //The producer attempted to update a transaction while another concurrent operation on the same transaction was ongoing.
        \\ TRANSACTION_COORDINATOR_FENCED =  52,  //Indicates that the transaction coordinator sending a WriteTxnMarker is no longer the current coordinator for a given producer.
        \\ TRANSACTIONAL_ID_AUTHORIZATION_FAILED =  53,  //Transactional Id authorization failed.
        \\ SECURITY_DISABLED =  54,  //Security features are disabled.
        \\ OPERATION_NOT_ATTEMPTED =  55,  //The broker did not attempt to execute this operation. This may happen for batched RPCs where some operations in the batch failed, causing the broker to respond without trying the rest.
        \\ KAFKA_STORAGE_ERROR =  56,  //Disk error when trying to access log file on the disk.
        \\ LOG_DIR_NOT_FOUND =  57,  //The user-specified log directory is not found in the broker config.
        \\ SASL_AUTHENTICATION_FAILED =  58,  //SASL Authentication failed.
        \\ UNKNOWN_PRODUCER_ID =  59,  //This exception is raised by the broker if it could not locate the producer metadata associated with the producerId in question. This could happen if, for instance, the producer's records were deleted because their retention time had elapsed. Once the last records of the producerId are removed, the producer's metadata is removed from the broker, and future appends by the producer will return this exception.
        \\ REASSIGNMENT_IN_PROGRESS =  60,  //A partition reassignment is in progress.
        \\ DELEGATION_TOKEN_AUTH_DISABLED =  61,  //Delegation Token feature is not enabled.
        \\ DELEGATION_TOKEN_NOT_FOUND =  62,  //Delegation Token is not found on server.
        \\ DELEGATION_TOKEN_OWNER_MISMATCH =  63,  //Specified Principal is not valid Owner/Renewer.
        \\ DELEGATION_TOKEN_REQUEST_NOT_ALLOWED =  64,  //Delegation Token requests are not allowed on PLAINTEXT/1-way SSL channels and on delegation token authenticated channels.
        \\ DELEGATION_TOKEN_AUTHORIZATION_FAILED =  65,  //Delegation Token authorization failed.
        \\ DELEGATION_TOKEN_EXPIRED =  66,  //Delegation Token is expired.
        \\ INVALID_PRINCIPAL_TYPE =  67,  //Supplied principalType is not supported.
        \\ NON_EMPTY_GROUP =  68,  //The group is not empty.
        \\ GROUP_ID_NOT_FOUND =  69,  //The group id does not exist.
        \\ FETCH_SESSION_ID_NOT_FOUND =  70,  //The fetch session ID was not found.
        \\ INVALID_FETCH_SESSION_EPOCH =  71,  //The fetch session epoch is invalid.
        \\ LISTENER_NOT_FOUND =  72,  //There is no listener on the leader broker that matches the listener on which metadata request was processed.
        \\ TOPIC_DELETION_DISABLED =  73,  //Topic deletion is disabled.
        \\ FENCED_LEADER_EPOCH =  74,  //The leader epoch in the request is older than the epoch on the broker.
        \\ UNKNOWN_LEADER_EPOCH =  75,  //The leader epoch in the request is newer than the epoch on the broker.
        \\ UNSUPPORTED_COMPRESSION_TYPE =  76,  //The requesting client does not support the compression type of given partition.
        \\ STALE_BROKER_EPOCH =  77,  //Broker epoch has changed.
        \\ OFFSET_NOT_AVAILABLE =  78,  //The leader high watermark has not caught up from a recent leader election so the offsets cannot be guaranteed to be monotonically increasing.
        \\ MEMBER_ID_REQUIRED =  79,  //The group member needs to have a valid member id before actually entering a consumer group.
        \\ PREFERRED_LEADER_NOT_AVAILABLE =  80,  //The preferred leader was not available.
        \\ GROUP_MAX_SIZE_REACHED =  81,  //The group has reached its maximum size.
        \\ FENCED_INSTANCE_ID =  82,  //The broker rejected this static consumer since another consumer with the same group.instance.id has registered with a different member.id.
        \\ ELIGIBLE_LEADERS_NOT_AVAILABLE =  83,  //Eligible topic partition leaders are not available.
        \\ ELECTION_NOT_NEEDED =  84,  //Leader election not needed for topic partition.
        \\ NO_REASSIGNMENT_IN_PROGRESS =  85,  //No partition reassignment is in progress.
        \\ GROUP_SUBSCRIBED_TO_TOPIC =  86,  //Deleting offsets of a topic is forbidden while the consumer group is actively subscribed to it.
        \\ INVALID_RECORD =  87,  //This record has failed the validation on broker and hence will be rejected.
        \\ UNSTABLE_OFFSET_COMMIT =  88,  //There are unstable offsets that need to be cleared.
        \\ THROTTLING_QUOTA_EXCEEDED =  89,  //The throttling quota has been exceeded.
        \\ PRODUCER_FENCED =  90,  //There is a newer producer with the same transactionalId which fences the current one.
        \\ RESOURCE_NOT_FOUND =  91,  //A request illegally referred to a resource that does not exist.
        \\ DUPLICATE_RESOURCE =  92,  //A request illegally referred to the same resource twice.
        \\ UNACCEPTABLE_CREDENTIAL =  93,  //Requested credential would not meet criteria for acceptability.
        \\ INCONSISTENT_VOTER_SET =  94,  //Indicates that the either the sender or recipient of a voter-only request is not one of the expected voters.
        \\ INVALID_UPDATE_VERSION =  95,  //The given update version was invalid.
        \\ FEATURE_UPDATE_FAILED =  96,  //Unable to update finalized features due to an unexpected server error.
        \\ PRINCIPAL_DESERIALIZATION_FAILURE =  97,  //Request principal deserialization failed during forwarding. This indicates an internal error on the broker cluster security setup.
        \\ SNAPSHOT_NOT_FOUND =  98,  //Requested snapshot was not found.
        \\ POSITION_OUT_OF_RANGE =  99,  //Requested position is not greater than or equal to zero, and less than the size of the snapshot.
        \\ UNKNOWN_TOPIC_ID =  100,  //This server does not host this topic ID.
        \\ DUPLICATE_BROKER_REGISTRATION =  101,  //This broker ID is already in use.
        \\ BROKER_ID_NOT_REGISTERED =  102,  //The given broker ID was not registered.
        \\ INCONSISTENT_TOPIC_ID =  103,  //The log's topic ID did not match the topic ID in the request.
        \\ INCONSISTENT_CLUSTER_ID =  104,  //The clusterId in the request does not match that found on the server.
        \\ TRANSACTIONAL_ID_NOT_FOUND =  105,  //The transactionalId could not be found.
        \\ FETCH_SESSION_TOPIC_ID_ERROR =  106,  //The fetch session encountered inconsistent topic ID usage.
        \\ INELIGIBLE_REPLICA =  107,  //The new ISR contains at least one ineligible replica.
        \\ NEW_LEADER_ELECTED =  108,  //The AlterPartition request successfully updated the partition state but the leader has changed.
        \\ OFFSET_MOVED_TO_TIERED_STORAGE =  109,  //The requested offset is moved to tiered storage.
        \\ FENCED_MEMBER_EPOCH =  110,  //The member epoch is fenced by the group coordinator. The member must abandon all its partitions and rejoin.
        \\ UNRELEASED_INSTANCE_ID =  111,  //The instance ID is still used by another member in the consumer group. That member must leave first.
        \\ UNSUPPORTED_ASSIGNOR =  112,  //The assignor or its version range is not supported by the consumer group.
        \\ STALE_MEMBER_EPOCH =  113,  //The member epoch is stale. The member must retry after receiving its updated member epoch via the ConsumerGroupHeartbeat API.
        \\ MISMATCHED_ENDPOINT_TYPE =  114,  //The request was sent to an endpoint of the wrong type.
        \\ UNSUPPORTED_ENDPOINT_TYPE =  115,  //This endpoint type is not supported yet.
        \\ UNKNOWN_CONTROLLER_ID =  116,  //This controller ID is not known.
        \\ UNKNOWN_SUBSCRIPTION_ID =  117,  //Client sent a push telemetry request with an invalid or outdated subscription ID.
        \\ TELEMETRY_TOO_LARGE =  118,  //Client sent a push telemetry request larger than the maximum size the broker will accept.
        \\ INVALID_REGISTRATION =  119,  //The controller has considered the broker registration to be invalid.
        \\ TRANSACTION_ABORTABLE =  120,  //The server encountered an error with the transaction. The client can abort the transaction to continue using this transactional ID.
        \\ INVALID_RECORD_STATE =  121,  //The record state is invalid. The acknowledgement of delivery could not be completed.
        \\ SHARE_SESSION_NOT_FOUND =  122,  //The share session was not found.
        \\ INVALID_SHARE_SESSION_EPOCH =  123,  //The share session epoch is invalid.
        \\ FENCED_STATE_EPOCH =  124,  //The share coordinator rejected the request because the share-group state epoch did not match.
        \\ INVALID_VOTER_KEY =  125,  //The voter key doesn't match the receiving replica's key.
        \\ DUPLICATE_VOTER =  126,  //The voter is already part of the set of voters.
        \\ VOTER_NOT_FOUND =  127,  //The voter is not part of the set of voters.
        \\ INVALID_REGULAR_EXPRESSION =  128,  //The regular expression is not valid.
        \\ REBOOTSTRAP_REQUIRED =  129,  //Client metadata is stale. The client should rebootstrap to obtain new metadata.
        \\ STREAMS_INVALID_TOPOLOGY =  130,  //The supplied topology is invalid.
        \\ STREAMS_INVALID_TOPOLOGY_EPOCH =  131,  //The supplied topology epoch is invalid.
        \\ STREAMS_TOPOLOGY_FENCED =  132,  //The supplied topology epoch is outdated.
        \\ SHARE_SESSION_LIMIT_REACHED =  133,  //The limit of share sessions has been reached
        \\ _, // Allow unknown errors
        \\};
    );
}

fn generateAllStructs(io: Io, arena: std.mem.Allocator, writer: *Io.Writer, input_files: []const []const u8) !void {
    try writer.writeAll("const std = @import(\"std\");");

    try writeErrorEnum(writer);

    try writer.writeAll(
        \\ pub fn writeUnsignedVarInt(writer: *std.Io.Writer, value: usize) !void {
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
        \\ pub fn readUnsignedVarInt(bytes: []const u8) !struct { value: usize, bytes_read: usize } {
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

    var buffer: [1024]u8 = undefined;
    var cwd = Io.Dir.cwd();

    for (input_files) |file| {
        std.log.debug("protocol file: {s}", .{file});

        const input = try cwd.openFile(io, file, .{ .allow_directory = false });
        var reader = input.readerStreaming(io, &buffer);

        var file_content: std.ArrayList(u8) = .empty;

        while (true) {
            const line = reader.interface.takeDelimiterExclusive('\n') catch |err| switch (err) {
                error.EndOfStream => break,
                else => return err,
            };

            reader.interface.discardAll(1) catch |err| switch (err) {
                error.EndOfStream => break,
                else => return err,
            };

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

    // arrays of basic types
    if (std.mem.eql(u8, kafka_type, "[]int8")) return "[]i8";
    if (std.mem.eql(u8, kafka_type, "[]int16")) return "[]i16";
    if (std.mem.eql(u8, kafka_type, "[]int32")) return "[]i32";
    if (std.mem.eql(u8, kafka_type, "[]int64")) return "[]i64";
    if (std.mem.eql(u8, kafka_type, "[]bool")) return "[]bool";
    if (std.mem.eql(u8, kafka_type, "[]string")) return "[][]const u8";
    if (std.mem.eql(u8, kafka_type, "[]bytes")) return "[][]const u8";
    if (std.mem.eql(u8, kafka_type, "[]records")) return "[][]const u8";
    if (std.mem.eql(u8, kafka_type, "[]uuid")) return "[][16]u8";
    if (std.mem.eql(u8, kafka_type, "[]float64")) return "[]f64";

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
        \\ // leaky deserialize implementation, zero copy for slices
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

    try createDeserialiseFields(protocol_json.fields, version, is_flexible, writer);

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

    // TODO handle sub types properly
    if (field.type[0] >= 'A' and field.type[0] <= 'Z') return ".{}";

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

fn createDeserialiseFields(
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
    try createDeserialiseFields(protocol_field.fields, version, is_flexible, writer);

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
    if (std.mem.eql(u8, kafka_type, "int8")) return createDeserialiseIntField(i8, field, writer);
    if (std.mem.eql(u8, kafka_type, "int16"))
        if (std.mem.eql(u8, field.snake_name, "error_code"))
            return createDeserialiseResponseError(field, writer)
        else
            return createDeserialiseIntField(i16, field, writer);
    if (std.mem.eql(u8, kafka_type, "int32")) return createDeserialiseIntField(i32, field, writer);
    if (std.mem.eql(u8, kafka_type, "int64")) return createDeserialiseIntField(i64, field, writer);
    if (std.mem.eql(u8, kafka_type, "bool")) return createDeserialiseBool(field, writer);
    if (std.mem.eql(u8, kafka_type, "string")) return createDeserialiseBytes(field, version, is_flexible, writer);
    if (std.mem.eql(u8, kafka_type, "bytes")) return createDeserialiseBytes(field, version, is_flexible, writer);
    if (std.mem.eql(u8, kafka_type, "records")) return createDeserialiseBytes(field, version, is_flexible, writer);
    if (std.mem.eql(u8, kafka_type, "uuid")) return createDeserialiseUuid(field, writer);
    if (std.mem.eql(u8, kafka_type, "float64")) @panic("float64");

    // Fallback for custom nested arrays (e.g. "[]Topic")
    if (kafka_type.len > 2 and kafka_type[0] == '[' and kafka_type[1] == ']') {
        try createDeserialiseArray(field, is_flexible, writer);
    } else {
        try createDeserialiseType(field, writer);
    }
    return true;
}

fn createDeserialiseUuid(
    field: ProtocolField,
    writer: *Io.Writer,
) !bool {
    try writer.print(
        \\{{
        \\    if (current_offset + 16 > bytes.len) {{
        \\       return error.TooShort;
        \\    }}
        \\    @memcpy(&self.{s}, bytes[current_offset..current_offset+16]);
        \\    current_offset += 16;
        \\}}
    , .{field.snake_name});

    return false;
}

fn createDeserialiseType(field: ProtocolField, writer: *Io.Writer) !void {
    try writer.print(
        \\ {{
        \\   current_offset += try self.{s}.deserialise(allocator, bytes[current_offset..]);
        \\ }}
    , .{field.snake_name});
}

fn createDeserialiseArray(
    field: ProtocolField,
    is_flexible: bool,
    writer: *Io.Writer,
) !void {
    if (!std.mem.eql(u8, "[]", field.type[0..2])) {
        std.log.err("Expecting array but got {s}", .{field.type});
        return error.ExpectingArray;
    }
    const kafka_type = field.type[2..];
    const type_name = mapKafkaType(kafka_type);

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
        \\     self.{s} = values;
        \\     for (values) |*value| {{
    , .{ field.snake_name, field.snake_name, type_name, field.snake_name });

    if (std.mem.eql(u8, kafka_type, "int32")) {
        _ = try createDeserialiseInt(i32, "value.*", writer);
    } else if (kafka_type[0] >= 'a' and kafka_type[0] <= 'z') {
        @panic(kafka_type);
    } else {
        try writer.writeAll(
            \\         current_offset += try value.deserialise(allocator, bytes[current_offset..]);
        );
    }

    try writer.writeAll(
        \\     }
        \\ }
        \\
    );

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
    value_name: []const u8,
    writer: *Io.Writer,
) !bool {
    try writer.print(
        \\{{
        \\    const size = {};
        \\    if (current_offset + size > bytes.len) return error.TooShort;
        \\    {s} = std.mem.readInt({}, bytes[current_offset .. current_offset + size][0..size], .big);
        \\    current_offset += size;
        \\}}
    , .{ @sizeOf(T), value_name, T });

    return false;
}

fn createDeserialiseIntField(
    T: type,
    field: ProtocolField,
    writer: *Io.Writer,
) !bool {
    var name_buffer: [128]u8 = undefined;
    const value_name = try std.fmt.bufPrint(&name_buffer, "self.{s}", .{field.snake_name});
    return createDeserialiseInt(T, value_name, writer);
}

fn createDeserialiseResponseError(
    field: ProtocolField,
    writer: *Io.Writer,
) !bool {
    try writer.print(
        \\{{
        \\    const size = 2;
        \\    if (current_offset + size > bytes.len) return error.TooShort;
        \\    const error_code: ResponseError = @enumFromInt(std.mem.readInt(i16, bytes[current_offset .. current_offset + size][0..size], .big));
        \\    if (@This().version != 0 and error_code == .UNSUPPORTED_VERSION) {{
        \\        return error.UnsupportedVersion;
        \\    }}
        \\    self.{s} = error_code;
        \\    current_offset += size;
        \\}}
    , .{field.snake_name});

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
    try createSerialiseFields(
        arena,
        protocol_json.fields,
        version,
        is_flexible,
        writer,
    );

    try writer.writeAll(
        \\   try writer.flush();
        \\ }
    );
}

fn createSerialiseSubType(
    arena: std.mem.Allocator,
    protocol_field: ProtocolField,
    version: usize,
    is_flexible: bool,
    writer: *Io.Writer,
) !void {
    try createSerialiseFields(
        arena,
        protocol_field.fields,
        version,
        is_flexible,
        writer,
    );

    try writer.writeAll(
        \\ }
    );
}

fn createSerialiseFields(
    arena: std.mem.Allocator,
    fields: []ProtocolField,
    version: usize,
    is_flexible: bool,
    writer: *Io.Writer,
) !void {
    try writer.writeAll(
        \\ pub fn serialise(self: *const @This(), writer: *std.Io.Writer) !void {
        \\
    );
    var has_field = false;
    for (fields) |field| {
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
}

fn createSerialiseField(
    arena: std.mem.Allocator,
    field: ProtocolField,
    version: usize,
    is_flexible: bool,
    writer: *Io.Writer,
) !void {
    _ = arena;

    const is_nullable = if (field.nullableVersions) |n|
        (try VersionRange.parse(n)).contains(version)
    else
        false;

    const kafka_type = field.type;
    if (std.mem.eql(u8, kafka_type, "int8"))
        return createSerialiseInt(i8, field, writer);
    if (std.mem.eql(u8, kafka_type, "int16"))
        return createSerialiseInt(i16, field, writer);
    if (std.mem.eql(u8, kafka_type, "int32"))
        return createSerialiseInt(i32, field, writer);
    if (std.mem.eql(u8, kafka_type, "int64"))
        return createSerialiseInt(i64, field, writer);
    if (std.mem.eql(u8, kafka_type, "bool"))
        return createSerialiseBool(field, writer);
    if (std.mem.eql(u8, kafka_type, "string"))
        return createSerialiseBytes(field, is_nullable, is_flexible, writer);
    if (std.mem.eql(u8, kafka_type, "bytes"))
        return createSerialiseBytes(field, is_nullable, is_flexible, writer);
    if (std.mem.eql(u8, kafka_type, "records"))
        return createSerialiseBytes(field, is_nullable, is_flexible, writer);
    if (std.mem.eql(u8, kafka_type, "uuid"))
        return try createSerialiseUuid(field, writer);
    if (std.mem.eql(u8, kafka_type, "float64")) @panic("float64");

    try createSerialiseArray(field, is_nullable, is_flexible, writer);
}

fn createSerialiseArray(
    field: ProtocolField,
    is_nullable: bool,
    is_flexible: bool,
    writer: *Io.Writer,
) !void {
    if (is_nullable) {
        try writer.print("if (self.{s}) |slice| {{", .{field.snake_name});
    } else {
        try writer.print("{{ const slice = self.{s};", .{field.snake_name});
    }

    if (is_flexible) {
        try writer.writeAll(
            \\ try writeUnsignedVarInt(writer, slice.len + 1);
        );
    } else {
        try writer.writeAll(
            \\ try writer.writeInt(i32, slice.len, .big);
        );
    }

    try writer.writeAll(
        \\for (slice) |value| {
        \\      try value.serialise(writer);
        \\ }
    );

    try writer.writeAll("}");

    if (is_nullable) {
        if (is_flexible) {
            try writer.writeAll(
                \\ else {
                \\   try writer.writeByte(0);
                \\ }   
            );
        } else {
            // i32 -1
            try writer.writeAll(
                \\ else {
                \\   try writer.writeAll(&.{0xFF,0xFF,0xFF,0xFF});
                \\ }   
            );
        }
    }
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

fn createSerialiseUuid(
    field: ProtocolField,
    writer: *Io.Writer,
) !void {
    try writer.print(
        \\ {{
        \\   try writer.writeAll(&self.{s});
        \\ }}
    , .{field.snake_name});
}

fn createSerialiseBool(
    field: ProtocolField,
    writer: *Io.Writer,
) !void {
    try writer.print(
        \\ {{
        \\   try writer.writeByte(@intFromBool(self.{s}));
        \\ }}
    , .{field.snake_name});
}

fn createSerialiseBytes(
    field: ProtocolField,
    is_nullable: bool,
    is_flexible: bool,
    writer: *Io.Writer,
) !void {
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

    const maybe_sub_type: ?[]const u8 =
        if (field.type[0] == '[' and field.type[1] == ']' and field.type[2] >= 'A' and field.type[2] <= 'Z')
            field.type[2..]
        else if (field.type[0] >= 'A' and field.type[0] <= 'Z')
            field.type
        else
            null;

    if (maybe_sub_type) |sub_type| {
        for (field.fields) |sub_field| {
            try mapSubtype(io, arena, sub_field, version, is_request, is_flexible, writer);
        }

        try writer.print(
            \\pub const {s} = struct {{
            \\   const version = {};
        , .{ sub_type, version });

        for (field.fields) |sub_field| {
            try mapField(sub_field, version, writer);
        }

        if (is_request) {
            try createSerialiseSubType(arena, field, version, is_flexible, writer);
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
    var zig_type = mapKafkaType(field.type);

    if (std.mem.eql(u8, "error_code", field.snake_name) and std.mem.eql(u8, "i16", zig_type)) {
        zig_type = "ResponseError";
    }

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
