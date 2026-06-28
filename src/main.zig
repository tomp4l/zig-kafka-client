const std = @import("std");
const Io = std.Io;

const kafka_client = @import("kafka_client");

const protocol = @import("protocol");

pub fn main(init: std.process.Init) !void {
    const host_name = try Io.net.HostName.init("localhost");

    const arena = init.arena.allocator();
    const io = init.io;

    const socket = try host_name.connect(init.io, 9092, .{ .mode = .stream });
    defer socket.close(init.io);

    var read_buf: [4096]u8 = undefined;
    var write_buf: [4096]u8 = undefined;

    var reader = socket.reader(init.io, &read_buf);
    var writer = socket.writer(init.io, &write_buf);

    var connection = kafka_client.BrokerConnection.init(&reader.interface, &writer.interface);

    try connection.connect(io, arena);
    defer connection.close(io, arena);

    const req = protocol.ApiVersionsRequestV3{
        .client_software_name = "test",
        .client_software_version = "0.0.0",
    };

    const response = try connection.makeRequest(protocol.ApiVersionsResponseV3, io, arena, req);

    for (response.value.api_keys) |k| {
        std.debug.print("Key {}: {}-{}\n", .{ k.api_key, k.min_version, k.max_version });
    }
}

test {
    _ = @import("protocol_tests.zig");
}
