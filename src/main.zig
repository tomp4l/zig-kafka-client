const std = @import("std");
const Io = std.Io;

const kafka_client = @import("kafka_client");

const protocol = @import("protocol");

pub fn main(init: std.process.Init) !void {
    const host_name = try Io.net.HostName.init("localhost");

    const socket = try host_name.connect(init.io, 9092, .{ .mode = .stream });
    defer socket.close(init.io);

    var read_buf: [4096]u8 = undefined;
    var write_buf: [4096]u8 = undefined;
    var stdout_buf: [4096]u8 = undefined;

    var reader = socket.reader(init.io, &read_buf);
    var writer = socket.writer(init.io, &write_buf);

    const stdout = Io.File.stdout();
    var stdout_writer = stdout.writer(init.io, &stdout_buf);

    const payload = [_]u8{
        0x00, 0x00, 0x00, 0x0a, // Size: 10
        0x00, 0x12, // API Key: 18
        0x00, 0x00, // API Version: 0
        0x00, 0x00, 0x00, 0x01, // Correlation ID: 1
        0xff, 0xff, // Client ID: null (-1)
    };

    try writer.interface.writeAll(&payload);
    try writer.interface.flush();
    _ = try reader.interface.stream(&stdout_writer.interface, .unlimited);
    try stdout_writer.interface.flush();
}
