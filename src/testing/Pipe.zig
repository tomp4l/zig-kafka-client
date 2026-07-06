const std = @import("std");
const Io = std.Io;

const QueueMessage = enum(u1) {
    data_available,
};

io: Io,
reader: Io.Reader,
writer: Io.Writer,
mutex: Io.Mutex = .init,
last_failure: ?anyerror = null,

socket_buffer: []u8 = undefined,
socket_queue: Io.Queue(QueueMessage),
socket_seek: usize = 0,
socket_end: usize = 0,

fn stream(self: *Io.Reader, writer: *Io.Writer, limit: Io.Limit) Io.Reader.StreamError!usize {
    const pipe: *@This() = @fieldParentPtr("reader", self);

    while (true) {
        pipe.mutex.lock(pipe.io) catch return error.EndOfStream;

        const available = pipe.socket_end - pipe.socket_seek;
        if (available > 0) {
            const total = limit.minInt(available);
            defer pipe.mutex.unlock(pipe.io);

            try writer.writeAll(pipe.socket_buffer[pipe.socket_seek .. pipe.socket_seek + total]);
            pipe.socket_seek += total;

            if (pipe.socket_seek == pipe.socket_end) {
                pipe.socket_seek = 0;
                pipe.socket_end = 0;
            }

            return total;
        } else {
            pipe.mutex.unlock(pipe.io);

            const next = pipe.socket_queue.getOne(pipe.io) catch |err| switch (err) {
                error.Canceled => return error.EndOfStream,
                error.Closed => return error.EndOfStream,
            };

            switch (next) {
                .data_available => {},
            }
        }
    }
}

pub fn sendData(self: *@This(), bytes: []const u8) !void {
    try self.mutex.lock(self.io);
    defer self.mutex.unlock(self.io);

    const start = self.socket_end;
    const end = start + bytes.len;

    if (end > self.socket_buffer.len) return error.MockSocketBufferFull;

    @memcpy(self.socket_buffer[start..end], bytes);
    self.socket_end = end;

    try self.socket_queue.putOne(self.io, .data_available);
}

fn drain(self: *Io.Writer, data: []const []const u8, splat: usize) Io.Writer.Error!usize {
    const pipe: *@This() = @fieldParentPtr("writer", self);
    var size: usize = 0;

    if (data.len > 1) {
        for (data[0 .. data.len - 1]) |d| {
            pipe.sendData(d) catch |e| {
                pipe.last_failure = e;
                return error.WriteFailed;
            };
            size += d.len;
        }
    }
    if (splat > 0) {
        const pattern = data[data.len - 1];
        for (0..splat) |_| {
            pipe.sendData(pattern) catch |e| {
                pipe.last_failure = e;
                return error.WriteFailed;
            };
            size += pattern.len;
        }
    }
    return size;
}

pub fn init(
    io: Io,
    reader_buffer: []u8,
    socket_buffer: []u8,
    queue_buffer: []QueueMessage,
) @This() {
    return .{
        .io = io,
        .reader = .{
            .buffer = reader_buffer,
            .vtable = &.{ .stream = stream },
            .seek = 0,
            .end = 0,
        },
        .writer = .{
            .buffer = &.{},
            .vtable = &.{ .drain = drain },
        },
        .socket_queue = .init(queue_buffer),
        .socket_buffer = socket_buffer,
    };
}

pub const DefaultPipeBuffers = struct {
    reader_buffer: [1024]u8 = undefined,
    queue_buffer: [32]QueueMessage = undefined,
    socket_buffer: [4096]u8 = undefined,

    pub const init: @This() = .{};
};

pub fn initWithBuffers(io: Io, buffers: *DefaultPipeBuffers) @This() {
    return .init(
        io,
        &buffers.reader_buffer,
        &buffers.socket_buffer,
        &buffers.queue_buffer,
    );
}

pub fn close(self: *@This()) void {
    self.socket_queue.close(self.io);
}

test "pipe behaviour" {
    const io = std.testing.io;

    var buffers: DefaultPipeBuffers = .init;
    var pipe = initWithBuffers(io, &buffers);

    const ReadFromReader = struct {
        fn f(reader: *Io.Reader) ![]u8 {
            const v = try reader.take(3);

            return v;
        }
    };

    var future = try io.concurrent(ReadFromReader.f, .{&pipe.reader});

    try io.sleep(.fromMilliseconds(1), .real);

    try pipe.sendData("ab");
    try pipe.sendData("cd");

    const result = try future.await(io);

    try std.testing.expectEqualStrings("abc", result);
    try std.testing.expectEqualStrings("d", try pipe.reader.take(1));

    pipe.close();

    try std.testing.expectError(error.EndOfStream, pipe.reader.take(1));
}
