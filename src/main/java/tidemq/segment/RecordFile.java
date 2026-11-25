package tidemq.segment;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;

public final class RecordFile {
    public static final int VERSION = 1;
    public static final int HEADER_BYTES = 24;
    public static final int RECORD_OVERHEAD = 8;
    public static final int MAX_BODY = 9 * 1024 * 1024;

    public record Header(int magic, long baseOffset, long createdAtMs) {}

    @FunctionalInterface
    public interface Visitor {
        void accept(long position, byte[] body) throws IOException;
    }

    private final Path path;
    private final Header header;
    private long end;

    private RecordFile(Path path, Header header, long end) {
        this.path = path;
        this.header = header;
        this.end = end;
    }

    public static RecordFile create(Path path, Header header) throws IOException {
        if (Files.exists(path)) {
            throw new IOException("record file already exists: " + path);
        }
        ByteBuffer bytes = ByteBuffer.allocate(HEADER_BYTES);
        bytes.putInt(header.magic()).putInt(VERSION).putLong(header.baseOffset()).putLong(header.createdAtMs());
        Disk.replace(path, bytes.array(), () -> {});
        return new RecordFile(path, header, HEADER_BYTES);
    }

    public static RecordFile open(Path path, int magic, long expectedBase, boolean repairTail, Visitor visitor)
            throws IOException {
        StandardOpenOption[] options = repairTail
                ? new StandardOpenOption[] {StandardOpenOption.READ, StandardOpenOption.WRITE}
                : new StandardOpenOption[] {StandardOpenOption.READ};
        try (FileChannel file = FileChannel.open(path, options)) {
            Header header = readHeader(file, magic);
            if (header.baseOffset() != expectedBase) {
                throw new IOException("record header base offset disagrees with expected base " + expectedBase);
            }
            long end = scan(file, repairTail, visitor);
            return new RecordFile(path, header, end);
        } catch (IOException error) {
            throw new IOException("reading " + path + ": " + error.getMessage(), error);
        }
    }

    private static Header readHeader(FileChannel file, int magic) throws IOException {
        ByteBuffer bytes = Disk.read(file, 0, HEADER_BYTES);
        int observedMagic = bytes.getInt();
        int version = bytes.getInt();
        long base = bytes.getLong();
        long created = bytes.getLong();
        if (observedMagic != magic || version != VERSION || base < 0 || created < 0) {
            throw new IOException("invalid v1 record file header");
        }
        return new Header(magic, base, created);
    }

    private static long scan(FileChannel file, boolean repairTail, Visitor visitor) throws IOException {
        long size = file.size();
        long position = HEADER_BYTES;
        while (position < size) {
            long remaining = size - position;
            if (remaining < Integer.BYTES) {
                return shortTail(file, position, repairTail);
            }
            int length = readLength(file, position);
            if (remaining < (long) RECORD_OVERHEAD + length) {
                return shortTail(file, position, repairTail);
            }
            visitor.accept(position, readBody(file, position, length));
            position += RECORD_OVERHEAD + length;
        }
        return position;
    }

    private static long shortTail(FileChannel file, long position, boolean repairTail) throws IOException {
        if (!repairTail) {
            throw new IOException("incomplete record at byte " + position);
        }
        file.truncate(position);
        file.force(true);
        return position;
    }

    private static int readLength(FileChannel file, long position) throws IOException {
        int length = Disk.read(file, position, Integer.BYTES).getInt();
        if (length <= 0 || length > MAX_BODY) {
            throw new IOException("invalid record length " + length + " at byte " + position);
        }
        return length;
    }

    private static byte[] readBody(FileChannel file, long position, int length) throws IOException {
        ByteBuffer bytes = Disk.read(file, position + Integer.BYTES, length + Integer.BYTES);
        byte[] body = new byte[length];
        bytes.get(body);
        if (bytes.getInt() != Disk.checksum(body)) {
            throw new IOException("checksum mismatch at byte " + position);
        }
        return body;
    }

    public long append(byte[] body, Disk.Commit beforeForce) throws IOException {
        if (body.length == 0 || body.length > MAX_BODY) {
            throw new IOException("invalid record size " + body.length);
        }
        long position = end;
        ByteBuffer encoded = ByteBuffer.allocate(RECORD_OVERHEAD + body.length);
        encoded.putInt(body.length).put(body).putInt(Disk.checksum(body)).flip();
        try (FileChannel file = FileChannel.open(path, StandardOpenOption.WRITE)) {
            if (file.size() != position) {
                throw new IOException("record file size changed outside broker: " + path);
            }
            file.position(position);
            Disk.write(file, encoded);
            beforeForce.apply();
            file.force(true);
            end += RECORD_OVERHEAD + body.length;
        }
        return position;
    }

    public byte[] read(long position) throws IOException {
        try (FileChannel file = FileChannel.open(path, StandardOpenOption.READ)) {
            return readBody(file, position, readLength(file, position));
        }
    }

    public void forEach(Visitor visitor) throws IOException {
        try (FileChannel file = FileChannel.open(path, StandardOpenOption.READ)) {
            scan(file, false, visitor);
        }
    }

    public Path path() {
        return path;
    }

    public Header header() {
        return header;
    }

    public long size() {
        return end;
    }
}
