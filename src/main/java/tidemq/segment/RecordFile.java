package tidemq.segment;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.concurrent.atomic.LongAdder;

public final class RecordFile {
    public static final int VERSION = 1;
    public static final int HEADER_BYTES = 24;
    public static final int RECORD_OVERHEAD = 8;
    public static final int MAX_BODY = 9 * 1024 * 1024;

    private static final int READ_WINDOW_BYTES = 8 * 1024;
    private static final byte[] EMPTY_WINDOW = new byte[0];
    private static final LongAdder READ_OPENS = new LongAdder();

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

    public static long readOpenCount() {
        return READ_OPENS.sum();
    }

    public static void resetReadOpenCount() {
        READ_OPENS.reset();
    }

    private static FileChannel openForRead(Path path) throws IOException {
        READ_OPENS.increment();
        return FileChannel.open(path, StandardOpenOption.READ);
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
        try (Reader reader = reader()) {
            return reader.read(position);
        }
    }

    public Reader reader() {
        return new Reader(this);
    }

    public void forEach(Visitor visitor) throws IOException {
        try (FileChannel file = FileChannel.open(path, StandardOpenOption.READ)) {
            scan(file, false, visitor);
        }
    }

    /**
     * A read-only view over one record file that keeps its channel open and
     * buffers a byte window, so consecutive records in the same segment are
     * served without reopening the file or issuing one positional read each.
     */
    public static final class Reader implements AutoCloseable {
        private final RecordFile file;
        private FileChannel channel;
        private byte[] window = EMPTY_WINDOW;
        private long windowStart = -1;
        private long windowEnd = -1;

        private Reader(RecordFile file) {
            this.file = file;
        }

        public byte[] read(long position) throws IOException {
            byte[] body = block(position);
            if (body != null) {
                return body;
            }
            FileChannel channel = channel();
            int length = Disk.read(channel, position, Integer.BYTES).getInt();
            checkLength(length, position);
            return readBody(channel, position, length);
        }

        /** Returns the verified body when the window covers the whole record, else null. */
        private byte[] block(long position) throws IOException {
            if (!covers(position, Integer.BYTES)) {
                refresh(position);
                if (!covers(position, Integer.BYTES)) {
                    return null;
                }
            }
            int length = intAt(position);
            checkLength(length, position);
            long bodyPosition = position + Integer.BYTES;
            int block = length + Integer.BYTES;
            if (block > READ_WINDOW_BYTES) {
                return null;
            }
            if (!covers(bodyPosition, block)) {
                refresh(bodyPosition);
                if (!covers(bodyPosition, block)) {
                    return null;
                }
            }
            byte[] body = new byte[length];
            System.arraycopy(window, (int) (bodyPosition - windowStart), body, 0, length);
            if (intAt(bodyPosition + length) != Disk.checksum(body)) {
                throw new IOException("checksum mismatch at byte " + position);
            }
            return body;
        }

        private static void checkLength(int length, long position) throws IOException {
            if (length <= 0 || length > MAX_BODY) {
                throw new IOException("invalid record length " + length + " at byte " + position);
            }
        }

        private int intAt(long position) {
            int at = (int) (position - windowStart);
            return ((window[at] & 0xff) << 24) | ((window[at + 1] & 0xff) << 16)
                    | ((window[at + 2] & 0xff) << 8) | (window[at + 3] & 0xff);
        }

        private boolean covers(long position, int count) {
            return position >= windowStart && windowEnd - position >= count;
        }

        private FileChannel channel() throws IOException {
            if (channel == null) {
                channel = openForRead(file.path);
            }
            return channel;
        }

        private void refresh(long position) throws IOException {
            FileChannel source = channel();
            long size = file.end;
            if (position >= size) {
                window = EMPTY_WINDOW;
                windowStart = position;
                windowEnd = position;
                return;
            }
            int wanted = (int) Math.min(READ_WINDOW_BYTES, size - position);
            ByteBuffer bytes = Disk.read(source, position, wanted);
            window = bytes.array();
            windowStart = position;
            windowEnd = position + wanted;
        }

        @Override
        public void close() throws IOException {
            FileChannel open = channel;
            channel = null;
            window = EMPTY_WINDOW;
            windowStart = -1;
            windowEnd = -1;
            if (open != null) {
                open.close();
            }
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
