package tidemq.segment;

import tidemq.Binary;
import tidemq.Boundary;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.Locale;
import java.util.function.LongSupplier;

public final class Segment {
    private static final System.Logger LOG = System.getLogger(Segment.class.getName());

    public static final int MAGIC = 0x544D5347; // TMSG
    static final String EXTENSION = ".seg";

    public record View(long baseOffset, long endOffset, long sizeBytes,
                       long createdAtMs, long latestAppendMs, boolean active) {}

    @FunctionalInterface
    public interface MessageVisitor {
        void accept(Message message) throws IOException;
    }

    private final TopicPartition partition;
    private final RecordFile file;
    private final Index index;

    private Segment(TopicPartition partition, RecordFile file, Index index) {
        this.partition = partition;
        this.file = file;
        this.index = index;
    }

    public static String fileName(long baseOffset) {
        return String.format(Locale.ROOT, "%020d%s", baseOffset, EXTENSION);
    }

    static Segment create(Path directory, TopicPartition partition, long base, long now) throws IOException {
        var file = RecordFile.create(directory.resolve(fileName(base)), new RecordFile.Header(MAGIC, base, now));
        return new Segment(partition, file, new Index(base));
    }

    public static Segment open(Path path, TopicPartition partition, boolean repairTail) throws IOException {
        long base = baseFromName(path);
        var index = new Index(base);
        RecordFile file = RecordFile.open(path, MAGIC, base, repairTail,
                (position, body) -> index.add(position, decode(body, partition)));
        return new Segment(partition, file, index);
    }

    /**
     * Opens a sealed segment, preferring its persisted offset index over a full
     * record scan. A missing, damaged or disagreeing index falls back to the v1
     * scan; when {@code upgrade} is set the rebuilt index is then persisted so
     * later restarts can skip the scan.
     */
    static Segment openSealed(Path path, TopicPartition partition, boolean upgrade) throws IOException {
        long base = baseFromName(path);
        try {
            SegmentIndex.Loaded loaded = SegmentIndex.load(path, base);
            if (loaded != null) {
                return restore(path, partition, base, loaded);
            }
        } catch (IOException invalid) {
            LOG.log(System.Logger.Level.WARNING, "ignoring segment index for " + path, invalid);
        }
        Segment scanned = open(path, partition, false);
        if (upgrade) {
            scanned.persistIndex();
        }
        return scanned;
    }

    private static Segment restore(Path path, TopicPartition partition, long base, SegmentIndex.Loaded loaded)
            throws IOException {
        RecordFile file = RecordFile.openHeader(path, MAGIC, base);
        if (loaded.segmentCreatedMs() != file.header().createdAtMs() || loaded.segmentBytes() != file.size()) {
            throw new IOException("persisted segment index disagrees with segment file");
        }
        long[] positions = loaded.positions();
        var segment = new Segment(partition, file, Index.restored(base, loaded.latestAppendMs(), positions));
        segment.get(base);
        Message last = segment.get(segment.endOffset() - 1);
        if (positions[positions.length - 1] + RecordFile.RECORD_OVERHEAD
                + MessageV1.wireBodyBytes(last) != file.size()) {
            throw new IOException("persisted segment index does not cover the whole segment");
        }
        return segment;
    }

    private static long baseFromName(Path path) throws IOException {
        String name = path.getFileName().toString();
        if (name.length() != 24 || !name.endsWith(EXTENSION)) {
            throw new IOException("invalid segment name " + name);
        }
        long base;
        try {
            base = Long.parseLong(name.substring(0, 20));
        } catch (NumberFormatException e) {
            throw new IOException("invalid segment base " + name, e);
        }
        if (base < 0 || !name.equals(fileName(base))) {
            throw new IOException("noncanonical segment name " + name);
        }
        return base;
    }

    private static Message decode(byte[] body, TopicPartition partition) throws IOException {
        try {
            return Binary.decode(body, in -> MessageV1.read(in, partition));
        } catch (IllegalArgumentException invalid) {
            throw new IOException("invalid persisted message for " + partition, invalid);
        }
    }

    boolean canFit(Message message, long maximumBytes) {
        return index.count == 0 || file.size() + RecordFile.RECORD_OVERHEAD
                + MessageV1.wireBodyBytes(message) <= maximumBytes;
    }

    void append(Message message, Boundary boundary) throws IOException {
        if (!partition.equals(message.partition()) || endOffset() != message.offset()) {
            throw new IOException("noncontiguous partition append");
        }
        byte[] body = Binary.encode(out -> MessageV1.write(out, message));
        long position = file.append(body, () -> boundary.reached(Boundary.Point.SEGMENT_BEFORE_FORCE));
        index.add(position, message);
        boundary.reached(Boundary.Point.SEGMENT_AFTER_FORCE);
    }

    Message get(long offset) throws IOException {
        Message message = decode(file.read(index.position(offset)), partition);
        if (message.offset() != offset) {
            throw new IOException("segment index disagreement at offset " + offset);
        }
        return message;
    }

    public void forEach(MessageVisitor visitor) throws IOException {
        file.forEach((position, body) -> visitor.accept(decode(body, partition)));
    }

    long baseOffset() {
        return file.header().baseOffset();
    }

    long endOffset() {
        return index.end();
    }

    public View view(boolean active) {
        long created = file.header().createdAtMs();
        return new View(baseOffset(), endOffset(), file.size(), created, Math.max(created, index.latest), active);
    }

    boolean eligibleForRetirement(long frontier, long now, long minimumAge) {
        long latest = Math.max(file.header().createdAtMs(), index.latest);
        return endOffset() <= frontier && now >= latest && now - latest >= minimumAge;
    }

    void remove() throws IOException {
        Files.delete(file.path());
        Files.deleteIfExists(SegmentIndex.pathFor(file.path()));
        Disk.forceDirectory(file.path().getParent());
    }

    void persistIndex() throws IOException {
        SegmentIndex.write(file.path(), baseOffset(), file.header().createdAtMs(), index.latest,
                file.size(), index.positions, index.count);
    }

    private static final class Index {
        private final long base;
        private long[] positions = new long[32];
        private int count;
        private long latest;

        Index(long base) {
            this.base = base;
        }

        static Index restored(long base, long latest, long[] positions) {
            var restored = new Index(base);
            restored.positions = positions;
            restored.count = positions.length;
            restored.latest = latest;
            return restored;
        }

        void add(long position, Message message) throws IOException {
            if (message.offset() != end()) {
                throw new IOException("expected offset " + end() + ", found " + message.offset());
            }
            if (count == positions.length) {
                positions = Arrays.copyOf(positions, Math.multiplyExact(count, 2));
            }
            positions[count++] = position;
            latest = Math.max(latest, message.appendedAtMs());
        }

        long position(long offset) throws IOException {
            if (offset < base || offset >= end()) {
                throw new IOException("offset " + offset + " outside segment " + base + ".." + end());
            }
            return positions[(int) (offset - base)];
        }

        long end() {
            return base + count;
        }
    }
}
