package tidemq.segment;

import tidemq.Binary;

import java.io.ByteArrayInputStream;
import java.io.DataInputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Locale;

/**
 * Persistent offset index for a sealed v1 segment, stored as a {@code <base>.idx}
 * sibling of the segment file. The index is only a hint: the segment remains the
 * source of truth. Indexes are replaced atomically when a segment is sealed and
 * are validated against the segment header, the file size and the first and last
 * record before use; any disagreement falls back to a full v1 record scan.
 */
final class SegmentIndex {
    static final String EXTENSION = ".idx";
    private static final int MAGIC = 0x544D5349; // TMSI
    private static final int VERSION = 1;
    private static final int FIXED_BYTES = 44; // magic, version, base, created, latest, segmentBytes, count
    private static final int ENTRY_BYTES = Long.BYTES;
    private static final int FOOTER_BYTES = Integer.BYTES;
    private static final long MAX_BYTES = 512L * 1024 * 1024;

    record Loaded(long segmentCreatedMs, long latestAppendMs, long segmentBytes, long[] positions) {}

    private SegmentIndex() {}

    static String fileName(long baseOffset) {
        return String.format(Locale.ROOT, "%020d%s", baseOffset, EXTENSION);
    }

    static Path pathFor(Path segmentFile) {
        String name = segmentFile.getFileName().toString();
        return segmentFile.resolveSibling(
                name.substring(0, name.length() - Segment.EXTENSION.length()) + EXTENSION);
    }

    static void write(Path segmentFile, long base, long segmentCreatedMs, long latestAppendMs,
                      long segmentBytes, long[] positions, int count) throws IOException {
        byte[] body = Binary.encode(out -> {
            out.writeInt(MAGIC);
            out.writeInt(VERSION);
            out.writeLong(base);
            out.writeLong(segmentCreatedMs);
            out.writeLong(latestAppendMs);
            out.writeLong(segmentBytes);
            out.writeInt(count);
            for (int i = 0; i < count; i++) {
                out.writeLong(positions[i]);
            }
        });
        byte[] content = ByteBuffer.allocate(body.length + FOOTER_BYTES)
                .put(body).putInt(Disk.checksum(body)).array();
        Disk.replace(pathFor(segmentFile), content, () -> {});
    }

    static Loaded load(Path segmentFile, long expectedBase) throws IOException {
        Path path = pathFor(segmentFile);
        if (!Files.isRegularFile(path)) {
            return null;
        }
        long size = Files.size(path);
        if (size < FIXED_BYTES + ENTRY_BYTES + FOOTER_BYTES || size > MAX_BYTES) {
            throw new IOException("invalid segment index size " + size + ": " + path);
        }
        try {
            return decode(Files.readAllBytes(path), expectedBase);
        } catch (IOException | IllegalArgumentException invalid) {
            throw new IOException("reading " + path + ": " + invalid.getMessage(), invalid);
        }
    }

    private static Loaded decode(byte[] bytes, long expectedBase) throws IOException {
        int bodyLength = bytes.length - FOOTER_BYTES;
        if (Disk.checksum(bytes, 0, bodyLength)
                != ByteBuffer.wrap(bytes, bodyLength, FOOTER_BYTES).getInt()) {
            throw new IOException("segment index checksum mismatch");
        }
        var in = new DataInputStream(new ByteArrayInputStream(bytes, 0, bodyLength));
        if (in.readInt() != MAGIC || in.readInt() != VERSION) {
            throw new IOException("invalid v1 segment index header");
        }
        long base = in.readLong();
        long created = in.readLong();
        long latest = in.readLong();
        long segmentBytes = in.readLong();
        int count = in.readInt();
        if (base != expectedBase || created < 0 || latest < 0
                || segmentBytes < RecordFile.HEADER_BYTES || count < 1
                || (long) FIXED_BYTES + (long) count * ENTRY_BYTES != bodyLength
                || count > (segmentBytes - RecordFile.HEADER_BYTES) / (RecordFile.RECORD_OVERHEAD + 1)) {
            throw new IOException("invalid v1 segment index header");
        }
        long[] positions = new long[count];
        long previous = -1;
        for (int i = 0; i < count; i++) {
            long position = in.readLong();
            long minimum = previous < 0 ? RecordFile.HEADER_BYTES : previous + RecordFile.RECORD_OVERHEAD + 1;
            if (position < minimum || position >= segmentBytes) {
                throw new IOException("segment index positions are not ordered");
            }
            positions[i] = position;
            previous = position;
        }
        if (positions[0] != RecordFile.HEADER_BYTES
                || segmentBytes - positions[count - 1] < RecordFile.RECORD_OVERHEAD + 1) {
            throw new IOException("segment index does not cover the segment");
        }
        if (in.read() != -1) {
            throw new IOException("unexpected trailing bytes in segment index");
        }
        return new Loaded(created, latest, segmentBytes, positions);
    }
}
