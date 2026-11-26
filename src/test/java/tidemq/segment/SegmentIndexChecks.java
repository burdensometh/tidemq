package tidemq.segment;

import tidemq.Boundary;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.Comparator;

/**
 * Segment-level checks for the persisted offset index, registered as a case in
 * {@code tidemq.BrokerTest}. Lives in this package to exercise the package-private
 * segment internals directly.
 */
public final class SegmentIndexChecks {
    private static final TopicPartition PARTITION = new TopicPartition("orders", 0);

    private SegmentIndexChecks() {}

    public static void check() throws Exception {
        roundtripStaleTornAndRemove();
        validIndexSkipsScanButNeverServesCorruptRecord();
    }

    private static void roundtripStaleTornAndRemove() throws Exception {
        Path dir = Files.createTempDirectory("tidemq-segment-");
        try {
            Path segPath = dir.resolve(Segment.fileName(0));
            Segment segment = Segment.create(dir, PARTITION, 0, 1000);
            for (int i = 0; i < 3; i++) {
                segment.append(message(i, 1001 + i), Boundary.NONE);
            }
            segment.persistIndex();
            Path indexPath = SegmentIndex.pathFor(segPath);
            check(Files.isRegularFile(indexPath), "persistIndex must create the index file");

            Segment restored = Segment.openSealed(segPath, PARTITION, false);
            eq(segment.view(false), restored.view(false));
            for (int i = 0; i < 3; i++) {
                eq(segment.get(i), restored.get(i));
            }

            // An index left behind by a grown segment is stale and ignored.
            segment.append(message(3, 1004), Boundary.NONE);
            Segment scanned = Segment.openSealed(segPath, PARTITION, true);
            eq(4L, scanned.endOffset());
            eq(segment.view(false), scanned.view(false));
            check(Files.isRegularFile(indexPath), "fallback scan must rewrite the index");

            // A torn index is ignored without being rewritten when not upgrading.
            byte[] valid = Files.readAllBytes(indexPath);
            Files.write(indexPath, Arrays.copyOf(valid, valid.length / 2));
            eq(4L, Segment.openSealed(segPath, PARTITION, false).endOffset());
            check(Files.size(indexPath) == valid.length / 2, "read-only open must not rewrite the index");
            eq(4L, Segment.openSealed(segPath, PARTITION, true).endOffset());
            check(Arrays.equals(valid, Files.readAllBytes(indexPath)), "rebuilt index must be deterministic");

            segment.remove();
            check(!Files.exists(segPath), "remove must delete the segment");
            check(!Files.exists(indexPath), "remove must delete the index");
        } finally {
            sweep(dir);
        }
    }

    private static void validIndexSkipsScanButNeverServesCorruptRecord() throws Exception {
        Path dir = Files.createTempDirectory("tidemq-segment-");
        try {
            Path segPath = dir.resolve(Segment.fileName(0));
            Segment segment = Segment.create(dir, PARTITION, 0, 1000);
            for (int i = 0; i < 3; i++) {
                segment.append(message(i, 1001 + i), Boundary.NONE);
            }
            segment.persistIndex();

            // Damage the middle record body; the first and last records stay intact.
            byte[] bytes = Files.readAllBytes(segPath);
            int firstLength = ByteBuffer.wrap(bytes, RecordFile.HEADER_BYTES, Integer.BYTES).getInt();
            int middle = RecordFile.HEADER_BYTES + RecordFile.RECORD_OVERHEAD + firstLength;
            bytes[middle + Integer.BYTES + 2] ^= 1;
            Files.write(segPath, bytes);

            // The valid index opens the segment without a full record scan, but the
            // damaged record is still rejected when read, never served as legitimate.
            Segment viaIndex = Segment.openSealed(segPath, PARTITION, false);
            eq(3L, viaIndex.endOffset());
            eq(message(0, 1001), viaIndex.get(0));
            eq(message(2, 1003), viaIndex.get(2));
            ioFails(() -> viaIndex.get(1));
            ioFails(() -> Segment.open(segPath, PARTITION, false));
        } finally {
            sweep(dir);
        }
    }

    private static Message message(int offset, long appendedAtMs) {
        return new Message(PARTITION, offset, appendedAtMs, "p", offset,
                new byte[0], ("v" + offset).getBytes(StandardCharsets.UTF_8));
    }

    private static void eq(Object expected, Object observed) {
        if (!expected.equals(observed)) {
            throw new AssertionError("expected " + expected + ", got " + observed);
        }
    }

    private static void check(boolean predicate, String detail) {
        if (!predicate) {
            throw new AssertionError(detail);
        }
    }

    private static void ioFails(ThrowingAction action) throws Exception {
        try {
            action.run();
        } catch (IOException expected) {
            return;
        }
        throw new AssertionError("expected IOException");
    }

    private static void sweep(Path dir) throws IOException {
        try (var tree = Files.walk(dir)) {
            for (Path path : tree.sorted(Comparator.reverseOrder()).toList()) {
                Files.delete(path);
            }
        }
    }

    @FunctionalInterface
    private interface ThrowingAction {
        void run() throws Exception;
    }
}
