package tidemq;

import tidemq.deadletter.DeadLetterReceipt;
import tidemq.deadletter.DeadLetters;
import tidemq.segment.Message;
import tidemq.segment.TopicPartition;
import tidemq.wire.TcpBroker;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;

import static tidemq.TestSupport.*;

/** Focused checks for the v2 dead-letter format and its v1 migration. */
final class DeadLettersTests {
    private static final TopicPartition P0 = new TopicPartition("orders", 0);
    private static final Set<String> ORDERS = Set.of("orders");

    private DeadLettersTests() {}

    static DeadLetterReceipt receipt(long sequence, String group, int attempts, String payload) {
        var message = new Message(P0, sequence, 1_700_000_000_000L + sequence, "producer", sequence,
                bytes("key-" + sequence), payload.getBytes(StandardCharsets.UTF_8));
        return new DeadLetterReceipt(sequence, group, attempts, 1_700_000_100_000L + sequence, message);
    }

    /** Migration preserves every field and removes the v1 log only afterwards. */
    static void v1Migration() throws Exception {
        var receipts = List.of(
                receipt(0, "alpha", 2, "first"),
                receipt(1, "beta", 3, "second"),
                receipt(2, "alpha", 2, "third"));
        try (var tmp = new Scratch()) {
            Path legacy = legacyDeadLetterJournal(tmp.root, receipts);
            eq(true, Files.exists(legacy.resolve(DeadLetters.V1_FILE)));
            try (var store = new DeadLetters(legacy, Boundary.NONE, 1_700_000_000_000L)) {
                var collected = new ArrayList<DeadLetterReceipt>();
                store.forEach(collected::add);
                eq(receipts, collected);
            }
            eq(false, Files.exists(legacy.resolve(DeadLetters.V1_FILE)));
            eq(true, Files.isRegularFile(legacy.resolve(DeadLetters.META_FILE)));
            eq(true, Files.isRegularFile(legacy.resolve(DeadLetters.DATA_FILE)));
            // Reopening the already-migrated store keeps the same sequence and payloads.
            try (var store = new DeadLetters(legacy, Boundary.NONE, 1_700_000_000_000L)) {
                eq(receipts.get(2), store.read("alpha", 0, 8, Integer.MAX_VALUE).receipts().getLast());
                eq(3L, store.read("alpha", 0, 8, Integer.MAX_VALUE).nextSequence());
            }
        }
    }

    /** An interruption mid-migration leaves a prefix; reopening continues without duplication. */
    static void migrationResumes() throws Exception {
        var receipts = List.of(
                receipt(0, "alpha", 2, "first"),
                receipt(1, "alpha", 2, "second"),
                receipt(2, "alpha", 2, "third"),
                receipt(3, "beta", 4, "fourth"));
        try (var tmp = new Scratch()) {
            Path legacy = legacyDeadLetterJournal(tmp.root, receipts);
            var fault = new Fault();
            fault.arm(Boundary.Point.DEADLETTER_BEFORE_FORCE);
            // The constructor faults while forcing the first migrated payload.
            try (var ignored = new DeadLetters(legacy, fault, 1_700_000_000_000L)) {
                throw new AssertionError("armed fault must interrupt migration");
            } catch (IOException expected) {
                // The v1 log is untouched.
            }
            eq(true, Files.exists(legacy.resolve(DeadLetters.V1_FILE)));
            try (var store = new DeadLetters(legacy, Boundary.NONE, 1_700_000_000_000L)) {
                var collected = new ArrayList<DeadLetterReceipt>();
                store.forEach(collected::add);
                eq(receipts, collected);
            }
            eq(false, Files.exists(legacy.resolve(DeadLetters.V1_FILE)));
        }
    }

    /** Metadata and payload bytes are stored in separate logs. */
    static void separateStorage() throws Exception {
        try (var tmp = new Scratch()) {
            Path legacy = legacyDeadLetterJournal(tmp.root, List.of());
            try (var store = new DeadLetters(legacy, Boundary.NONE, 1_700_000_000_000L)) {
                byte[] payload = new byte[4096];
                payload[0] = 'x';
                var message = new Message(P0, 7, 1_700_000_000_000L, "p", 9, bytes("k"), payload);
                store.archive("alpha", message, 2, 1_700_000_100_000L);
            }
            long metaBytes = Files.size(legacy.resolve(DeadLetters.META_FILE));
            long dataBytes = Files.size(legacy.resolve(DeadLetters.DATA_FILE));
            eq(true, dataBytes > 4096);
            eq(true, metaBytes < 512);
        }
    }

    /** A cursor issued before migration finds the same rows, in order, after it. */
    static void pagingCursor() throws Exception {
        var receipts = List.of(
                receipt(0, "alpha", 2, "a0"),
                receipt(1, "beta", 3, "b1"),
                receipt(2, "alpha", 2, "a2"),
                receipt(3, "alpha", 2, "a3"),
                receipt(4, "beta", 2, "b4"));
        try (var tmp = new Scratch()) {
            Path legacy = legacyDeadLetterJournal(tmp.root, receipts);
            // Cursor values that a v1 reader would have used: global sequence
            // positions intersected with the group's own subset.
            try (var store = new DeadLetters(legacy, Boundary.NONE, 1_700_000_000_000L)) {
                var first = store.read("alpha", 0, 2, Integer.MAX_VALUE);
                eq(3L, first.nextSequence());
                eq(List.of(receipts.get(0), receipts.get(2)), first.receipts());
                var second = store.read("alpha", first.nextSequence(), 2, Integer.MAX_VALUE);
                eq(5L, second.nextSequence());
                eq(List.of(receipts.get(3)), second.receipts());
                var empty = store.read("alpha", second.nextSequence(), 2, Integer.MAX_VALUE);
                eq(List.of(), empty.receipts());
                eq(5L, empty.nextSequence());
                // The beta group keeps its own numbering without reordering.
                eq(List.of(receipts.get(1), receipts.get(4)),
                        store.read("beta", 0, 8, Integer.MAX_VALUE).receipts());
            }
        }
    }

    /** Two dead letters for one group and one source collapse into a single row. */
    static void archiveIsIdempotent() throws Exception {
        try (var tmp = new Scratch()) {
            Path legacy = legacyDeadLetterJournal(tmp.root, List.of());
            try (var store = new DeadLetters(legacy, Boundary.NONE, 1_700_000_000_000L)) {
                var message = receipt(0, "alpha", 2, "once").message();
                var first = store.archive("alpha", message, 2, 1_700_000_000_000L);
                var second = store.archive("alpha", message, 2, 1_700_000_500_000L);
                eq(first, second);
                eq(1, store.read("alpha", 0, 8, Integer.MAX_VALUE).receipts().size());
                eq(1L, store.read("alpha", 0, 8, Integer.MAX_VALUE).nextSequence());
            }
        }
    }

    /** A constrained per-page byte budget splits rows the same way as the v1 journal would. */
    static void pagingHonoursByteBudget() throws Exception {
        var receipts = List.of(
                receipt(0, "alpha", 2, "short"),
                receipt(1, "alpha", 2, "a-longer-payload-than-the-first-one"),
                receipt(2, "alpha", 2, "tiny"));
        try (var tmp = new Scratch()) {
            Path legacy = legacyDeadLetterJournal(tmp.root, receipts);
            try (var store = new DeadLetters(legacy, Boundary.NONE, 1_700_000_000_000L)) {
                // Each row's exact wire size decides whether it fits the frame.
                int first = receipts.get(0).wireBytes();
                var one = store.read("alpha", 0, 8, first);
                eq(List.of(receipts.get(0)), one.receipts());
                eq(1L, one.nextSequence());
                // A budget too small for a single row is a capacity error, not a
                // silent skip, matching the pre-migration behaviour.
                try {
                    store.read("alpha", one.nextSequence(), 8, first);
                    throw new AssertionError("expected capacity refusal");
                } catch (BrokerException expected) {
                    eq(BrokerException.Code.CAPACITY, expected.code());
                }
                // A budget large enough drains everything.
                var all = store.read("alpha", 0, 8, Integer.MAX_VALUE);
                eq(receipts, all.receipts());
                eq(3L, all.nextSequence());
            }
        }
    }

    /**
     * A pre-upgrade data directory holding a v1 dead-letter journal and matching
     * group snapshot opens through the broker, serves pages that agree with the
     * old cursors, and completes the migration on disk.
     */
    static void brokerMigrationEndToEnd() throws Exception {
        try (var tmp = new Scratch()) {
            var config = settings(tmp.root);
            var time = new Time();
            // Build a normal v1-era directory by producing and terminating one message.
            List<DeadLetterReceipt> legacyRows = new ArrayList<>();
            try (var server = TcpBroker.start(config, time, Boundary.NONE); var client = connect(server)) {
                BrokerTest.publish(client, "p", 0, bytes("unhandled"));
                long epoch = client.join("g", "m", ORDERS).number();
                client.pull(epoch, 1);
                time.advance(1000);
                client.pull(epoch, 1);
                time.advance(1000);
                eq(List.of(), client.pull(epoch, 1));
                var row = client.deadLetters("g", 0, 8).receipts().getFirst();
                legacyRows.add(new DeadLetterReceipt(row.sequence(), row.group(), row.attempts(),
                        row.terminalAtMs(), row.message()));
            }
            // Replace the v2 pair with an equivalent v1 journal and drop the marker.
            Path data = tmp.root;
            Files.deleteIfExists(data.resolve(DeadLetters.META_FILE));
            Files.deleteIfExists(data.resolve(DeadLetters.DATA_FILE));
            Files.deleteIfExists(data.resolve("deadletter.v2.complete"));
            var file = tidemq.segment.RecordFile.create(data.resolve(DeadLetters.V1_FILE),
                    new tidemq.segment.RecordFile.Header(0x544D444C, 0, 1_700_000_000_000L));
            for (DeadLetterReceipt row : legacyRows) {
                file.append(Binary.encode(out -> DeadLetters.write(out, row)), () -> {});
            }
            try (var server = TcpBroker.start(config, time, Boundary.NONE); var client = connect(server)) {
                var page = client.deadLetters("g", 0, 8);
                eq(legacyRows, page.receipts());
                eq(List.of(), client.deadLetters("g", page.nextSequence(), 8).receipts());
                // The group frontier was already complete before migration.
                eq(1L, client.group("g").frontiers().get(P0).next());
            }
            eq(false, Files.exists(data.resolve(DeadLetters.V1_FILE)));
            eq(true, Files.exists(data.resolve(DeadLetters.META_FILE)));
            eq(true, Files.exists(data.resolve(DeadLetters.DATA_FILE)));
        }
    }
}
