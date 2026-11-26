package tidemq.delivery;

import tidemq.group.GroupEpoch;
import tidemq.segment.Message;
import tidemq.segment.TopicPartition;

import java.util.ArrayList;
import java.util.List;

/**
 * Rerunnable proof that a sweep's deadline-check cost follows the number of due leases, not the
 * size of the whole group book. Run via {@code bash scripts/expiry-proof.sh}.
 *
 * <p>The hard assertions are on exact entry-inspection counts (deterministic); wall-clock columns
 * are supporting evidence and are not used for pass/fail because shared CI machines are noisy.
 */
public final class ExpiryScalingProof {
    private static final TopicPartition TOPIC = new TopicPartition("orders", 0);
    private static final GroupEpoch EPOCH = new GroupEpoch("g", "m", 1, List.of(TOPIC));
    private static final long NOW = 1_700_000_000_000L;
    private static final long FAR = NOW + 600_000L;

    public static void main(String[] args) {
        scalingWithTotalLeases();
        scalingWithDueLeases();
        deadlineReindexAfterAccept();
        staleRecordsAreFenced();
        System.out.println("EXPIRY_SCALING_PROOF PASS");
    }

    private static void scalingWithTotalLeases() {
        System.out.println("# fixed due=8, grow valid leases");
        System.out.println("# total\tdue\tindexed_checks\tfullscan_checks\tindexed_us\tfullscan_us");
        int due = 8;
        for (int total : new int[] {1_000, 10_000, 50_000, 200_000}) {
            LeaseBook book = book(total, due);
            long indexedUs = time(() -> {
                book.resetExpiryInspections();
                List<LeaseBook.Entry> got = book.expired(NOW);
                require(got.size() == due, "indexed expired count");
            });
            long checks = book.expiryInspections();
            long scanUs = time(() -> fullScan(book, NOW));
            long scanned = fullScan(book, NOW);
            require(checks == due, "indexed checks must touch only due entries, got " + checks);
            require(scanned == total + due, "full scan still checks every entry");
            System.out.printf("%d\t%d\t%d\t%d\t%.1f\t%.1f%n",
                    total, due, checks, scanned, indexedUs / 1000d, scanUs / 1000d);
        }
    }

    private static void scalingWithDueLeases() {
        int total = 200_000;
        System.out.println("# fixed total=200000, grow due leases");
        System.out.println("# due\tindexed_checks\tindexed_us");
        for (int due : new int[] {1, 8, 128, 2_048}) {
            LeaseBook book = book(total, due);
            long us = time(() -> {
                book.resetExpiryInspections();
                List<LeaseBook.Entry> got = book.expired(NOW);
                require(got.size() == due, "indexed expired count");
            });
            long checks = book.expiryInspections();
            require(checks == due, "checks must grow with due entries, got " + checks + " for due " + due);
            System.out.printf("%d\t%d\t%.1f%n", due, checks, us / 1000d);
        }
    }

    private static void deadlineReindexAfterAccept() {
        var book = new LeaseBook();
        book.reserve(1, EPOCH, 7L, NOW + 300, 1, message(0));
        LeaseBook.Entry entry = last(book);
        require(book.expired(NOW + 300).size() == 1, "reservation should be due at its deadline");
        // Simulate the engine path: ACCEPT extends the deadline and records the attempt count.
        book.accept(entry, NOW + 1_000, 1);
        require(book.expired(NOW + 300).isEmpty(), "accepted lease must not expire at the old reservation deadline");
        require(book.expired(NOW + 999).isEmpty(), "accepted lease still valid before its deadline");
        require(book.expired(NOW + 1_000).size() == 1, "accepted lease expires at the new deadline");
        System.out.println("PASS accept reindexes the deadline");
    }

    private static void staleRecordsAreFenced() {
        var book = new LeaseBook();
        book.reserve(1, EPOCH, 7L, NOW + 1_000, 1, message(0));
        LeaseBook.Entry first = last(book);
        book.accept(first, NOW + 1_000, 1);
        // ACK settles and removes the entry.
        book.remove(first);
        require(!book.isLive(first), "acked entry must not be live");
        require(!book.has(TOPIC, 0), "ack must release the slot");

        // A later delivery occupies the same slot with a brand new token.
        book.reserve(2, EPOCH, 7L, NOW + 5_000, 2, message(0));
        LeaseBook.Entry second = last(book);
        require(book.has(TOPIC, 0), "redelivery must occupy the slot");
        require(book.count("m") == 1, "member inflight count must be one");

        // A stale expiry record for the ACKed first lease must not evict the new lease.
        book.remove(first);
        require(book.isLive(second), "new lease must stay live after stale remove");
        require(book.has(TOPIC, 0), "stale expiry record must not release the new lease slot");
        require(book.count("m") == 1, "stale remove must not corrupt inflight count");
        require(book.expired(NOW + 1_000).isEmpty(), "old deadline record must not resurface");

        // Generation replacement discards the whole book; an old entry is foreign to the new one.
        var nextGeneration = new LeaseBook();
        require(!nextGeneration.isLive(first), "old generation entry must not be live in a new book");
        nextGeneration.remove(first); // must be a no-op even for a reused slot
        require(!nextGeneration.has(TOPIC, 0), "foreign remove must not create state");
        System.out.println("PASS ack and generation stale records are fenced");
    }

    private static LeaseBook.Entry last(LeaseBook book) {
        List<LeaseBook.Entry> entries = book.entries();
        return entries.get(entries.size() - 1);
    }

    private static LeaseBook book(int valid, int due) {
        var book = new LeaseBook();
        long id = 0;
        for (int i = 0; i < valid; i++) {
            book.reserve(++id, EPOCH, 7L, FAR, 1, message(i));
        }
        for (int i = 0; i < due; i++) {
            // Due offsets live in their own offset range so every Slot stays unique.
            book.reserve(++id, EPOCH, 7L, NOW, 1, message(valid + i));
        }
        return book;
    }

    private static long fullScan(LeaseBook book, long now) {
        long[] checks = {0};
        List<LeaseBook.Entry> due = new ArrayList<>();
        book.forEach(entry -> {
            checks[0]++;
            if (entry.activeUntil() <= now) {
                due.add(entry);
            }
        });
        if (due.size() != dueCount(book, now)) {
            throw new AssertionError("full scan mismatch");
        }
        return checks[0];
    }

    private static int dueCount(LeaseBook book, long now) {
        return book.expired(now).size();
    }

    private static Message message(long offset) {
        return new Message(TOPIC, offset, NOW, "p", offset, new byte[0], new byte[16]);
    }

    private static long time(Runnable action) {
        action.run(); // warmup
        long best = Long.MAX_VALUE;
        for (int repeat = 0; repeat < 5; repeat++) {
            long start = System.nanoTime();
            action.run();
            best = Math.min(best, System.nanoTime() - start);
        }
        return best;
    }

    private static void require(boolean condition, String message) {
        if (!condition) {
            throw new AssertionError(message);
        }
    }
}
