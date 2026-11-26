package proof;

import tidemq.Boundary;
import tidemq.Settings;
import tidemq.wire.TcpBroker;
import tidemq.wire.TideClient;

import java.net.InetAddress;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Clock;
import java.time.Instant;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Self-contained, rerunnable TCP proof of the expiry/fencing invariants:
 * <ul>
 *   <li>reserved expiry only releases the slot (attempt stays at 1, no dead letter);</li>
 *   <li>accepted expiry retries or terminates by the original persisted attempt count;</li>
 *   <li>an ACKed lease's late expiry record never dead-letters;</li>
 *   <li>a stale expiry record from a replaced generation never evicts a new lease or duplicates DLQ.</li>
 * </ul>
 * Run via {@code bash scripts/expiry-proof.sh}.
 */
public final class ExpiryInvariantProof {
    private static final Set<String> ORDERS = Set.of("orders");

    public static void main(String[] args) throws Exception {
        reservedExpiryOnlyReleasesSlot();
        reservedExpiryReleasesInflightCapacity();
        acceptedExpiryRetriesThenTerminatesByOriginalCount();
        onlyTheDueLeaseExpiresAmongStaggeredDeadlines();
        ackAtMaxAttemptsNeverDeadLetters();
        staleGenerationRecordDoesNotKillNewLease();
        staleGenerationRecordDoesNotDuplicateDeadLetter();
        System.out.println("EXPIRY_INVARIANT_PROOF PASS");
    }

    private static void reservedExpiryOnlyReleasesSlot() throws Exception {
        try (Harness h = Harness.open()) {
            h.produce("reserved");
            long epoch = h.client.join("g1", "m", ORDERS).number();
            var first = h.client.reserve(epoch, 1); // raw PULL: reserved, not accepted
            eq(1, first.getFirst().attempt(), "first reservation attempt");
            eq(1, h.client.flow("g1").reserved(), "flow reserved");
            h.time.advance(301); // reservation timeout = 300ms
            var again = h.client.reserve(epoch, 1);
            eq(List.of(0L), leaseOffsets(again), "same offset after reserve expiry");
            eq(1, again.getFirst().attempt(), "reservation expiry must not consume an attempt");
            eq(0, h.client.group("g1").frontiers().get(Harness.P0).retrying(),
                    "reservation expiry must not record a retry attempt");
            var refs = again.stream().map(tidemq.delivery.DeliveryLease::ref).toList();
            h.client.accept(epoch, refs);
            eq(1L, h.client.ack(epoch, refs).get(Harness.P0), "ack after accept");
            eq(0, h.client.deadLetters("g1", 0, 8).receipts().size(), "no dead letter for reserved expiry");
        }
        System.out.println("PASS reserved expiry only releases the slot");
    }

    private static void reservedExpiryReleasesInflightCapacity() throws Exception {
        try (Harness h = Harness.open()) {
            for (int i = 0; i < 40; i++) {
                h.produce(i, "m" + i);
            }
            long epoch = h.client.join("g-cap", "m", ORDERS).number();
            // maxInflight=32, maxBatch=16 in the proof settings: fill all slots with reservations.
            eq(16, h.client.reserve(epoch, 16).size(), "first reservation batch");
            eq(16, h.client.reserve(epoch, 16).size(), "second reservation batch");
            eq(List.of(), h.client.reserve(epoch, 16), "no capacity once 32 reserved");
            eq(32, h.client.flow("g-cap").reserved(), "flow counts all reservations");
            h.time.advance(301); // every reservation deadline passes
            var fresh = h.client.reserve(epoch, 16);
            eq(16, fresh.size(), "reserved expiry must release inflight slots");
            check(fresh.stream().allMatch(l -> l.attempt() == 1), "released reservations reuse attempt 1");
        }
        System.out.println("PASS reserved expiry releases inflight capacity");
    }

    private static void onlyTheDueLeaseExpiresAmongStaggeredDeadlines() throws Exception {
        try (Harness h = Harness.open()) {
            for (int i = 0; i < 3; i++) {
                h.produce(i, "m" + i);
            }
            long epoch = h.client.join("g-stagger", "m", ORDERS).number();
            var r0 = h.client.pull(epoch, 1); // deadline t+1000
            h.time.advance(100);
            var r1 = h.client.pull(epoch, 1); // deadline t+1100
            h.time.advance(100);
            var r2 = h.client.pull(epoch, 1); // deadline t+1200
            eq(List.of(0L), receivedOffsets(r0), "r0 offset");
            eq(List.of(1L), receivedOffsets(r1), "r1 offset");
            eq(List.of(2L), receivedOffsets(r2), "r2 offset");
            h.time.advance(850); // t=1050: only r0 (deadline 1000) is due
            // ACKing r1/r2 sweeps r0 first, then settles the still-valid later leases.
            eq(0L, ack(h.client, epoch, List.of(r1.getFirst(), r2.getFirst())).get(Harness.P0),
                    "frontier still blocked by offset 0");
            var redelivered = h.client.pull(epoch, 8);
            eq(List.of(0L), receivedOffsets(redelivered), "only the due lease is redelivered");
            eq(2, redelivered.getFirst().lease().attempt(), "redelivered after one accepted attempt");
            eq(3L, ack(h.client, epoch, redelivered).get(Harness.P0), "all settled");
            eq(0, h.client.deadLetters("g-stagger", 0, 8).receipts().size(), "no dead letter");
        }
        System.out.println("PASS only the due lease expires among staggered deadlines");
    }

    private static void acceptedExpiryRetriesThenTerminatesByOriginalCount() throws Exception {
        try (Harness h = Harness.open()) {
            h.produce("retry");
            long epoch = h.client.join("g2", "m", ORDERS).number();
            var first = h.client.pull(epoch, 1); // auto accept -> attempts 1
            eq(1, first.getFirst().lease().attempt(), "attempt one");
            h.time.advance(1000); // lease timeout
            var second = h.client.pull(epoch, 1); // expire -> redeliver, accept -> attempts 2 (max)
            eq(2, second.getFirst().lease().attempt(), "attempt two");
            h.time.advance(1000); // expire exhausted accepted lease -> terminal
            eq(List.of(), h.client.pull(epoch, 1), "no redelivery after max attempts");
            var page = h.client.deadLetters("g2", 0, 8);
            eq(1, page.receipts().size(), "one dead letter");
            eq(2, page.receipts().getFirst().attempts(), "dead letter records original count 2");
            eq(1L, h.client.group("g2").frontiers().get(Harness.P0).next(), "frontier advances");
            h.time.advance(60_000);
            eq(List.of(), h.client.pull(epoch, 8), "nothing after terminal");
            eq(1, h.client.deadLetters("g2", 0, 8).receipts().size(), "still exactly one dead letter");
        }
        System.out.println("PASS accepted expiry retries then terminates by original count");
    }

    private static void ackAtMaxAttemptsNeverDeadLetters() throws Exception {
        try (Harness h = Harness.open()) {
            h.produce("acked-max");
            long epoch = h.client.join("g3", "m", ORDERS).number();
            h.client.pull(epoch, 1);
            h.time.advance(1000);
            var maxAttempt = h.client.pull(epoch, 1); // attempts becomes 2 = max
            eq(2, maxAttempt.getFirst().lease().attempt(), "reached max attempt");
            // ACK wins before the second lease expires.
            eq(1L, ack(h.client, epoch, maxAttempt).get(Harness.P0), "ack max-attempt lease");
            h.time.advance(60_000); // every old deadline record is now due
            eq(List.of(), h.client.pull(epoch, 8), "no redelivery after ack");
            eq(0, h.client.deadLetters("g3", 0, 8).receipts().size(),
                    "late expiry record after ACK must not write a dead letter");
            eq(1L, h.client.group("g3").frontiers().get(Harness.P0).next(), "acked frontier");
        }
        System.out.println("PASS ACKed max-attempt lease is never dead-lettered by a stale expiry record");
    }

    private static void staleGenerationRecordDoesNotKillNewLease() throws Exception {
        try (Harness h = Harness.open()) {
            h.produce("generation");
            long e1 = h.client.join("g4", "m", ORDERS).number();
            var first = h.client.pull(e1, 1); // attempts 1, deadline t+1000
            eq(1, first.getFirst().lease().attempt(), "first attempt");
            h.time.advance(500);
            try (TideClient replacement = TideClient.connect(
                    InetAddress.getLoopbackAddress().getHostAddress(), h.server.port())) {
                long e2 = replacement.join("g4", "m", ORDERS).number(); // fences e1
                check(e2 > e1, "epoch must advance on same-member replacement");
                var second = replacement.pull(e2, 1); // same offset, attempts 2, deadline t=500+1000
                eq(List.of(0L), receivedOffsets(second), "new generation redelivers same offset");
                h.time.advance(700); // t=1200: past OLD deadline (1000), before NEW deadline (1500)
                eq(List.of(), replacement.pull(e2, 8), "sweep must not expire the live new lease");
                eq(1L, ack(replacement, e2, second).get(Harness.P0),
                        "new lease must still be ackable after the old deadline passed");
                eq(0, replacement.deadLetters("g4", 0, 8).receipts().size(), "no dead letter");
            }
        }
        System.out.println("PASS stale generation expiry record does not evict a new lease");
    }

    private static void staleGenerationRecordDoesNotDuplicateDeadLetter() throws Exception {
        try (Harness h = Harness.open()) {
            h.produce("dlq-gen");
            long e1 = h.client.join("g5", "m", ORDERS).number();
            h.client.pull(e1, 1);
            h.time.advance(1000);
            var exhausted = h.client.pull(e1, 1); // attempts 2 = max
            eq(2, exhausted.getFirst().lease().attempt(), "exhausted attempt");
            h.time.advance(100);
            try (TideClient replacement = TideClient.connect(
                    InetAddress.getLoopbackAddress().getHostAddress(), h.server.port())) {
                long e2 = replacement.join("g5", "m", ORDERS).number(); // invalidate terminalizes it
                check(e2 > e1, "epoch advances");
                h.time.advance(60_000); // all stale records due
                eq(List.of(), replacement.pull(e2, 8), "terminalized offset not redelivered");
                var page = replacement.deadLetters("g5", 0, 8);
                eq(1, page.receipts().size(), "exactly one dead letter across generations");
                eq(1L, replacement.group("g5").frontiers().get(Harness.P0).next(), "frontier advanced");
            }
        }
        System.out.println("PASS generation replacement yields at most one dead letter");
    }

    private static List<Long> leaseOffsets(List<tidemq.delivery.DeliveryLease> leases) {
        return leases.stream().map(l -> l.message().offset()).toList();
    }

    private static List<Long> receivedOffsets(List<TideClient.Received> received) {
        return received.stream().map(r -> r.lease().message().offset()).toList();
    }

    private static Map<tidemq.segment.TopicPartition, Long> ack(
            TideClient client, long epoch, List<TideClient.Received> data) throws Exception {
        var refs = data.stream().map(r -> r.lease().ref()).toList();
        return client.ack(epoch, refs);
    }

    private static void eq(Object expected, Object actual, String what) {
        if (!Objects.equals(expected, actual)) {
            throw new AssertionError(what + ": expected " + expected + ", got " + actual);
        }
    }

    private static void check(boolean value, String what) {
        if (!value) {
            throw new AssertionError(what);
        }
    }

    static final class MutableClock extends Clock {
        private final AtomicLong now = new AtomicLong(1_700_000_000_000L);

        void advance(long ms) {
            now.addAndGet(ms);
        }

        @Override
        public ZoneId getZone() {
            return ZoneOffset.UTC;
        }

        @Override
        public Clock withZone(ZoneId zone) {
            return this;
        }

        @Override
        public Instant instant() {
            return Instant.ofEpochMilli(now.get());
        }

        @Override
        public long millis() {
            return now.get();
        }
    }

    static final class Harness implements AutoCloseable {
        static final tidemq.segment.TopicPartition P0 = new tidemq.segment.TopicPartition("orders", 0);

        final Path root;
        final MutableClock time;
        final TcpBroker server;
        final TideClient client;

        private Harness(Path root, MutableClock time, TcpBroker server, TideClient client) {
            this.root = root;
            this.time = time;
            this.server = server;
            this.client = client;
        }

        static Harness open() throws Exception {
            Path root = Files.createTempDirectory("tidemq-proof-");
            var time = new MutableClock();
            var settings = new Settings(InetAddress.getLoopbackAddress(), 0, root,
                    Map.of("orders", 1), 4096, 2048, 64 * 1024, 64, 10_000, 16,
                    16, 32, 128, 1000, 300, 2, 1000, 60_000);
            TcpBroker server = TcpBroker.start(settings, time, Boundary.NONE);
            TideClient client = TideClient.connect(InetAddress.getLoopbackAddress().getHostAddress(), server.port());
            return new Harness(root, time, server, client);
        }

        void produce(String payload) throws Exception {
            produce(0, payload);
        }

        void produce(long sequence, String payload) throws Exception {
            client.produce("p", sequence, "orders", 0,
                    "order".getBytes(StandardCharsets.UTF_8), payload.getBytes(StandardCharsets.UTF_8));
        }

        @Override
        public void close() throws Exception {
            client.close();
            server.close();
            try (var tree = Files.walk(root)) {
                tree.sorted(Comparator.reverseOrder()).forEach(path -> {
                    try {
                        Files.deleteIfExists(path);
                    } catch (Exception ignored) {
                        // best effort cleanup
                    }
                });
            }
        }
    }
}
