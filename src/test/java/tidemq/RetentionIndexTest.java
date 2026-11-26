package tidemq;

import tidemq.group.GroupEpoch;
import tidemq.group.RetentionStats;
import tidemq.delivery.DeliveryLease;
import tidemq.segment.RetentionPreview;
import tidemq.segment.TopicPartition;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static tidemq.TestSupport.eq;
import static tidemq.TestSupport.check;

/**
 * Proofs for the incremental retention index:
 *
 * <ol>
 *   <li>with many durable (offline) groups, advancing one group's watermark makes the next
 *       retention pass touch only the associated partitions, and the work stays constant as the
 *       number of groups grows;</li>
 *   <li>preview and physical deletion still follow the minimum contiguous watermark across every
 *       subscribing group together with segment age;</li>
 *   <li>after restart, and after an offline group re-enters, the cached physical lower bound,
 *       retention preview and pullable offsets all agree.</li>
 * </ol>
 */
public final class RetentionIndexTest {
    private static final Set<String> ORDERS = Set.of("orders");
    private static final Set<String> BOTH = Set.of("orders", "events");
    private static final int ORDER_PARTITIONS = 4;
    private static final int EVENT_PARTITIONS = 2;
    private static final int TOTAL_PARTITIONS = ORDER_PARTITIONS + EVENT_PARTITIONS;
    private static final int GROUPS = 16;
    private static final int MESSAGES_PER_PARTITION = 8;

    record Case(String name, TestSupport.Action body) {}

    public static void main(String[] selected) throws Exception {
        var tests = List.of(
                new Case("retirement-work-follows-changed-partitions", RetentionIndexTest::changeLocality),
                new Case("ungrouped-partition-barrier-follows-live-end", RetentionIndexTest::ungroupedPartition),
                new Case("retention-survives-restart-and-offline-reentry", RetentionIndexTest::restartAndReentry));
        List<String> filters = List.of(selected);
        int passed = 0;
        for (Case test : tests) {
            if (filters.isEmpty() || filters.contains(test.name())) {
                test.body().run();
                System.out.println("PASS " + test.name());
                passed++;
            }
        }
        if (passed == 0) {
            throw new IllegalArgumentException("no matching tests");
        }
        System.out.println("PASS total=" + passed);
    }

    private static void changeLocality() throws Exception {
        try (var tmp = new TestSupport.Scratch()) {
            var time = new TestSupport.Time();
            var config = TestSupport.settings(tmp.root,
                    Map.of("orders", ORDER_PARTITIONS, "events", EVENT_PARTITIONS), 1000);
            try (var broker = Broker.open(config, time, Boundary.NONE)) {
                // A wide fleet of durable groups, most of which stay offline for the whole test.
                for (int g = 0; g < GROUPS; g++) {
                    broker.join(new Broker.Member(groupName(g), "m", session(g)), BOTH);
                }
                for (int p = 0; p < ORDER_PARTITIONS; p++) {
                    for (int i = 0; i < MESSAGES_PER_PARTITION; i++) {
                        broker.produce("producer", (long) p * MESSAGES_PER_PARTITION + i,
                                "orders", p, TestSupport.bytes("k"), TestSupport.bytes("payload"));
                    }
                }
                for (int p = 0; p < EVENT_PARTITIONS; p++) {
                    for (int i = 0; i < MESSAGES_PER_PARTITION; i++) {
                        broker.produce("producer", 10_000L + p * MESSAGES_PER_PARTITION + i,
                                "events", p, TestSupport.bytes("k"), TestSupport.bytes("payload"));
                    }
                }
                // Drain registration dirt once so the idle baseline is clean.
                eq(TOTAL_PARTITIONS, broker.retentionStats().trackedPartitions());
                eq(TOTAL_PARTITIONS, broker.retentionStats().dirtyPartitions());
                eq(0, broker.retire());
                RetentionStats baseline = broker.retentionStats();
                eq(0, baseline.dirtyPartitions());

                // Advance every group in turn. Because all groups start at the same watermark,
                // the per-partition minimum moves on every advance. The bookkeeping for one group
                // touches only that group's own partitions: each watermark step is one frontier
                // read and one minimum evaluation on a single partition, regardless of GROUPS.
                var perGroupReads = new ArrayList<Long>(GROUPS);
                for (int g = 0; g < GROUPS; g++) {
                    String name = groupName(g);
                    long epoch = broker.join(new Broker.Member(name, "m", session(g)), BOTH).number();
                    long frontierReadsBefore = broker.retentionStats().frontierReads();
                    long evaluationsBefore = broker.retentionStats().minimumEvaluations();
                    drainAndAck(broker, name, epoch);
                    RetentionStats after = broker.retentionStats();
                    long reads = after.frontierReads() - frontierReadsBefore;
                    long evaluations = after.minimumEvaluations() - evaluationsBefore;
                    // Every frontier read is paired with one minimum evaluation of one partition.
                    eq(reads, evaluations);
                    perGroupReads.add(reads);
                    // Only partitions whose barrier moved are flagged, and each drain clears them.
                    check(after.dirtyPartitions() <= TOTAL_PARTITIONS, "dirty set must stay within touched partitions");
                    eq(0, broker.retire());
                    eq(0, broker.retentionStats().dirtyPartitions());
                }
                // Each group consumes the same number of offsets, so the index work per group is
                // identical even though the number of co-resident durable groups keeps growing.
                long firstGroupReads = perGroupReads.getFirst();
                check(firstGroupReads > 0, "advancing group must perform index work");
                for (long reads : perGroupReads) {
                    eq(firstGroupReads, reads);
                }
                // Total advance work is group work * group count, never O(groups^2): a later group
                // never re-reads the watermarks of the GROUPS-1 groups that stayed put.
                long readsAfterAllGroups = broker.retentionStats().frontierReads();
                eq(firstGroupReads * GROUPS, readsAfterAllGroups - baseline.frontierReads());

                // A pass with no further movement evaluates no partition and reads no watermark:
                // the cost of an idle retention scan is independent of the durable group count.
                long drainedBefore = broker.retentionStats().partitionsDrained();
                eq(0, broker.retire());
                RetentionStats idle = broker.retentionStats();
                eq(drainedBefore, idle.partitionsDrained());
                eq(readsAfterAllGroups, idle.frontierReads());

                // A subsequent out-of-order acknowledgement that leaves every contiguous
                // watermark in place creates neither a frontier read nor a dirty partition.
                String moving = groupName(0);
                var member = new Broker.Member(moving, "m", session(0));
                long epoch = broker.join(member, BOTH).number();
                produceGapMessages(broker);
                long readsBeforeGap = broker.retentionStats().frontierReads();
                // Drain until all new messages are leased, then accept them.
                List<DeliveryLease> leased = new ArrayList<>();
                for (;;) {
                    var batch = broker.pull(member, epoch, 16, 1024 * 1024);
                    if (batch.isEmpty()) {
                        break;
                    }
                    leased.addAll(batch);
                }
                check(leased.size() >= 2 * TOTAL_PARTITIONS, "expected two gap messages per partition");
                broker.accept(member, epoch, leased.stream().map(DeliveryLease::ref).toList());
                // Per partition, acknowledge only the highest gap offset; the lower one stays an
                // open hole, so no contiguous watermark can advance.
                var byPartition = new java.util.TreeMap<TopicPartition, List<DeliveryLease>>();
                leased.forEach(lease -> byPartition
                        .computeIfAbsent(lease.message().partition(), key -> new ArrayList<>()).add(lease));
                var holeAcks = new ArrayList<DeliveryLease.Ref>();
                for (var entry : byPartition.entrySet()) {
                    var sorted = entry.getValue().stream()
                            .sorted(java.util.Comparator.comparingLong(lease -> lease.message().offset())).toList();
                    holeAcks.add(sorted.getLast().ref());
                }
                broker.ack(member, epoch, holeAcks);
                eq(readsBeforeGap, broker.retentionStats().frontierReads());
                eq(0, broker.retentionStats().dirtyPartitions());
                eq(0, broker.retire());
                assertBarriersMatch(broker);
            }
        }
    }

    private static void produceGapMessages(Broker broker) throws Exception {
        long sequence = 1_000_000L;
        for (int p = 0; p < ORDER_PARTITIONS; p++) {
            for (int i = 0; i < 2; i++) {
                broker.produce("gap-producer", sequence++, "orders", p,
                        TestSupport.bytes("k"), TestSupport.bytes("payload"));
            }
        }
        for (int p = 0; p < EVENT_PARTITIONS; p++) {
            for (int i = 0; i < 2; i++) {
                broker.produce("gap-producer", sequence++, "events", p,
                        TestSupport.bytes("k"), TestSupport.bytes("payload"));
            }
        }
    }

    private static void restartAndReentry() throws Exception {
        try (var tmp = new TestSupport.Scratch()) {
            var time = new TestSupport.Time();
            // Zero minimum age so that once the watermark clears a sealed segment, age never blocks.
            var config = TestSupport.settings(tmp.root, Map.of("orders", 1), 0);
            byte[] payload = new byte[1450];
            TopicPartition p0 = new TopicPartition("orders", 0);
            for (int g = 0; g < GROUPS; g++) {
                try (var broker = Broker.open(config, time, Boundary.NONE)) {
                    broker.join(new Broker.Member(groupName(g), "m", session(g)), ORDERS);
                    if (g == 0) {
                        for (int i = 0; i < 7; i++) {
                            broker.produce("p", i, "orders", 0, TestSupport.bytes("k"), payload);
                        }
                    }
                }
            }
            long slowGroupWatermark;
            long retainedLow;
            try (var broker = Broker.open(config, time, Boundary.NONE)) {
                // Rebuilt index: every durable group still pins the partition at offset 0.
                RetentionStats rebuilt = broker.retentionStats();
                eq(0, rebuilt.dirtyPartitions());
                eq((long) GROUPS, rebuilt.registeredGroups());
                var blocked = broker.previewRetention(p0);
                eq(0L, blocked.allowedEndExclusive());
                eq(GROUPS, blocked.limitingGroupCount());
                eq(0, blocked.eligibleSegments());
                check(blocked.aboveGroupFrontier(), "offline groups must still pin sealed segments");
                eq(0, broker.retire());
                eq(4, broker.partition(p0, 20).segmentCount());

                // Fast group drains; the GROUPS-1 offline groups still hold the barrier at 0.
                long fastEpoch = broker.join(new Broker.Member(groupName(0), "m", session(0)), ORDERS).number();
                drainAndAck(broker, groupName(0), fastEpoch);
                var stillPinned = broker.previewRetention(p0);
                eq(GROUPS - 1, stillPinned.limitingGroupCount());
                eq(0L, stillPinned.resultingLow());
                eq(0, broker.retire());
                slowGroupWatermark = broker.group(groupName(1)).frontiers().get(p0).next();
                eq(0L, slowGroupWatermark);
            }
            // Restart with the fast group ahead and the remaining groups offline.
            try (var broker = Broker.open(config, time, Boundary.NONE)) {
                time.advance(1000);
                // Physical lower bound, preview barrier and pullable start still agree at offset 0.
                eq(0L, broker.partition(p0, 1).range().retainedFrom());
                var blocked = broker.previewRetention(p0);
                eq(0L, blocked.allowedEndExclusive());
                eq(GROUPS - 1, blocked.limitingGroupCount());

                // One offline group re-enters and drains, but the other offline groups remain.
                String reentered = groupName(1);
                long epoch = broker.join(new Broker.Member(reentered, "m", session(1)), ORDERS).number();
                var batch = broker.pull(new Broker.Member(reentered, "m", session(1)), epoch, 16, 1024 * 1024);
                eq(List.of(0L, 1L, 2L, 3L, 4L, 5L, 6L), offsetsOf(batch));
                ack(broker, reentered, epoch, batch);
                var stillBlocked = broker.previewRetention(p0);
                eq(GROUPS - 2, stillBlocked.limitingGroupCount());
                eq(0, broker.retire());
                eq(0L, broker.partition(p0, 1).range().retainedFrom());
            }
            // Drain every remaining group; the cached minimum barrier must then release the prefix.
            try (var broker = Broker.open(config, time, Boundary.NONE)) {
                for (int g = 2; g < GROUPS; g++) {
                    String name = groupName(g);
                    long epoch = broker.join(new Broker.Member(name, "m", session(g)), ORDERS).number();
                    drainAndAck(broker, name, epoch);
                }
                RetentionStats before = broker.retentionStats();
                eq(1, before.dirtyPartitions());
                var ready = broker.previewRetention(p0);
                // Every durable group has now advanced to the partition end; all 16 still
                // subscribe, so all 16 jointly define the barrier at the live end.
                eq(GROUPS, ready.limitingGroupCount());
                eq(7L, ready.allowedEndExclusive());
                check(ready.keepsActive(), "active segment is never retired");
                eq(3, ready.eligibleSegments());
                eq(6L, ready.resultingLow());
                int deleted = broker.retire();
                eq(3, deleted);
                retainedLow = broker.partition(p0, 20).range().retainedFrom();
                eq(6L, retainedLow);
                eq(1, broker.partition(p0, 20).segmentCount());
                assertBarriersMatch(broker);
            }
            // Final restart: physical lower bound, preview and the first pullable offset agree at 6.
            try (var broker = Broker.open(config, time, Boundary.NONE)) {
                eq(6L, broker.partition(p0, 1).range().retainedFrom());
                var after = broker.previewRetention(p0);
                eq(6L, after.resultingLow());
                eq(6L, after.firstKeptBase());
                eq(0, after.eligibleSegments());
                String fresh = "newcomer";
                long epoch = broker.join(new Broker.Member(fresh, "m", 999), ORDERS).number();
                var pullable = broker.pull(new Broker.Member(fresh, "m", 999), epoch, 16, 1024 * 1024);
                eq(List.of(6L), offsetsOf(pullable));
                eq(0, broker.retire());
                eq(6L, broker.partition(p0, 1).range().retainedFrom());
            }
        }
    }

    private static void ungroupedPartition() throws Exception {
        try (var tmp = new TestSupport.Scratch()) {
            var time = new TestSupport.Time();
            // "events" has no subscribing group; its barrier must always track the live end.
            var config = TestSupport.settings(tmp.root,
                    Map.of("orders", 1, "events", 1), 1000);
            byte[] payload = new byte[1450];
            try (var broker = Broker.open(config, time, Boundary.NONE)) {
                broker.join(new Broker.Member("subscribed", "m", 1), Set.of("orders"));
                for (int i = 0; i < 7; i++) {
                    broker.produce("p", i, "orders", 0, TestSupport.bytes("k"), payload);
                    broker.produce("e", i, "events", 0, TestSupport.bytes("k"), payload);
                }
                TopicPartition events = new TopicPartition("events", 0);
                TopicPartition orders = new TopicPartition("orders", 0);

                // Ungrouped partition: zero limiting groups and the live end as the barrier.
                var eventsPreview = broker.previewRetention(events);
                eq(0, eventsPreview.limitingGroupCount());
                eq(7L, eventsPreview.allowedEndExclusive());

                // Grouped partition: the single offline group pins it at offset 0.
                var ordersPreview = broker.previewRetention(orders);
                eq(1, ordersPreview.limitingGroupCount());
                eq(0L, ordersPreview.allowedEndExclusive());

                // Appending to the ungrouped partition moves its barrier immediately, proving the
                // value is read live rather than cached from construction time.
                broker.produce("e", 7, "events", 0, TestSupport.bytes("k"), payload);
                eq(8L, broker.previewRetention(events).allowedEndExclusive());

                // An explicit incremental pass reads no group watermark for the ungrouped
                // partition (its barrier is the live end); nothing is dirty because no group moved.
                long beforeReads = broker.retentionStats().frontierReads();
                time.advance(1000);
                eq(0, broker.retire());
                eq(beforeReads, broker.retentionStats().frontierReads());
                eq(8L, broker.previewRetention(events).allowedEndExclusive());

                // The periodic full age sweep reclaims the ungrouped partition's sealed prefix up
                // to the live end while keeping the active segment, once the minimum age elapses.
                time.advance(1000);
                int sealedBefore = broker.partition(events, 20).segmentCount() - 1;
                check(sealedBefore > 0, "expected sealed segments on the ungrouped partition");
                eq(sealedBefore, broker.sweepRetention());
                check(broker.partition(events, 20).range().retainedFrom() > 0,
                        "ungrouped sealed prefix must be reclaimable by age alone");
                eq(1, broker.partition(events, 20).segmentCount());
            }
        }
    }

    private static void assertBarriersMatch(Broker broker) throws Exception {
        // Recompute every barrier by walking the durable groups directly and compare with the cache.
        var partitions = new ArrayList<TopicPartition>();
        for (var entry : broker.topics().entrySet()) {
            for (int number = 0; number < entry.getValue(); number++) {
                partitions.add(new TopicPartition(entry.getKey(), number));
            }
        }
        for (TopicPartition partition : partitions) {
            long expected = broker.partition(partition, 1).range().nextOffset();
            int expectedGroups = 0;
            for (String name : groupNames()) {
                var frontier = broker.group(name).frontiers().get(partition);
                if (frontier != null) {
                    expectedGroups++;
                    expected = Math.min(expected, frontier.next());
                }
            }
            var barrier = broker.previewRetention(partition);
            eq(expected, barrier.allowedEndExclusive());
            eq(expectedGroups, barrier.limitingGroupCount());
        }
    }

    private static void drainAndAck(Broker broker, String group, long epoch) throws Exception {
        var member = new Broker.Member(group, "m", session(indexOf(group)));
        for (;;) {
            var batch = broker.pull(member, epoch, 16, 1024 * 1024);
            if (batch.isEmpty()) {
                break;
            }
            ack(broker, group, epoch, batch);
        }
    }

    private static void ack(Broker broker, String group, long epoch, List<DeliveryLease> leases) throws Exception {
        var member = new Broker.Member(group, "m", session(indexOf(group)));
        broker.accept(member, epoch, leases.stream().map(DeliveryLease::ref).toList());
        broker.ack(member, epoch, leases.stream().map(DeliveryLease::ref).toList());
    }

    private static List<Long> offsetsOf(List<DeliveryLease> leases) {
        return leases.stream().map(lease -> lease.message().offset()).toList();
    }

    private static List<String> groupNames() {
        var names = new ArrayList<String>(GROUPS);
        for (int g = 0; g < GROUPS; g++) {
            names.add(groupName(g));
        }
        return names;
    }

    private static String groupName(int index) {
        return "g" + String.format("%02d", index);
    }

    private static int indexOf(String name) {
        return Integer.parseInt(name.substring(1));
    }

    private static long session(int groupIndex) {
        return 100L + groupIndex;
    }
}
