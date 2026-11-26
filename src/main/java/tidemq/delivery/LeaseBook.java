package tidemq.delivery;

import tidemq.BrokerException;
import tidemq.group.GroupEpoch;
import tidemq.segment.Message;
import tidemq.segment.TopicPartition;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;

final class LeaseBook {
    private record Slot(TopicPartition partition, long offset) {}

    static final class Entry {
        private final DeliveryLease.Ref ref;
        private final long epoch;
        private final long session;
        private final String member;
        private final Slot slot;
        private long activeUntil;
        private boolean accepted;
        private int attempts;

        Entry(DeliveryLease lease, long session, String member, Slot slot) {
            this.ref = lease.ref();
            this.epoch = lease.epoch();
            this.session = session;
            this.member = member;
            this.slot = slot;
            this.activeUntil = lease.reserveUntilMs();
            this.attempts = lease.attempt();
        }

        DeliveryLease.Ref ref() {
            return ref;
        }

        long activeUntil() {
            return activeUntil;
        }

        boolean accepted() {
            return accepted;
        }

        /** Persisted delivery count that was fixed when this lease was accepted. */
        int attempts() {
            return attempts;
        }

        String member() {
            return member;
        }

        long epoch() {
            return epoch;
        }

        long session() {
            return session;
        }

        void accept(long expiresAt, int acceptedAttempts) {
            activeUntil = expiresAt;
            accepted = true;
            attempts = acceptedAttempts;
        }

        /** Backward-compatible deadline-only acceptance; keeps the reserved/prospective count. */
        void accept(long expiresAt) {
            activeUntil = expiresAt;
            accepted = true;
        }
    }

    private final Map<Long, Entry> byToken = new LinkedHashMap<>();
    private final Map<Slot, Entry> bySlot = new HashMap<>();
    private final Map<String, Integer> byMember = new HashMap<>();
    private final Map<String, Integer> rotation = new HashMap<>();
    // Deadline-ordered expiry index. Only buckets at or before "now" are touched by a sweep,
    // so the per-cycle inspection cost grows with due entries rather than all in-flight leases.
    private final TreeMap<Long, Set<Entry>> byDeadline = new TreeMap<>();
    private long expiryInspections;

    boolean has(TopicPartition partition, long offset) {
        return bySlot.containsKey(new Slot(partition, offset));
    }

    int count(String member) {
        return byMember.getOrDefault(member, 0);
    }

    int partitionStart(String member) {
        return rotation.getOrDefault(member, 0);
    }

    void partitionStart(String member, int start) {
        rotation.put(member, start);
    }

    DeliveryLease reserve(long id, GroupEpoch epoch, long session, long deadline, int attempt, Message message) {
        var lease = new DeliveryLease(id, epoch.number(), deadline, attempt, message);
        var slot = new Slot(message.partition(), message.offset());
        var entry = new Entry(lease, session, epoch.member(), slot);
        bySlot.put(slot, entry);
        byToken.put(id, entry);
        byMember.merge(epoch.member(), 1, Integer::sum);
        indexDeadline(entry);
        return lease;
    }

    /**
     * Moves a live entry from its reservation deadline to the accepted-lease deadline and records
     * the persisted attempt count. Reindexing here keeps the expiry index exact after ACCEPT.
     */
    void accept(Entry entry, long expiresAt, int acceptedAttempts) {
        unindexDeadline(entry);
        entry.accept(expiresAt, acceptedAttempts);
        indexDeadline(entry);
    }

    Entry require(DeliveryLease.Ref ref, GroupEpoch epoch, long session) throws BrokerException {
        Entry entry = byToken.get(ref.id());
        if (entry == null || entry.session != session || !entry.ref.equals(ref)
                || entry.epoch != epoch.number() || !entry.member.equals(epoch.member())) {
            throw new BrokerException(BrokerException.Code.UNKNOWN_LEASE, "no active delivery for lease " + ref.id());
        }
        return entry;
    }

    /**
     * Returns exactly the live entries whose current deadline has passed. Iteration starts at the
     * first deadline bucket, so valid future leases are never inspected. The extra current-deadline
     * guard also discards an index bucket left stale by a direct deadline mutation.
     */
    List<Entry> expired(long now) {
        var expired = new ArrayList<Entry>();
        for (Set<Entry> bucket : byDeadline.headMap(now, true).values()) {
            for (Entry entry : bucket) {
                expiryInspections++;
                if (entry.activeUntil() <= now && isLive(entry)) {
                    expired.add(entry);
                }
            }
        }
        return expired;
    }

    /** Identity fence: an ACKed or removed entry, or one from a discarded generation book, is not live. */
    boolean isLive(Entry entry) {
        return byToken.get(entry.ref().id()) == entry;
    }

    List<Entry> entries() {
        return List.copyOf(byToken.values());
    }

    void forEach(java.util.function.Consumer<Entry> action) {
        byToken.values().forEach(action);
    }

    /**
     * Conditional, identity-safe removal. A stale expiry record for an already ACKed/replaced lease
     * cannot remove a newer lease that happens to occupy the same partition/offset slot.
     */
    void remove(Entry entry) {
        if (!byToken.remove(entry.ref().id(), entry)) {
            return;
        }
        bySlot.remove(entry.slot, entry);
        unindexDeadline(entry);
        byMember.compute(entry.member, (member, count) -> count == null || count == 1 ? null : count - 1);
    }

    /** Number of entry deadline checks performed by {@link #expired(long)} calls since creation/reset. */
    long expiryInspections() {
        return expiryInspections;
    }

    /** Alias for {@link #expiryInspections()} using the "check" vocabulary. */
    long expiryChecks() {
        return expiryInspections;
    }

    /** Alias for {@link #expiryInspections()} using the "scanned entry" vocabulary. */
    long scannedEntries() {
        return expiryInspections;
    }

    void resetExpiryInspections() {
        expiryInspections = 0;
    }

    private void indexDeadline(Entry entry) {
        byDeadline.computeIfAbsent(entry.activeUntil(), key -> new LinkedHashSet<>()).add(entry);
    }

    private void unindexDeadline(Entry entry) {
        Set<Entry> bucket = byDeadline.get(entry.activeUntil());
        if (bucket == null) {
            return;
        }
        bucket.remove(entry);
        if (bucket.isEmpty()) {
            byDeadline.remove(entry.activeUntil());
        }
    }
}
