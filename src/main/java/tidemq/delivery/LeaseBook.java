package tidemq.delivery;

import tidemq.BrokerException;
import tidemq.group.GroupEpoch;
import tidemq.segment.Message;
import tidemq.segment.TopicPartition;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

final class LeaseBook {
    private record Slot(TopicPartition partition, long offset) {}

    /**
     * A partition whose ownership moved while the prior owner's session is still
     * alive. The destination must not pull it until the source session ends or
     * hands the partition back.
     */
    record Handoff(TopicPartition partition, String member, long session) {}

    static final class Entry {
        private final DeliveryLease.Ref ref;
        private final long epoch;
        private final long session;
        private final String member;
        private final Slot slot;
        private long activeUntil;
        private boolean accepted;

        Entry(DeliveryLease lease, long session, String member, Slot slot) {
            this.ref = lease.ref();
            this.epoch = lease.epoch();
            this.session = session;
            this.member = member;
            this.slot = slot;
            this.activeUntil = lease.reserveUntilMs();
        }

        DeliveryLease.Ref ref() {
            return ref;
        }

        long activeUntil() {
            return activeUntil;
        }

        long session() {
            return session;
        }

        long epoch() {
            return epoch;
        }

        boolean accepted() {
            return accepted;
        }

        String member() {
            return member;
        }

        void accept(long expiresAt) {
            activeUntil = expiresAt;
            accepted = true;
        }
    }

    private final Map<Long, Entry> byToken = new LinkedHashMap<>();
    private final Map<Slot, Entry> bySlot = new HashMap<>();
    private final Map<String, Integer> byMember = new HashMap<>();
    private final Map<String, Integer> rotation = new HashMap<>();
    private final Map<TopicPartition, Handoff> handoffs = new HashMap<>();

    boolean has(TopicPartition partition, long offset) {
        return bySlot.containsKey(new Slot(partition, offset));
    }

    Handoff handoff(TopicPartition partition) {
        return handoffs.get(partition);
    }

    java.util.Collection<Handoff> handoffs() {
        return handoffs.values();
    }

    void blockHandoff(TopicPartition partition, String member, long session) {
        handoffs.put(partition, new Handoff(partition, member, session));
    }

    void unblockHandoff(TopicPartition partition) {
        handoffs.remove(partition);
    }

    /** Drops every handoff sourced by the ended session; returns the removed blocks. */
    List<Handoff> unblockSession(long session) {
        var removed = new ArrayList<Handoff>();
        var iterator = handoffs.values().iterator();
        while (iterator.hasNext()) {
            Handoff handoff = iterator.next();
            if (handoff.session() == session) {
                removed.add(handoff);
                iterator.remove();
            }
        }
        return removed;
    }

    Entry token(long id) {
        return byToken.get(id);
    }

    List<Entry> entriesForSession(long session) {
        var result = new ArrayList<Entry>();
        for (Entry entry : byToken.values()) {
            if (entry.session == session) {
                result.add(entry);
            }
        }
        return result;
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
        return lease;
    }

    Entry require(DeliveryLease.Ref ref, GroupEpoch epoch, long session) throws BrokerException {
        Entry entry = byToken.get(ref.id());
        if (entry == null || entry.session != session || !entry.ref.equals(ref)
                || entry.epoch != epoch.number() || !entry.member.equals(epoch.member())) {
            throw new BrokerException(BrokerException.Code.UNKNOWN_LEASE, "no active delivery for lease " + ref.id());
        }
        return entry;
    }

    List<Entry> expired(long now) {
        var expired = new ArrayList<Entry>();
        for (Entry entry : byToken.values()) {
            if (entry.activeUntil() <= now) {
                expired.add(entry);
            }
        }
        return expired;
    }

    List<Entry> entries() {
        return List.copyOf(byToken.values());
    }

    void forEach(java.util.function.Consumer<Entry> action) {
        byToken.values().forEach(action);
    }

    void remove(Entry entry) {
        byToken.remove(entry.ref().id());
        bySlot.remove(entry.slot);
        byMember.compute(entry.member, (member, count) -> count == 1 ? null : count - 1);
    }
}
