package tidemq.group;

import tidemq.BrokerException;
import tidemq.segment.TopicPartition;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.NavigableMap;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;

public final class GroupState {
    record Member(String name, long session) {}

    /** One membership incarnation that owns partitions. Session ids are per TCP connection. */
    public record Owner(String member, long session) {}

    /** Immutable assignment/session snapshot used to derive cooperative handoffs. */
    public record Ownership(long epoch, Map<TopicPartition, Owner> partitions,
                            Map<String, Long> sessions) {
        static final Ownership EMPTY = new Ownership(0L, Map.of(), Map.of());
    }

    private final String name;
    private final Set<String> subscriptions;
    private final NavigableMap<TopicPartition, AckFrontier> frontiers;
    private final NavigableMap<String, Member> members = new TreeMap<>();
    private Map<String, List<TopicPartition>> assignments = Map.of();
    private long epoch;

    GroupState(String name, Set<String> subscriptions, NavigableMap<TopicPartition, AckFrontier> frontiers, long epoch) {
        this.name = name;
        this.subscriptions = Collections.unmodifiableSet(new TreeSet<>(subscriptions));
        this.frontiers = frontiers;
        this.epoch = epoch;
    }

    public String name() {
        return name;
    }

    public long epoch() {
        return epoch;
    }

    Set<String> subscriptions() {
        return subscriptions;
    }

    Map<TopicPartition, AckFrontier> frontiers() {
        return Collections.unmodifiableMap(frontiers);
    }

    public AckFrontier frontier(TopicPartition partition) {
        return frontiers.get(partition);
    }

    public Set<TopicPartition> partitions() {
        return Collections.unmodifiableSet(frontiers.navigableKeySet());
    }

    int memberCount() {
        return members.size();
    }

    Member member(String name) {
        return members.get(name);
    }

    /** Current session of a member name, or {@code -1} when the member is absent. */
    public long sessionOf(String name) {
        Member member = members.get(name);
        return member == null ? -1L : member.session();
    }

    /** Reverse assignment map plus live sessions, captured under the broker transition lock. */
    public Ownership ownership() {
        var owners = new TreeMap<TopicPartition, Owner>();
        assignments.forEach((member, assigned) -> {
            long session = members.get(member).session();
            var owner = new Owner(member, session);
            for (TopicPartition partition : assigned) {
                owners.put(partition, owner);
            }
        });
        var sessions = new TreeMap<String, Long>();
        members.forEach((member, value) -> sessions.put(member, value.session()));
        return new Ownership(epoch, Collections.unmodifiableMap(owners),
                Collections.unmodifiableMap(sessions));
    }

    void join(String name, long session) throws BrokerException {
        advanceEpoch();
        members.put(name, new Member(name, session));
        rebalance();
    }

    boolean leave(String name, long session) throws BrokerException {
        Member member = members.get(name);
        if (member == null || member.session() != session) {
            return false;
        }
        advanceEpoch();
        members.remove(name);
        rebalance();
        return true;
    }

    private void advanceEpoch() throws BrokerException {
        if (epoch == Long.MAX_VALUE) {
            throw new BrokerException(BrokerException.Code.CAPACITY, "group epoch exhausted");
        }
        epoch++;
    }

    private void rebalance() {
        if (members.isEmpty()) {
            assignments = Map.of();
            return;
        }
        List<String> names = new ArrayList<>(members.keySet());
        var byMember = new TreeMap<String, List<TopicPartition>>();
        names.forEach(name -> byMember.put(name, new ArrayList<>()));
        int next = 0;
        for (TopicPartition partition : frontiers.keySet()) {
            byMember.get(names.get(next)).add(partition);
            next = (next + 1) % names.size();
        }
        byMember.replaceAll((name, assigned) -> List.copyOf(assigned));
        assignments = Collections.unmodifiableMap(byMember);
    }

    GroupEpoch current(String name, long session) throws BrokerException {
        Member member = members.get(name);
        if (member == null || member.session() != session) {
            throw new BrokerException(BrokerException.Code.STALE_EPOCH, "membership belongs to another session");
        }
        return new GroupEpoch(this.name, name, epoch, assignments.get(name));
    }

    Map<String, List<TopicPartition>> assignments() {
        return assignments;
    }
}
