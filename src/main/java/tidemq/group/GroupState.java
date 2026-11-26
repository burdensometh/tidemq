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

    /**
     * Adopts partitions added by an online expansion. Every new partition starts
     * at watermark zero; existing frontiers are untouched. When the group has
     * connected members the generation advances and assignments rebalance so the
     * added partitions become claimable. Offline groups still record the
     * frontiers durably and rebalance on their next join.
     */
    void adoptPartitions(List<TopicPartition> added) throws BrokerException {
        boolean changed = false;
        for (TopicPartition partition : added) {
            if (frontiers.putIfAbsent(partition, new AckFrontier(0)) == null) {
                changed = true;
            }
        }
        if (!changed) {
            return;
        }
        if (!members.isEmpty()) {
            advanceEpoch();
        }
        rebalance();
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
