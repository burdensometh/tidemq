package tidemq.group;

import tidemq.Boundary;
import tidemq.BrokerException;
import tidemq.Settings;
import tidemq.segment.SegmentStore;
import tidemq.segment.TopicPartition;

import java.io.IOException;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;

public final class GroupDirectory {
    public static final int MAX_GROUPS = 1024;

    public record View(String name, long epoch, Set<String> subscriptions,
                       Map<String, List<TopicPartition>> assignments,
                       Map<TopicPartition, AckFrontier.View> frontiers) {}

    public record RetentionBarrier(long frontier, int groupCount, List<String> firstGroups) {}

    private final SegmentStore segments;
    private final GroupStore store;
    private final int maxMembers;
    private final Map<String, GroupState> groups = new TreeMap<>();

    public GroupDirectory(Settings settings, SegmentStore segments, Boundary boundary) throws IOException {
        this.segments = segments;
        this.maxMembers = settings.maxMembers();
        this.store = new GroupStore(settings.dataDirectory().resolve("groups"), boundary, segments);
        store.load(MAX_GROUPS).forEach(group -> groups.put(group.name(), group));
    }

    public GroupEpoch join(String name, String member, long session, Set<String> subscriptions) throws IOException {
        Settings.requireIdentifier(name, "group");
        Settings.requireIdentifier(member, "member");
        if (subscriptions.isEmpty() || subscriptions.size() > 16) {
            throw new BrokerException(BrokerException.Code.BAD_REQUEST, "a group needs 1..16 topics");
        }
        for (String topic : subscriptions) {
            segments.partitionCount(topic);
        }
        GroupState group = groups.get(name);
        if (group == null) {
            if (groups.size() == MAX_GROUPS) {
                throw new BrokerException(BrokerException.Code.CAPACITY, "durable group limit reached");
            }
            group = createGroup(name, subscriptions);
        } else if (!group.subscriptions().equals(subscriptions)) {
            throw new BrokerException(BrokerException.Code.GROUP_CONFLICT, "group subscriptions are immutable");
        }
        GroupState.Member previous = group.member(member);
        if (previous != null && previous.session() == session) {
            return group.current(member, session);
        }
        if (previous == null && group.memberCount() == maxMembers) {
            throw new BrokerException(BrokerException.Code.CAPACITY, "member limit reached for " + name);
        }
        group.join(member, session);
        store.save(group, true);
        groups.put(name, group);
        return group.current(member, session);
    }

    private GroupState createGroup(String name, Set<String> subscriptions) throws BrokerException {
        var frontiers = new TreeMap<TopicPartition, AckFrontier>();
        for (TopicPartition partition : segments.partitions()) {
            if (subscriptions.contains(partition.topic())) {
                frontiers.put(partition, new AckFrontier(segments.range(partition).retainedFrom()));
            }
        }
        return new GroupState(name, subscriptions, frontiers, 0);
    }

    public boolean disconnect(String group, String member, long session) throws IOException {
        GroupState current = groups.get(group);
        if (current == null || !current.leave(member, session)) {
            return false;
        }
        store.save(current, true);
        return true;
    }

    /**
     * Makes freshly added partitions durable in every group subscribed to the
     * expanded topic and advances the generation of connected groups so their
     * members rebalance onto the new partitions.
     */
    public void expandTopic(String topic, List<TopicPartition> added) throws IOException {
        for (GroupState group : groups.values()) {
            if (!group.subscriptions().contains(topic)) {
                continue;
            }
            boolean generationChange = group.memberCount() > 0;
            group.adoptPartitions(added);
            store.save(group, generationChange);
        }
    }

    public List<String> groupsSubscribedTo(String topic) {
        return groups.values().stream().filter(group -> group.subscriptions().contains(topic))
                .map(GroupState::name).toList();
    }

    public GroupState require(String group) throws BrokerException {
        GroupState state = groups.get(group);
        if (state == null) {
            throw new BrokerException(BrokerException.Code.GROUP_CONFLICT, "unknown group " + group);
        }
        return state;
    }

    public GroupEpoch current(String group, String member, long session) throws BrokerException {
        return require(group).current(member, session);
    }

    public GroupEpoch requireEpoch(String group, String member, long session, long epoch) throws BrokerException {
        GroupEpoch current = current(group, member, session);
        if (current.number() != epoch) {
            throw new BrokerException(BrokerException.Code.STALE_EPOCH, "membership epoch changed to " + current.number());
        }
        return current;
    }

    public void saveProgress(GroupState state) throws IOException {
        store.save(state, false);
    }

    public List<GroupState> states() {
        return List.copyOf(groups.values());
    }

    public View view(String group) throws BrokerException {
        GroupState state = require(group);
        var frontiers = new TreeMap<TopicPartition, AckFrontier.View>();
        state.frontiers().forEach((partition, frontier) -> frontiers.put(partition, frontier.view()));
        return new View(group, state.epoch(), state.subscriptions(), state.assignments(),
                Collections.unmodifiableMap(frontiers));
    }

    public Map<TopicPartition, Long> retentionFrontiers() throws BrokerException {
        var minimums = new TreeMap<TopicPartition, Long>();
        for (TopicPartition partition : segments.partitions()) {
            minimums.put(partition, segments.range(partition).nextOffset());
        }
        for (GroupState group : groups.values()) {
            group.frontiers().forEach((partition, frontier) -> minimums.merge(partition, frontier.next(), Math::min));
        }
        return minimums;
    }

    public RetentionBarrier retentionBarrier(TopicPartition partition) throws BrokerException {
        long minimum = segments.range(partition).nextOffset();
        int groupCount = 0;
        var names = new java.util.ArrayList<String>(8);
        for (GroupState group : groups.values()) {
            AckFrontier frontier = group.frontier(partition);
            if (frontier == null || frontier.next() > minimum) {
                continue;
            }
            if (frontier.next() < minimum) {
                names.clear();
                groupCount = 0;
                minimum = frontier.next();
            }
            groupCount++;
            if (names.size() < 8) {
                names.add(group.name());
            }
        }
        return new RetentionBarrier(minimum, groupCount, List.copyOf(names));
    }
}
