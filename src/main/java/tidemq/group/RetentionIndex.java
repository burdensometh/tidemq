package tidemq.group;

import tidemq.BrokerException;
import tidemq.segment.SegmentStore;
import tidemq.segment.TopicPartition;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

/**
 * Incremental index of the per-partition retention barrier.
 *
 * <p>The barrier of a partition is the minimum contiguous watermark across every durable group
 * subscribed to it; partitions without a subscribing group use the live partition end. The index
 * caches one entry per partition and only revisits the partitions linked to a group creation or
 * watermark advance, so a single group's progress costs work proportional to the partitions it
 * touches rather than a sweep of every durable group. Restart rebuilds the same state from the
 * persisted group snapshots.
 */
final class RetentionIndex {
    private static final class PartitionState {
        private final TreeMap<String, Long> watermarks = new TreeMap<>();
        private long minimum;
        private boolean dirty;
    }

    private final SegmentStore segments;
    private final TreeMap<TopicPartition, PartitionState> partitions = new TreeMap<>();
    private long frontierReads;
    private long minimumEvaluations;
    private long barrierEvaluations;
    private long partitionsDrained;
    private long fullSweeps;
    private long registeredGroups;

    RetentionIndex(SegmentStore segments) throws BrokerException {
        this.segments = segments;
        for (TopicPartition partition : segments.partitions()) {
            PartitionState state = new PartitionState();
            state.minimum = segments.range(partition).nextOffset();
            partitions.put(partition, state);
        }
    }

    /** Rebuilds cached watermarks from the durable groups loaded during broker startup. */
    void rebuild(List<GroupState> groups) throws BrokerException {
        for (GroupState group : groups) {
            register(group);
        }
        clearDirty();
    }

    /** Registers a newly created or re-entered durable group on every partition it subscribes to. */
    void register(GroupState group) throws BrokerException {
        registeredGroups++;
        for (Map.Entry<TopicPartition, AckFrontier> entry : group.frontiers().entrySet()) {
            TopicPartition partition = entry.getKey();
            PartitionState state = require(partition);
            state.watermarks.put(group.name(), entry.getValue().next());
            frontierReads++;
            refreshMinimum(partition, state);
            state.dirty = true;
        }
    }

    /**
     * Applies a contiguous watermark advance for one group on one partition.
     * Out-of-order completions that leave the contiguous watermark unchanged are not reported.
     */
    void advance(GroupState group, TopicPartition partition, long watermark) throws BrokerException {
        PartitionState state = require(partition);
        Long previous = state.watermarks.put(group.name(), watermark);
        if (previous == null) {
            throw new IllegalStateException("group " + group.name() + " was never registered on " + partition);
        }
        frontierReads++;
        long oldMinimum = state.minimum;
        refreshMinimum(partition, state);
        if (state.minimum != oldMinimum) {
            state.dirty = true;
        }
    }

    /** Frontiers of the partitions whose cached barrier moved since the last drain. */
    Map<TopicPartition, Long> drainDirty() {
        var changed = new TreeMap<TopicPartition, Long>();
        for (Map.Entry<TopicPartition, PartitionState> entry : partitions.entrySet()) {
            PartitionState state = entry.getValue();
            if (state.dirty) {
                state.dirty = false;
                changed.put(entry.getKey(), state.minimum);
                partitionsDrained++;
            }
        }
        return changed;
    }

    /** Cached frontiers of every partition; the periodic age sweep never walks the groups. */
    Map<TopicPartition, Long> allFrontiers() throws BrokerException {
        fullSweeps++;
        var frontiers = new TreeMap<TopicPartition, Long>();
        for (Map.Entry<TopicPartition, PartitionState> entry : partitions.entrySet()) {
            TopicPartition partition = entry.getKey();
            PartitionState state = entry.getValue();
            frontiers.put(partition, state.watermarks.isEmpty()
                    ? segments.range(partition).nextOffset() : state.minimum);
            partitionsDrained++;
        }
        clearDirty();
        return frontiers;
    }

    GroupDirectory.RetentionBarrier barrier(TopicPartition partition, int sampleLimit)
            throws BrokerException {
        PartitionState state = require(partition);
        barrierEvaluations++;
        // No subscribing group: the live partition end is the barrier, evaluated on demand.
        if (state.watermarks.isEmpty()) {
            return new GroupDirectory.RetentionBarrier(segments.range(partition).nextOffset(), 0, List.of());
        }
        long minimum = Long.MAX_VALUE;
        int groupCount = 0;
        var names = new ArrayList<String>(Math.min(sampleLimit, state.watermarks.size()));
        for (Map.Entry<String, Long> entry : state.watermarks.entrySet()) {
            long watermark = entry.getValue();
            if (watermark > minimum) {
                continue;
            }
            if (watermark < minimum) {
                minimum = watermark;
                groupCount = 0;
                names.clear();
            }
            groupCount++;
            if (names.size() < sampleLimit) {
                names.add(entry.getKey());
            }
        }
        return new GroupDirectory.RetentionBarrier(minimum, groupCount, List.copyOf(names));
    }

    int dirtyCount() {
        int count = 0;
        for (PartitionState state : partitions.values()) {
            if (state.dirty) {
                count++;
            }
        }
        return count;
    }

    RetentionStats stats() {
        return new RetentionStats(frontierReads, minimumEvaluations, barrierEvaluations,
                partitionsDrained, fullSweeps, registeredGroups, partitions.size(), dirtyCount());
    }

    private PartitionState require(TopicPartition partition) {
        PartitionState state = partitions.get(partition);
        if (state == null) {
            throw new IllegalStateException("unknown partition " + partition);
        }
        return state;
    }

    private void refreshMinimum(TopicPartition partition, PartitionState state) throws BrokerException {
        minimumEvaluations++;
        state.minimum = state.watermarks.isEmpty()
                ? segments.range(partition).nextOffset()
                : Collections.min(state.watermarks.values());
    }

    private void clearDirty() {
        for (PartitionState state : partitions.values()) {
            state.dirty = false;
        }
    }
}
