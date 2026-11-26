package tidemq.group;

/**
 * Observable counters of the incremental retention index, used by tests to prove that retention
 * work follows changed group/partition associations rather than a full rescan of every group.
 */
public record RetentionStats(long frontierReads, long minimumEvaluations, long barrierEvaluations,
                             long partitionsDrained, long fullSweeps, long registeredGroups,
                             int trackedPartitions, int dirtyPartitions) {
}
