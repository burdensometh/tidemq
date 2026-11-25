package tidemq.segment;

import java.util.List;

public record RetentionPreview(TopicPartition partition, long evaluatedAtMs, long minimumAgeMs,
                               long allowedEndExclusive, int limitingGroupCount, List<String> limitingGroups,
                               int eligibleSegments, long eligibleBytes, long resultingLow,
                               long firstKeptBase, long firstKeptEnd, boolean keepsActive,
                               boolean aboveGroupFrontier, long remainingAgeMs) {
    public RetentionPreview {
        limitingGroups = List.copyOf(limitingGroups);
    }
}
