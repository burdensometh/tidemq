package tidemq.group;

import tidemq.segment.TopicPartition;

import java.util.List;

public record GroupEpoch(String group, String member, long number, List<TopicPartition> assignment) {
    public GroupEpoch {
        assignment = List.copyOf(assignment);
    }
}
