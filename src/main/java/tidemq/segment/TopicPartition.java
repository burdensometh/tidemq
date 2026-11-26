package tidemq.segment;

import tidemq.Settings;

public record TopicPartition(String topic, int partition) implements Comparable<TopicPartition> {
    public TopicPartition {
        Settings.requireIdentifier(topic, "topic");
        if (partition < 0 || partition >= Settings.MAX_PARTITIONS) {
            throw new IllegalArgumentException("invalid partition " + partition);
        }
    }

    @Override
    public int compareTo(TopicPartition other) {
        int byTopic = topic.compareTo(other.topic);
        return byTopic == 0 ? Integer.compare(partition, other.partition) : byTopic;
    }

    @Override
    public String toString() {
        return topic + "/" + partition;
    }
}
