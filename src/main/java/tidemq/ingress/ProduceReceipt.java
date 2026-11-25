package tidemq.ingress;

import tidemq.segment.TopicPartition;

public record ProduceReceipt(
        String producer, long sequence, TopicPartition partition, long offset, long appendedAtMs,
        boolean replayed) {
    public ProduceReceipt asReplay() {
        return new ProduceReceipt(producer, sequence, partition, offset, appendedAtMs, true);
    }
}
