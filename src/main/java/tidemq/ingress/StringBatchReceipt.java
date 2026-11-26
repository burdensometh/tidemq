package tidemq.ingress;

import tidemq.segment.TopicPartition;

import java.util.List;
import java.util.Objects;
import java.util.stream.LongStream;

/** Batch receipt whose producer-supplied identifier is textual. */
public record StringBatchReceipt(
        String producer,
        String batchId,
        TopicPartition partition,
        long baseOffset,
        int count,
        long appendedAtMs,
        boolean replayed) {

    public StringBatchReceipt {
        Objects.requireNonNull(producer, "producer");
        Objects.requireNonNull(batchId, "batchId");
        Objects.requireNonNull(partition, "partition");
        if (baseOffset < 0 || count <= 0 || appendedAtMs < 0) {
            throw new IllegalArgumentException("invalid batch receipt range or timestamp");
        }
    }

    public String id() {
        return batchId;
    }

    public long offset() {
        return baseOffset;
    }

    public long firstOffset() {
        return baseOffset;
    }

    public long lastOffset() {
        return baseOffset + count - 1L;
    }

    public int size() {
        return count;
    }

    public List<Long> offsets() {
        return LongStream.range(baseOffset, baseOffset + count).boxed().toList();
    }

    public StringBatchReceipt asReplay() {
        return new StringBatchReceipt(producer, batchId, partition, baseOffset, count, appendedAtMs, true);
    }
}
