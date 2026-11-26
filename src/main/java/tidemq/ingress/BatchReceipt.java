package tidemq.ingress;

import tidemq.segment.TopicPartition;

import java.util.AbstractList;
import java.util.List;
import java.util.Objects;
import java.util.stream.LongStream;

/**
 * Durable, reusable receipt for an ordered producer batch. Offsets form the
 * contiguous half-open range {@code [baseOffset(), baseOffset() + count())}.
 *
 * <p>The receipt is also an immutable list of singular-style receipts, allowing
 * call sites that model a batch as {@code List<ProduceReceipt>} to consume it.
 */
public final class BatchReceipt extends AbstractList<ProduceReceipt> {
    private final String producer;
    private final long batchId;
    private final TopicPartition partition;
    private final long baseOffset;
    private final int count;
    private final long appendedAtMs;
    private final boolean replayed;

    public BatchReceipt(String producer, long batchId, TopicPartition partition, long baseOffset,
                        int count, long appendedAtMs, boolean replayed) {
        this.producer = Objects.requireNonNull(producer, "producer");
        this.partition = Objects.requireNonNull(partition, "partition");
        if (batchId < 0) {
            throw new IllegalArgumentException("batch id must be nonnegative");
        }
        if (baseOffset < 0 || count <= 0 || appendedAtMs < 0) {
            throw new IllegalArgumentException("invalid batch receipt range or timestamp");
        }
        if (baseOffset > Long.MAX_VALUE - count + 1L) {
            throw new IllegalArgumentException("batch offset range overflows");
        }
        this.batchId = batchId;
        this.baseOffset = baseOffset;
        this.count = count;
        this.appendedAtMs = appendedAtMs;
        this.replayed = replayed;
    }

    public String producer() {
        return producer;
    }

    public long batchId() {
        return batchId;
    }

    public TopicPartition partition() {
        return partition;
    }

    public long baseOffset() {
        return baseOffset;
    }

    public int count() {
        return count;
    }

    public long appendedAtMs() {
        return appendedAtMs;
    }

    public boolean replayed() {
        return replayed;
    }

    /** Compatibility name for callers that treat the batch id as a sequence. */
    public long sequence() {
        return batchId;
    }

    public long sequenceId() {
        return batchId;
    }

    public long batchSequence() {
        return batchId;
    }

    public long id() {
        return batchId;
    }

    public long batchKey() {
        return batchId;
    }

    /** Singular-receipt style alias for the first offset. */
    public long offset() {
        return baseOffset;
    }

    public long base() {
        return baseOffset;
    }

    public long startOffset() {
        return baseOffset;
    }

    public long startingOffset() {
        return baseOffset;
    }

    public long offsetBase() {
        return baseOffset;
    }

    public long firstOffset() {
        return baseOffset;
    }

    public long first() {
        return baseOffset;
    }

    public long lastOffset() {
        return baseOffset + count - 1L;
    }

    public long last() {
        return lastOffset();
    }

    @Override
    public int size() {
        return count;
    }

    public int itemCount() {
        return count;
    }

    public long appendedAt() {
        return appendedAtMs;
    }

    public long timestamp() {
        return appendedAtMs;
    }

    public boolean duplicated() {
        return replayed;
    }

    public boolean duplicate() {
        return replayed;
    }

    public boolean isReplayed() {
        return replayed;
    }

    @Override
    public ProduceReceipt get(int index) {
        if (index < 0 || index >= count) {
            throw new IndexOutOfBoundsException("index " + index + " outside batch of " + count);
        }
        return new ProduceReceipt(producer, batchId, partition, baseOffset + index,
                appendedAtMs, replayed);
    }

    public List<Long> offsets() {
        return LongStream.range(baseOffset, baseOffset + count).boxed().toList();
    }

    public List<Long> offsetList() {
        return offsets();
    }

    public long offsetAt(int index) {
        if (index < 0 || index >= count) {
            throw new IndexOutOfBoundsException(index);
        }
        return baseOffset + index;
    }

    public List<ProduceReceipt> receipts() {
        return this;
    }

    public BatchReceipt asReplay() {
        return new BatchReceipt(producer, batchId, partition, baseOffset, count, appendedAtMs, true);
    }

    @Override
    public String toString() {
        return "BatchReceipt["
                + "producer=" + producer
                + ", batchId=" + batchId
                + ", partition=" + partition
                + ", baseOffset=" + baseOffset
                + ", count=" + count
                + ", appendedAtMs=" + appendedAtMs
                + ", replayed=" + replayed + ']';
    }
}
