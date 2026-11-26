package tidemq.ingress;

import java.util.List;
import java.util.Objects;

/**
 * Portable producer batch request. This type is a convenience/adapter surface;
 * callers can also use the typed {@code produceBatch} overloads directly.
 */
public class ProducerBatch {
    private final String producer;
    private final Object batchId;
    private final String topic;
    private final int partition;
    private final List<?> items;

    public ProducerBatch(String producer, long batchId, String topic, int partition, List<?> items) {
        this.producer = Objects.requireNonNull(producer, "producer");
        this.batchId = batchId;
        this.topic = Objects.requireNonNull(topic, "topic");
        this.partition = partition;
        this.items = items == null ? List.of() : List.copyOf(items);
    }

    public ProducerBatch(String producer, String batchId, String topic, int partition, List<?> items) {
        this.producer = Objects.requireNonNull(producer, "producer");
        this.batchId = Objects.requireNonNull(batchId, "batchId");
        this.topic = Objects.requireNonNull(topic, "topic");
        this.partition = partition;
        this.items = items == null ? List.of() : List.copyOf(items);
    }

    public String producer() {
        return producer;
    }

    public Object batchId() {
        return batchId;
    }

    public long sequence() {
        return batchId instanceof Number number ? number.longValue() : TideBatchIds.surrogate(batchId.toString());
    }

    public String batchIdentifier() {
        return batchId.toString();
    }

    public String topic() {
        return topic;
    }

    public int partition() {
        return partition;
    }

    public List<?> items() {
        return items;
    }

    public List<?> messages() {
        return items;
    }

    public List<?> records() {
        return items;
    }
}
