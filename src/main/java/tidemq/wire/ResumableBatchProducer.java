package tidemq.wire;

import tidemq.ingress.BatchReceipt;

import java.io.IOException;
import java.util.List;
import java.util.OptionalLong;

/** Batch-focused spelling of {@link ResumableProducer}'s batch support. */
public final class ResumableBatchProducer implements AutoCloseable {
    private final ResumableProducer delegate;

    public ResumableBatchProducer(String producer, long firstSequence, int maxConnectionsPerSend,
                                  TideClient.Connector connect) {
        this.delegate = new ResumableProducer(producer, firstSequence, maxConnectionsPerSend, connect);
    }

    public BatchReceipt send(String topic, int partition, byte[] key, List<byte[]> payloads)
            throws IOException {
        return delegate.sendBatch(topic, partition, key, payloads);
    }

    public BatchReceipt send(String topic, int partition, List<byte[]> payloads) throws IOException {
        return delegate.sendBatch(topic, partition, payloads);
    }

    public BatchReceipt sendBatch(String topic, int partition, byte[] key, List<byte[]> payloads)
            throws IOException {
        return delegate.sendBatch(topic, partition, key, payloads);
    }

    public BatchReceipt sendBatch(String topic, int partition, List<byte[]> payloads) throws IOException {
        return delegate.sendBatch(topic, partition, payloads);
    }

    public BatchReceipt retryPendingBatch() throws IOException {
        return delegate.retryPendingBatch();
    }

    public OptionalLong pendingSequence() {
        return delegate.pendingSequence();
    }

    @Override
    public void close() throws IOException {
        delegate.close();
    }
}
