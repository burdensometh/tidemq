package tidemq.wire;

import tidemq.BrokerException;
import tidemq.Settings;
import tidemq.ingress.BatchReceipt;
import tidemq.ingress.ProduceReceipt;
import tidemq.segment.MessageV1;

import java.io.IOException;
import java.util.List;
import java.util.Objects;
import java.util.OptionalLong;
import java.util.concurrent.locks.ReentrantLock;

/**
 * A sequential producer that keeps one outstanding identity until its receipt
 * is known. Callers persist the producer id and sequence if they also need to
 * retry across a client process restart.
 */
public final class ResumableProducer implements AutoCloseable {
    public static final class DeliveryUncertain extends IOException {
        private final String producer;
        private final long sequence;

        DeliveryUncertain(String producer, long sequence, Throwable cause) {
            super("receipt unavailable for " + producer + "/" + sequence, cause);
            this.producer = producer;
            this.sequence = sequence;
        }

        public String producer() {
            return producer;
        }

        public long sequence() {
            return sequence;
        }
    }

    private record Pending(long sequence, String topic, int partition, byte[] key, byte[] body) {
        Pending {
            key = key.clone();
            body = body.clone();
        }
    }

    private record PendingBatch(long sequence, String topic, int partition, byte[] key,
                                List<byte[]> payloads) {
        private PendingBatch {
            key = key.clone();
            payloads = payloads.stream().map(byte[]::clone).map(bytes -> (byte[]) bytes).toList();
        }
    }

    private final String producer;
    private final TideClient.Connector connect;
    private final int maxConnectionsPerSend;
    private final ReentrantLock operation = new ReentrantLock();
    private long nextSequence;
    private Pending pending;
    private PendingBatch pendingBatch;
    private TideClient client;
    private boolean pendingBeforeRetry;
    private boolean exhausted;
    private boolean closed;

    public ResumableProducer(String producer, long firstSequence, int maxConnectionsPerSend,
                             TideClient.Connector connect) {
        this.producer = Settings.requireIdentifier(producer, "producer");
        if (firstSequence < 0 || maxConnectionsPerSend < 1 || maxConnectionsPerSend > 8) {
            throw new IllegalArgumentException("invalid initial sequence or connection attempts");
        }
        this.nextSequence = firstSequence;
        this.maxConnectionsPerSend = maxConnectionsPerSend;
        this.connect = Objects.requireNonNull(connect);
    }

    public OptionalLong pendingSequence() {
        operation.lock();
        try {
            if (pending != null) {
                return OptionalLong.of(pending.sequence());
            }
            if (pendingBatch != null) {
                return OptionalLong.of(pendingBatch.sequence());
            }
            return OptionalLong.empty();
        } finally {
            operation.unlock();
        }
    }

    public BatchReceipt sendBatch(String topic, int partition, byte[] key, List<byte[]> payloads)
            throws IOException {
        operation.lock();
        try {
            requireOpen();
            if (pending != null || pendingBatch != null) {
                throw new IllegalStateException("resolve outstanding producer receipt before sending another message");
            }
            if (exhausted) {
                throw new IllegalStateException("producer sequence space exhausted");
            }
            Settings.requireIdentifier(topic, "topic");
            if (payloads == null || payloads.isEmpty() || payloads.size() > 256) {
                throw new IllegalArgumentException("batch must contain 1..256 payloads");
            }
            if (partition < -1 || partition > 63 || key.length > Settings.MAX_KEY_BYTES) {
                throw new IllegalArgumentException("invalid route or key size");
            }
            for (byte[] payload : payloads) {
                if (payload.length > MessageV1.MAX_PAYLOAD) {
                    throw new IllegalArgumentException("invalid payload size");
                }
            }
            pendingBatch = new PendingBatch(nextSequence, topic, partition, key, payloads);
            return flushPendingBatch();
        } finally {
            operation.unlock();
        }
    }

    public BatchReceipt sendBatch(String topic, int partition, List<byte[]> payloads) throws IOException {
        return sendBatch(topic, partition, new byte[0], payloads);
    }

    public BatchReceipt retryPendingBatch() throws IOException {
        operation.lock();
        try {
            requireOpen();
            if (pendingBatch == null) {
                throw new IllegalStateException("no outstanding producer batch receipt");
            }
            return flushPendingBatch();
        } finally {
            operation.unlock();
        }
    }

    public ProduceReceipt send(String topic, int partition, byte[] key, byte[] body)
            throws IOException {
        operation.lock();
        try {
            requireOpen();
            if (pending != null || pendingBatch != null) {
                throw new IllegalStateException("resolve outstanding producer receipt before sending another message");
            }
            if (exhausted) {
                throw new IllegalStateException("producer sequence space exhausted");
            }
            Settings.requireIdentifier(topic, "topic");
            if (partition < -1 || partition > 63 || key.length > Settings.MAX_KEY_BYTES
                    || body.length > MessageV1.MAX_PAYLOAD) {
                throw new IllegalArgumentException("invalid route, key size or payload size");
            }
            pending = new Pending(nextSequence, topic, partition, key, body);
            return flushPending();
        } finally {
            operation.unlock();
        }
    }

    public ProduceReceipt retryPending() throws IOException {
        operation.lock();
        try {
            requireOpen();
            if (pending == null) {
                throw new IllegalStateException("no outstanding producer receipt");
            }
            return flushPending();
        } finally {
            operation.unlock();
        }
    }

    private ProduceReceipt flushPending() throws IOException {
        IOException lastFailure = null;
        boolean uncertain = pendingBeforeRetry;
        for (int attempt = 0; attempt < maxConnectionsPerSend; attempt++) {
            if (client == null) {
                try {
                    client = connect.open();
                } catch (IOException failure) {
                    lastFailure = failure;
                    continue;
                }
            }
            try {
                ProduceReceipt receipt = client.produce(producer, pending.sequence(), pending.topic(),
                        pending.partition(), pending.key(), pending.body());
                if (!producer.equals(receipt.producer()) || pending.sequence() != receipt.sequence()
                        || !pending.topic().equals(receipt.partition().topic())
                        || (pending.partition() >= 0 && pending.partition() != receipt.partition().partition())) {
                    throw new IOException("production receipt disagrees with request identity");
                }
                finish();
                return receipt;
            } catch (BrokerException rejected) {
                if (rejected.code() == BrokerException.Code.STORAGE_FAILURE || uncertain) {
                    pendingBeforeRetry = true;
                    throw new DeliveryUncertain(producer, pending.sequence(), rejected);
                }
                pending = null;
                throw rejected;
            } catch (IOException failure) {
                uncertain = true;
                lastFailure = failure;
                release(failure);
            }
        }
        if (uncertain) {
            pendingBeforeRetry = true;
            throw new DeliveryUncertain(producer, pending.sequence(), lastFailure);
        }
        pending = null;
        throw lastFailure == null ? new IOException("unable to open producer connection") : lastFailure;
    }

    private BatchReceipt flushPendingBatch() throws IOException {
        IOException lastFailure = null;
        boolean uncertain = pendingBeforeRetry;
        for (int attempt = 0; attempt < maxConnectionsPerSend; attempt++) {
            if (client == null) {
                try {
                    client = connect.open();
                } catch (IOException failure) {
                    lastFailure = failure;
                    continue;
                }
            }
            try {
                BatchReceipt receipt = client.produceBatch(producer, pendingBatch.sequence(),
                        pendingBatch.topic(), pendingBatch.partition(), pendingBatch.key(),
                        pendingBatch.payloads());
                if (!producer.equals(receipt.producer())
                        || pendingBatch.sequence() != receipt.batchId()
                        || !pendingBatch.topic().equals(receipt.partition().topic())
                        || (pendingBatch.partition() >= 0
                            && pendingBatch.partition() != receipt.partition().partition())) {
                    throw new IOException("batch receipt disagrees with request identity");
                }
                finishBatch();
                return receipt;
            } catch (BrokerException rejected) {
                if (rejected.code() == BrokerException.Code.STORAGE_FAILURE || uncertain) {
                    pendingBeforeRetry = true;
                    throw new DeliveryUncertain(producer, pendingBatch.sequence(), rejected);
                }
                pendingBatch = null;
                throw rejected;
            } catch (IOException failure) {
                uncertain = true;
                lastFailure = failure;
                release(failure);
            }
        }
        if (uncertain) {
            pendingBeforeRetry = true;
            throw new DeliveryUncertain(producer, pendingBatch.sequence(), lastFailure);
        }
        pendingBatch = null;
        throw lastFailure == null ? new IOException("unable to open producer connection") : lastFailure;
    }

    private void finish() {
        if (pending.sequence() == Long.MAX_VALUE) {
            exhausted = true;
        } else {
            nextSequence++;
        }
        pending = null;
        pendingBeforeRetry = false;
    }

    private void finishBatch() {
        if (pendingBatch.sequence() == Long.MAX_VALUE) {
            exhausted = true;
        } else {
            nextSequence++;
        }
        pendingBatch = null;
        pendingBeforeRetry = false;
    }

    private void release(IOException failure) {
        try {
            client.close();
        } catch (IOException close) {
            failure.addSuppressed(close);
        } finally {
            client = null;
        }
    }

    private void requireOpen() throws IOException {
        if (closed) {
            throw new IOException("producer is closed");
        }
    }

    @Override
    public void close() throws IOException {
        operation.lock();
        try {
            if (!closed) {
                closed = true;
                if (client != null) {
                    client.close();
                    client = null;
                }
            }
        } finally {
            operation.unlock();
        }
    }
}
