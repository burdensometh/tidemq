package tidemq.wire;

import tidemq.BrokerException;
import tidemq.Settings;
import tidemq.delivery.DeliveryLease;
import tidemq.group.GroupEpoch;
import tidemq.segment.TopicPartition;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.locks.ReentrantLock;

/**
 * Keeps a consumer's stable identity across socket reconnects. A batch belongs
 * to the socket incarnation and group epoch that received it. Settlement
 * failures remain visible to the application.
 */
public final class GroupConsumer implements AutoCloseable {
    public static final class Batch {
        private final GroupConsumer owner;
        private final long incarnation;
        private final long epoch;
        private final List<TideClient.Received> messages;

        private Batch(GroupConsumer owner, long incarnation, long epoch, List<TideClient.Received> messages) {
            this.owner = owner;
            this.incarnation = incarnation;
            this.epoch = epoch;
            this.messages = List.copyOf(messages);
        }

        public long epoch() {
            return epoch;
        }

        public List<TideClient.Received> messages() {
            return messages;
        }

        public boolean isEmpty() {
            return messages.isEmpty();
        }
    }

    private final String group;
    private final String member;
    private final Set<String> topics;
    private final TideClient.Connector connect;
    private final ReentrantLock operation = new ReentrantLock();
    private TideClient client;
    private GroupEpoch assignment;
    private long incarnation;
    private boolean closed;

    public GroupConsumer(String group, String member, Set<String> topics, TideClient.Connector connect) {
        this.group = Settings.requireIdentifier(group, "group");
        this.member = Settings.requireIdentifier(member, "member");
        this.topics = Set.copyOf(topics);
        if (this.topics.isEmpty() || this.topics.size() > WireValues.MAX_TOPICS) {
            throw new IllegalArgumentException("invalid number of subscriptions");
        }
        this.topics.forEach(topic -> Settings.requireIdentifier(topic, "topic"));
        this.connect = Objects.requireNonNull(connect);
    }

    public GroupEpoch assignment() throws IOException {
        operation.lock();
        try {
            ensureJoined();
            assignment = client.assignment();
            return assignment;
        } catch (BrokerException requested) {
            throw requested;
        } catch (IOException transport) {
            if (client != null) {
                drop(transport);
            }
            throw transport;
        } finally {
            operation.unlock();
        }
    }

    public Batch poll(int limit) throws IOException {
        operation.lock();
        try {
            ensureJoined();
            int capped = Math.min(limit, client.maxBatch());
            try {
                return pollAssigned(capped);
            } catch (BrokerException changed) {
                if (changed.code() != BrokerException.Code.STALE_EPOCH) {
                    throw changed;
                }
                assignment = client.assignment();
                return pollAssigned(capped);
            }
        } catch (BrokerException requested) {
            throw requested;
        } catch (IOException transport) {
            if (client != null) {
                drop(transport);
            }
            throw transport;
        } finally {
            operation.unlock();
        }
    }

    private Batch pollAssigned(int limit) throws IOException {
        var deliveries = client.pull(assignment.number(), limit);
        return new Batch(this, incarnation, assignment.number(), deliveries);
    }

    public Map<TopicPartition, Long> acknowledge(Batch batch) throws IOException {
        return acknowledge(batch, batch.messages());
    }

    public Map<TopicPartition, Long> acknowledge(Batch batch, List<TideClient.Received> processed)
            throws IOException {
        if (batch.owner != this || !batch.messages().containsAll(processed)) {
            throw new IllegalArgumentException("deliveries do not belong to this consumer batch");
        }
        operation.lock();
        try {
            if (closed || client == null || batch.incarnation != incarnation) {
                throw new BrokerException(BrokerException.Code.STALE_EPOCH,
                        "batch belongs to an earlier connection or epoch");
            }
            if (processed.isEmpty()) {
                return Map.of();
            }
            List<DeliveryLease.Ref> refs = processed.stream().map(TideClient.Received::lease)
                    .map(DeliveryLease::ref).toList();
            try {
                return client.ack(assignment.number(), refs);
            } catch (BrokerException changed) {
                if (changed.code() != BrokerException.Code.STALE_EPOCH
                        || batch.incarnation != incarnation) {
                    throw changed;
                }
                // Cooperative rebalance advanced the group generation on this same
                // connection: refresh once and settle against the current epoch.
                assignment = client.assignment();
                return client.ack(assignment.number(), refs);
            }
        } catch (BrokerException rejected) {
            throw rejected;
        } catch (IOException transport) {
            drop(transport);
            throw new IOException("acknowledgement result unknown for " + group + "/" + member, transport);
        } finally {
            operation.unlock();
        }
    }

    private void ensureJoined() throws IOException {
        if (closed) {
            throw new IOException("consumer is closed");
        }
        if (client != null) {
            return;
        }
        if (incarnation == Long.MAX_VALUE) {
            throw new IOException("consumer connection space exhausted");
        }
        client = connect.open();
        try {
            assignment = client.join(group, member, topics);
            incarnation++;
        } catch (IOException error) {
            drop(error);
            throw error;
        }
    }

    private void drop(IOException error) {
        try {
            client.close();
        } catch (IOException close) {
            error.addSuppressed(close);
        } finally {
            client = null;
            assignment = null;
        }
    }

    @Override
    public void close() throws IOException {
        operation.lock();
        try {
            if (closed) {
                return;
            }
            closed = true;
            if (client != null) {
                try (TideClient active = client) {
                    active.leave();
                } finally {
                    client = null;
                    assignment = null;
                }
            }
        } finally {
            operation.unlock();
        }
    }
}
