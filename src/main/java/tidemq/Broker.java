package tidemq;

import tidemq.deadletter.DeadLetters;
import tidemq.delivery.DeliveryEngine;
import tidemq.delivery.DeliveryLease;
import tidemq.delivery.FlowView;
import tidemq.group.GroupDirectory;
import tidemq.group.GroupEpoch;
import tidemq.ingress.Ingress;
import tidemq.ingress.ProduceReceipt;
import tidemq.segment.RetentionPreview;
import tidemq.segment.SegmentStore;
import tidemq.segment.TopicPartition;

import java.io.IOException;
import java.time.Clock;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.locks.ReentrantLock;

public final class Broker implements AutoCloseable {
    private static final System.Logger LOG = System.getLogger(Broker.class.getName());

    public record Member(String group, String name, long session) {}

    @FunctionalInterface
    private interface Operation<T> {
        T run() throws IOException;
    }

    private final Settings settings;
    private final Clock clock;
    private final DataDirectory directory;
    private final SegmentStore segments;
    private final Ingress ingress;
    private final GroupDirectory groups;
    private final DeadLetters deadLetters;
    private final DeliveryEngine delivery;
    private final ReentrantLock transitions = new ReentrantLock();
    private long lastTime;
    private long lastRetention;
    private IOException failed;
    private boolean closed;

    public static Broker open(Settings settings, Clock clock, Boundary boundary) throws IOException {
        DataDirectory directory = DataDirectory.claim(settings.dataDirectory());
        try {
            return new Broker(settings, clock, boundary, directory);
        } catch (IOException | RuntimeException failure) {
            try {
                directory.close();
            } catch (IOException closeError) {
                failure.addSuppressed(closeError);
            }
            throw failure;
        }
    }

    private Broker(Settings settings, Clock clock, Boundary boundary, DataDirectory directory) throws IOException {
        this.settings = settings;
        this.clock = clock;
        this.directory = directory;
        var layout = new StorageLayout(settings.dataDirectory());
        long now = now();
        segments = new SegmentStore(settings, boundary, now);
        ingress = new Ingress(settings, segments, now);
        groups = new GroupDirectory(settings, segments, boundary);
        deadLetters = new DeadLetters(settings.dataDirectory(), boundary, now);
        delivery = new DeliveryEngine(settings, segments, groups, deadLetters);
        delivery.recover(now);
        layout.publish();
        lastRetention = now;
    }

    public ProduceReceipt produce(String producer, long sequence, String topic, int partition,
                                               byte[] key, byte[] payload) throws IOException {
        return run(() -> ingress.publish(producer, sequence, topic, partition, key, payload, now()));
    }

    public GroupEpoch join(Member member, Set<String> topics) throws IOException {
        return run(() -> {
            long before;
            try {
                before = groups.require(member.group()).epoch();
            } catch (BrokerException unknown) {
                before = 0;
            }
            GroupEpoch epoch = groups.join(member.group(), member.name(), member.session(), topics);
            if (before != epoch.number()) {
                delivery.invalidate(member.group(), now());
            }
            return epoch;
        });
    }

    public void leave(Member member) throws IOException {
        run(() -> {
            if (groups.disconnect(member.group(), member.name(), member.session())) {
                delivery.invalidate(member.group(), now());
            }
            return null;
        });
    }

    public GroupEpoch assignment(Member member) throws IOException {
        return run(() -> groups.current(member.group(), member.name(), member.session()));
    }

    public List<DeliveryLease> pull(Member member, long epoch, int limit, int byteBudget)
            throws IOException {
        return run(() -> delivery.pull(checkEpoch(member, epoch), member.session(), limit, byteBudget, now()));
    }

    public List<DeliveryLease.Acceptance> accept(Member member, long epoch, List<DeliveryLease.Ref> refs)
            throws IOException {
        return run(() -> delivery.accept(checkEpoch(member, epoch), member.session(), refs, now()));
    }

    public List<DeliveryLease.Acceptance> renew(Member member, long epoch, List<DeliveryLease.Ref> refs)
            throws IOException {
        return run(() -> delivery.renew(checkEpoch(member, epoch), member.session(), refs, now()));
    }

    public Map<TopicPartition, Long> ack(Member member, long epoch, List<DeliveryLease.Ref> refs)
            throws IOException {
        return run(() -> delivery.ack(checkEpoch(member, epoch), member.session(), refs, now()));
    }

    private GroupEpoch checkEpoch(Member member, long epoch) throws BrokerException {
        return groups.requireEpoch(member.group(), member.name(), member.session(), epoch);
    }

    public GroupDirectory.View group(String name) throws IOException {
        return run(() -> groups.view(name));
    }

    public FlowView flow(String name, int memberLimit) throws IOException {
        return run(() -> delivery.view(name, memberLimit, now()));
    }

    public SegmentStore.PartitionView partition(TopicPartition partition, int limit) throws IOException {
        return run(() -> segments.view(partition, limit));
    }

    public DeadLetters.Page deadLetters(String group, long from, int limit, int byteBudget)
            throws IOException {
        return run(() -> {
            groups.require(group);
            return deadLetters.read(group, from, limit, byteBudget);
        });
    }

    public Map<String, Integer> topics() throws IOException {
        return run(segments::topics);
    }

    public int retire() throws IOException {
        return run(() -> retire(now()));
    }

    public RetentionPreview previewRetention(TopicPartition partition) throws IOException {
        return run(() -> {
            var barrier = groups.retentionBarrier(partition);
            return segments.preview(partition, barrier.frontier(), now(), barrier.groupCount(), barrier.firstGroups());
        });
    }

    private int retire(long now) throws IOException {
        int retired = 0;
        for (var entry : groups.retentionFrontiers().entrySet()) {
            retired += segments.retire(entry.getKey(), entry.getValue(), now);
        }
        lastRetention = now;
        return retired;
    }

    public void maintain() throws IOException {
        run(() -> {
            long now = now();
            delivery.sweep(now);
            if (now - lastRetention >= settings.retentionIntervalMs()) {
                retire(now);
            }
            return null;
        });
    }

    private long now() {
        lastTime = Math.max(lastTime, clock.millis());
        return lastTime;
    }

    private <T> T run(Operation<T> operation) throws IOException {
        transitions.lock();
        try {
            if (closed || failed != null) {
                throw new BrokerException(BrokerException.Code.STORAGE_FAILURE, "broker storage is unavailable");
            }
            return operation.run();
        } catch (BrokerException requestError) {
            throw requestError;
        } catch (IOException storageError) {
            failed = storageError;
            LOG.log(System.Logger.Level.ERROR, "broker fenced after storage failure", storageError);
            throw new BrokerException(BrokerException.Code.STORAGE_FAILURE, "storage operation failed; restart required");
        } finally {
            transitions.unlock();
        }
    }

    @Override
    public void close() throws IOException {
        transitions.lock();
        try {
            if (!closed) {
                closed = true;
                directory.close();
            }
        } finally {
            transitions.unlock();
        }
    }
}
