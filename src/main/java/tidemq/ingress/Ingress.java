package tidemq.ingress;

import tidemq.BrokerException;
import tidemq.Settings;
import tidemq.segment.Message;
import tidemq.segment.SegmentStore;
import tidemq.segment.TopicPartition;

import java.io.IOException;
import java.util.Arrays;
import java.util.Objects;

public final class Ingress {
    public static final int ROUTE_BY_KEY = -1;

    private final SegmentStore segments;
    private final ReceiptIndex receipts;
    private final int messageLimit;

    public Ingress(Settings settings, SegmentStore segments, long now) throws IOException {
        this.segments = segments;
        this.messageLimit = settings.messageBytes();
        this.receipts = new ReceiptIndex(settings.dataDirectory().resolve("produce.v1.journal"), now);
        receipts.verifyRanges(segments);
        segments.forEach(receipts::reconcile);
    }

    public ProduceReceipt publish(String producer, long sequence, String topic, int partition,
                                  byte[] key, byte[] value, long now) throws IOException {
        Objects.requireNonNull(key);
        Objects.requireNonNull(value);
        var identity = new ReceiptIndex.Identity(producer, sequence);
        Settings.requireIdentifier(topic, "topic");
        if (key.length > Settings.MAX_KEY_BYTES || value.length > messageLimit) {
            throw new BrokerException(BrokerException.Code.BAD_REQUEST, "key or payload exceeds configured limit");
        }
        int partitionCount = segments.partitionCount(topic);
        int resolved = partition;
        if (partition == ROUTE_BY_KEY) {
            if (key.length == 0) {
                throw new BrokerException(BrokerException.Code.BAD_REQUEST, "automatic routing requires a key");
            }
            resolved = Math.floorMod(Arrays.hashCode(key), partitionCount);
        }
        if (resolved < 0 || resolved >= partitionCount) {
            throw new BrokerException(BrokerException.Code.UNKNOWN_TOPIC, "unknown partition " + topic + "/" + partition);
        }
        var route = new TopicPartition(topic, resolved);
        byte[] digest = ReceiptIndex.fingerprint(route, key, value);
        ReceiptIndex.Entry previous = receipts.lookup(identity);
        if (previous != null) {
            if (!Arrays.equals(previous.hash(), digest)) {
                throw new BrokerException(BrokerException.Code.PRODUCER_CONFLICT,
                        "producer sequence already has a different message");
            }
            return previous.receipt().asReplay();
        }
        Message message = segments.append(route, producer, sequence, key, value, now);
        return receipts.record(message, digest);
    }
}
