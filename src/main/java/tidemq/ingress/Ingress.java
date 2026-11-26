package tidemq.ingress;

import tidemq.BrokerException;
import tidemq.Settings;
import tidemq.segment.Message;
import tidemq.segment.SegmentStore;
import tidemq.segment.TopicPartition;

import java.io.IOException;
import java.security.MessageDigest;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;

public final class Ingress {
    public static final int ROUTE_BY_KEY = -1;

    private final SegmentStore segments;
    private final ReceiptIndex receipts;
    private final BatchJournal batches;
    private final Map<TopicPartition, ReceiptIndex.Identity> pendingBatchOwners = new HashMap<>();
    private final int messageLimit;
    private final int batchLimit;

    public Ingress(Settings settings, SegmentStore segments, long now) throws IOException {
        this.segments = segments;
        this.messageLimit = settings.messageBytes();
        this.batchLimit = settings.maxBatch();
        this.batches = new BatchJournal(settings.dataDirectory().resolve(BatchJournal.FILE_NAME), now);
        recoverBatches();
        for (var pending : batches.pending()) {
            pendingBatchOwners.put(pending.getValue().partition(), pending.getKey());
        }
        this.receipts = new ReceiptIndex(settings.dataDirectory().resolve("produce.v1.journal"), now);
        receipts.verifyRanges(segments);
        segments.forEach(message -> {
            if (!batches.covers(message.partition(), message.offset())) {
                receipts.reconcile(message);
            }
        });
        verifyCommittedBatches();
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
        int resolved = resolvePartition(topic, partition, key);
        var route = new TopicPartition(topic, resolved);
        rejectWhileBatchPending(route);
        byte[] digest = ReceiptIndex.fingerprint(route, key, value);
        ReceiptIndex.Entry previous = receipts.lookup(identity);
        if (previous != null) {
            if (!Arrays.equals(previous.hash(), digest)) {
                throw new BrokerException(BrokerException.Code.PRODUCER_CONFLICT,
                        "producer sequence already has a different message");
            }
            return previous.receipt().asReplay();
        }

        BatchReceipt batch = batches.replay(identity);
        if (batch != null) {
            byte[] batchDigest = BatchJournal.fingerprint(route, List.of(new BatchItem(key, value)));
            BatchJournal.Stored stored = batches.lookup(identity);
            if (!route.equals(batch.partition()) || !MessageDigest.isEqual(stored.hash(), batchDigest)) {
                throw new BrokerException(BrokerException.Code.PRODUCER_CONFLICT,
                        "producer sequence already has a different batch message");
            }
            return new ProduceReceipt(batch.producer(), batch.batchId(), batch.partition(),
                    batch.baseOffset(), batch.appendedAtMs(), true);
        }

        Message message = segments.append(route, producer, sequence, key, value, now);
        return receipts.record(message, digest);
    }

    public BatchReceipt publishBatch(String producer, long batchId, String topic, int partition,
                                     List<?> rawItems, long now) throws IOException {
        List<BatchItem> items = asBatchItems(rawItems);
        var identity = new ReceiptIndex.Identity(producer, batchId);
        Settings.requireIdentifier(topic, "topic");
        validateItems(items);
        if (partition < 0) {
            throw new BrokerException(BrokerException.Code.BAD_REQUEST,
                    "producer batches require one explicit partition");
        }
        int partitionCount = segments.partitionCount(topic);
        if (partition >= partitionCount) {
            throw new BrokerException(BrokerException.Code.UNKNOWN_TOPIC,
                    "unknown partition " + topic + "/" + partition);
        }
        var route = new TopicPartition(topic, partition);
        byte[] digest = BatchJournal.fingerprint(route, items);
        ReceiptIndex.Identity pendingOwner = pendingBatchOwners.get(route);
        if (pendingOwner != null && !pendingOwner.equals(identity)) {
            throw new BrokerException(BrokerException.Code.PRODUCER_CONFLICT,
                    "partition has an unresolved producer batch; resend its original producer and batch id");
        }

        BatchJournal.Stored existing = batches.lookup(identity);
        if (existing != null) {
            if (!route.equals(existing.partition()) || existing.count() != items.size()
                    || !MessageDigest.isEqual(existing.hash(), digest)) {
                throw new BrokerException(BrokerException.Code.PRODUCER_CONFLICT,
                        "producer batch identity already has different content");
            }
            if (existing.committed()) {
                return batches.replay(identity);
            }
            // Recovery truncates an interrupted intent to its planned base. The
            // broker is fenced after an in-process storage fault, so reaching here
            // in a fresh Ingress means the prefix is no longer present.
            long currentEnd = segments.range(route).nextOffset();
            if (currentEnd != existing.baseOffset()) {
                throw new IOException("uncommitted batch " + identity
                        + " cannot be completed at noncontiguous end " + currentEnd);
            }
            return appendAndCommitBatch(identity, route, items, digest, existing.baseOffset(),
                    existing.appendedAtMs());
        }

        ReceiptIndex.Entry singular = receipts.lookup(identity);
        if (singular != null) {
            if (items.size() != 1
                    || !route.equals(singular.receipt().partition())
                    || !Arrays.equals(singular.hash(), ReceiptIndex.fingerprint(
                            route, items.getFirst().key(), items.getFirst().payload()))) {
                throw new BrokerException(BrokerException.Code.PRODUCER_CONFLICT,
                        "producer sequence already has a different message");
            }
            return new BatchReceipt(producer, batchId, route, singular.receipt().offset(), 1,
                    singular.receipt().appendedAtMs(), true);
        }

        long baseOffset = segments.range(route).nextOffset();
        if (baseOffset == Long.MAX_VALUE) {
            throw new IOException("offset space exhausted for " + route);
        }
        batches.recordIntent(identity, route, baseOffset, items.size(), now, digest);
        return appendAndCommitBatch(identity, route, items, digest, baseOffset, now);
    }

    public BatchReceipt publishBatch(String producer, long batchId, String topic, int partition,
                                     byte[] key, List<?> payloads, long now) throws IOException {
        List<BatchItem> items = payloads.stream()
                .map(BatchItems::from)
                .map(item -> new BatchItem(key, item.payload()))
                .toList();
        return publishBatch(producer, batchId, topic, partition, items, now);
    }

    public BatchReceipt appendBatch(String producer, long batchId, String topic, int partition,
                                    List<?> rawItems, long now) throws IOException {
        return publishBatch(producer, batchId, topic, partition, rawItems, now);
    }

    public BatchReceipt appendBatch(String producer, long batchId, String topic, int partition,
                                    byte[] key, List<?> payloads, long now) throws IOException {
        return publishBatch(producer, batchId, topic, partition, key, payloads, now);
    }

    private BatchReceipt appendAndCommitBatch(ReceiptIndex.Identity identity, TopicPartition route,
                                              List<BatchItem> items, byte[] digest, long baseOffset,
                                              long appendedAtMs) throws IOException {
        try {
            long expectedOffset = baseOffset;
            for (BatchItem item : items) {
                Message message = segments.append(route, identity.producer(), identity.sequence(),
                        item.key(), item.payload(), appendedAtMs);
                if (message.offset() != expectedOffset) {
                    throw new IOException("batch append lost contiguous offset: expected "
                            + expectedOffset + ", got " + message.offset());
                }
                expectedOffset++;
            }
            BatchJournal.Stored intent = batches.lookup(identity);
            if (intent == null) {
                // Should be impossible: callers create or recover the intent first.
                batches.recordIntent(identity, route, baseOffset, items.size(), appendedAtMs, digest);
                intent = batches.lookup(identity);
            }
            BatchReceipt receipt = batches.commit(identity, intent);
            pendingBatchOwners.remove(route, identity);
            return receipt;
        } catch (RuntimeException failure) {
            // Once intent exists, any unchecked append/index failure leaves an
            // uncertain tail. Fence the broker and let journal recovery rewind it.
            throw new IOException("producer batch append failed", failure);
        }
    }

    private int resolvePartition(String topic, int partition, byte[] key) throws BrokerException {
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
        return resolved;
    }

    private void rejectWhileBatchPending(TopicPartition route) throws BrokerException {
        if (pendingBatchOwners.containsKey(route)) {
            throw new BrokerException(BrokerException.Code.PRODUCER_CONFLICT,
                    "partition has an unresolved producer batch; resend that batch before other writes");
        }
    }

    private List<BatchItem> asBatchItems(List<?> rawItems) throws BrokerException {
        if (rawItems == null) {
            throw new BrokerException(BrokerException.Code.BAD_REQUEST, "producer batch items are required");
        }
        var items = new ArrayList<BatchItem>(rawItems.size());
        for (Object raw : rawItems) {
            try {
                items.add(BatchItems.from(raw));
            } catch (IllegalArgumentException invalid) {
                throw new BrokerException(BrokerException.Code.BAD_REQUEST, invalid.getMessage());
            }
        }
        return List.copyOf(items);
    }

    private void validateItems(List<BatchItem> items) throws BrokerException {
        if (items == null || items.isEmpty()) {
            throw new BrokerException(BrokerException.Code.BAD_REQUEST, "producer batch must contain at least one message");
        }
        if (items.size() > batchLimit) {
            throw new BrokerException(BrokerException.Code.BAD_REQUEST, "producer batch exceeds delivery.max.batch");
        }
        for (BatchItem item : items) {
            Objects.requireNonNull(item, "batch item");
            if (item.key().length > Settings.MAX_KEY_BYTES || item.payload().length > messageLimit) {
                throw new BrokerException(BrokerException.Code.BAD_REQUEST,
                        "batch key or payload exceeds configured limit");
            }
        }
    }

    private void recoverBatches() throws IOException {
        for (var entry : batches.pending()) {
            var identity = entry.getKey();
            var stored = entry.getValue();
            TopicPartition partition = stored.partition();
            SegmentStore.Range range = segments.range(partition);
            long base = stored.baseOffset();
            long end = range.nextOffset();
            if (base < range.retainedFrom() || base > end) {
                throw new IOException("batch intent points outside retained range of " + partition);
            }
            long available = end - base;
            if (available == stored.count() && batchBytesMatch(stored, base)) {
                batches.commitExisting(identity);
            } else if (available >= 0 && available <= stored.count()) {
                // Remove a partial or mismatching prefix so it can never be read.
                // Truncation also deletes/rewinds any segment created by the batch.
                segments.truncateTo(partition, base);
            } else {
                throw new IOException("batch intent for " + identity
                        + " has later records and cannot be recovered safely");
            }
        }
    }

    private void verifyCommittedBatches() throws IOException {
        for (var entry : batches.records()) {
            var identity = entry.getKey();
            var stored = entry.getValue();
            if (!stored.committed()) {
                // Interrupted intents remain pending until the producer retries;
                // recoverBatches has already rewound their visible segment tail.
                continue;
            }
            SegmentStore.Range range = segments.range(stored.partition());
            long endExclusive = Math.addExact(stored.baseOffset(), stored.count());
            if (endExclusive > range.nextOffset()) {
                throw new IOException("committed batch points outside retained range for " + identity);
            }
            if (stored.baseOffset() >= range.retainedFrom()
                    && !batchBytesMatch(stored, stored.baseOffset())) {
                throw new IOException("committed batch bytes disagree with journal for " + identity);
            }
        }
    }

    private boolean batchBytesMatch(BatchJournal.Stored stored, long baseOffset) throws IOException {
        var messages = new ArrayList<Message>(stored.count());
        for (int i = 0; i < stored.count(); i++) {
            messages.add(segments.read(stored.partition(), baseOffset + i));
        }
        byte[] actual = BatchJournal.fingerprintMessages(messages);
        return MessageDigest.isEqual(actual, stored.hash());
    }
}
