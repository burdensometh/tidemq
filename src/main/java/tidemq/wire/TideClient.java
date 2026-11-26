package tidemq.wire;

import tidemq.Binary;
import tidemq.BrokerException;
import tidemq.Settings;
import tidemq.deadletter.DeadLetters;
import tidemq.delivery.DeliveryLease;
import tidemq.delivery.FlowView;
import tidemq.group.GroupDirectory;
import tidemq.group.GroupEpoch;
import tidemq.ingress.BatchItem;
import tidemq.ingress.BatchReceipt;
import tidemq.ingress.ProduceReceipt;
import tidemq.ingress.StringBatchReceipt;
import tidemq.segment.MessageV1;
import tidemq.segment.RetentionPreview;
import tidemq.segment.SegmentStore;
import tidemq.segment.TopicPartition;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.EOFException;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.locks.ReentrantLock;

public final class TideClient implements AutoCloseable {
    public static final int DEFAULT_TIMEOUT_MS = 5000;
    @FunctionalInterface
    public interface Connector {
        TideClient open() throws IOException;
    }

    private static final Binary.Writer EMPTY = out -> {};

    public record Received(DeliveryLease lease, long expiresAtMs) {}

    public record ProduceItem(byte[] key, byte[] payload) {
        public ProduceItem(byte[] payload) {
            this(EMPTY_KEY, payload);
        }
    }

    public record ProduceBatchItem(byte[] key, byte[] payload) {
        public ProduceBatchItem(byte[] payload) {
            this(EMPTY_KEY, payload);
        }
    }

    public record BatchEntry(byte[] key, byte[] payload) {
        public BatchEntry(byte[] payload) {
            this(EMPTY_KEY, payload);
        }
    }

    public record Item(byte[] key, byte[] payload) {
        public Item(byte[] payload) {
            this(EMPTY_KEY, payload);
        }
    }

    public record BatchMessage(byte[] key, byte[] payload) {
        public BatchMessage(byte[] payload) {
            this(EMPTY_KEY, payload);
        }
    }

    public record Message(byte[] key, byte[] payload) {
        public Message(byte[] payload) {
            this(EMPTY_KEY, payload);
        }
    }

    public record ProduceMessage(byte[] key, byte[] payload) {
        public ProduceMessage(byte[] payload) {
            this(EMPTY_KEY, payload);
        }
    }

    public record Record(byte[] key, byte[] payload) {
        public Record(byte[] payload) {
            this(EMPTY_KEY, payload);
        }
    }

    public static class ProducerBatch extends tidemq.ingress.ProducerBatch {
        public ProducerBatch(String producer, long batchId, String topic, int partition, List<?> items) {
            super(producer, batchId, topic, partition, items);
        }

        public ProducerBatch(String producer, String batchId, String topic, int partition, List<?> items) {
            super(producer, batchId, topic, partition, items);
        }
    }

    public static final class BatchRequest extends ProducerBatch {
        public BatchRequest(String producer, long batchId, String topic, int partition, List<?> items) {
            super(producer, batchId, topic, partition, items);
        }

        public BatchRequest(String producer, String batchId, String topic, int partition, List<?> items) {
            super(producer, batchId, topic, partition, items);
        }
    }

    public static final class ProduceBatch extends ProducerBatch {
        public ProduceBatch(String producer, long batchId, String topic, int partition, List<?> items) {
            super(producer, batchId, topic, partition, items);
        }

        public ProduceBatch(String producer, String batchId, String topic, int partition, List<?> items) {
            super(producer, batchId, topic, partition, items);
        }
    }

    public static final class Batch extends ProducerBatch {
        public Batch(String producer, long batchId, String topic, int partition, List<?> items) {
            super(producer, batchId, topic, partition, items);
        }

        public Batch(String producer, String batchId, String topic, int partition, List<?> items) {
            super(producer, batchId, topic, partition, items);
        }
    }

    private static final byte[] EMPTY_KEY = new byte[0];

    private final Socket socket;
    private final DataInputStream in;
    private final DataOutputStream out;
    private final WireValues.Welcome welcome;
    private final ReentrantLock requests = new ReentrantLock();
    private long lastRequest;

    public static TideClient connect(String host, int port) throws IOException {
        return connect(host, port, DEFAULT_TIMEOUT_MS, Settings.MAX_FRAME_BYTES);
    }

    public static TideClient connect(String host, int port, int timeoutMs, int maxFrame) throws IOException {
        return connect(host, port, timeoutMs, maxFrame, WireValues.VERSION, WireValues.VERSION);
    }

    public static TideClient connect(String host, int port, int timeoutMs, int maxFrame,
                                    int minVersion, int maxVersion) throws IOException {
        if (timeoutMs < 1 || maxFrame < Settings.MIN_FRAME_BYTES || maxFrame > Settings.MAX_FRAME_BYTES) {
            throw new IllegalArgumentException("invalid timeout or frame limit");
        }
        var socket = new Socket();
        try {
            socket.connect(new InetSocketAddress(host, port), timeoutMs);
            socket.setSoTimeout(timeoutMs);
            socket.setTcpNoDelay(true);
            return new TideClient(socket, maxFrame, minVersion, maxVersion);
        } catch (IOException | RuntimeException failure) {
            try {
                socket.close();
            } catch (IOException close) {
                failure.addSuppressed(close);
            }
            throw failure;
        }
    }

    private TideClient(Socket socket, int maxFrame, int minVersion, int maxVersion) throws IOException {
        this.socket = socket;
        in = new DataInputStream(new BufferedInputStream(socket.getInputStream()));
        out = new DataOutputStream(new BufferedOutputStream(socket.getOutputStream()));
        welcome = exchange(Opcode.HELLO, Settings.MIN_FRAME_BYTES, output -> {
            output.writeInt(minVersion);
            output.writeInt(maxVersion);
            output.writeInt(maxFrame);
        }, input -> new WireValues.Welcome(input.readInt(), input.readInt(), input.readInt()));
        if (welcome.version() != WireValues.VERSION || welcome.version() < minVersion
                || welcome.version() > maxVersion || welcome.maxFrame() < Settings.MIN_FRAME_BYTES
                || welcome.maxFrame() > maxFrame || welcome.maxBatch() < 1
                || welcome.maxBatch() > WireValues.MAX_BATCH) {
            throw new IOException("broker sent unsupported session limits");
        }
    }

    public int version() {
        return welcome.version();
    }

    public int maxFrame() {
        return welcome.maxFrame();
    }

    public int maxBatch() {
        return welcome.maxBatch();
    }

    public void ping() throws IOException {
        request(Opcode.PING, EMPTY, input -> null);
    }

    public ProduceReceipt produce(String producer, long sequence, String topic, int partition,
                                  byte[] key, byte[] payload) throws IOException {
        var publication = new WireValues.Publication(producer, sequence, topic, partition, key, payload);
        return request(Opcode.PRODUCE, output -> WireValues.writePublication(output, publication),
                WireValues::readReceipt);
    }

    public BatchReceipt produceBatch(String producer, long batchId, String topic, int partition,
                                     List<?> items) throws IOException {
        List<BatchItem> batchItems = items.stream().map(TideClient::toBatchItem).toList();
        var batch = new WireValues.BatchPublication(producer, batchId, topic, partition, batchItems);
        return request(Opcode.PRODUCE_BATCH, output -> WireValues.writeBatchPublication(output, batch),
                WireValues::readBatchReceipt);
    }

    public StringBatchReceipt produceBatch(String producer, String batchId, String topic, int partition,
                                           List<?> items) throws IOException {
        long surrogate = stringBatchSurrogate(batchId);
        BatchReceipt receipt = produceBatch(producer, surrogate, topic, partition, items);
        return new StringBatchReceipt(receipt.producer(), batchId, receipt.partition(),
                receipt.baseOffset(), receipt.count(), receipt.appendedAtMs(), receipt.replayed());
    }

    static long stringBatchSurrogate(String batchId) {
        try {
            byte[] digest = java.security.MessageDigest.getInstance("SHA-256")
                    .digest(batchId.getBytes(java.nio.charset.StandardCharsets.UTF_8));
            long value = 0;
            for (int i = 0; i < 8; i++) {
                value = (value << 8) | (digest[i] & 255L);
            }
            return value & Long.MAX_VALUE;
        } catch (java.security.NoSuchAlgorithmException impossible) {
            throw new IllegalStateException(impossible);
        }
    }

    public BatchReceipt produceBatch(String producer, long batchId, String topic, int partition,
                                     byte[] key, List<?> payloads) throws IOException {
        List<BatchItem> items = payloads.stream()
                .map(TideClient::toBatchItem)
                .map(item -> new BatchItem(key, item.payload()))
                .toList();
        return produceBatch(producer, batchId, topic, partition, items);
    }

    public BatchReceipt produceBatch(String producer, long batchId, String topic, int partition,
                                     List<?> items, byte[] defaultKey) throws IOException {
        List<BatchItem> rewritten = items.stream().map(value -> {
            BatchItem item = toBatchItem(value);
            return new BatchItem(defaultKey, item.payload());
        }).toList();
        return produceBatch(producer, batchId, topic, partition, rewritten);
    }

    public BatchReceipt produceBatch(String producer, long batchId, String topic, int partition,
                                     List<byte[]> keys, List<byte[]> payloads) throws IOException {
        if (keys.size() != payloads.size()) {
            throw new IllegalArgumentException("keys and payloads must have equal lengths");
        }
        var items = new java.util.ArrayList<BatchItem>(keys.size());
        for (int i = 0; i < keys.size(); i++) {
            items.add(new BatchItem(keys.get(i), payloads.get(i)));
        }
        return produceBatch(producer, batchId, topic, partition, items);
    }

    public BatchReceipt produceBatch(String producer, long batchId, String topic, int partition,
                                     byte[][] keys, byte[][] payloads) throws IOException {
        if (keys.length != payloads.length) {
            throw new IllegalArgumentException("keys and payloads must have equal lengths");
        }
        var items = new java.util.ArrayList<BatchItem>(keys.length);
        for (int i = 0; i < keys.length; i++) {
            items.add(new BatchItem(keys[i], payloads[i]));
        }
        return produceBatch(producer, batchId, topic, partition, items);
    }

    public BatchReceipt produceBatch(Object batchRequest) throws IOException {
        return BatchRequests.send(this, batchRequest);
    }

    public BatchReceipt produce(Object batchRequest) throws IOException {
        return produceBatch(batchRequest);
    }

    public BatchReceipt sendBatch(Object batchRequest) throws IOException {
        return produceBatch(batchRequest);
    }

    public BatchReceipt produceBatch(String producer, long batchId, String topic, int partition,
                                     Object... items) throws IOException {
        return produceBatch(producer, batchId, topic, partition, List.of(items));
    }

    public BatchReceipt publishBatch(String producer, long batchId, String topic, int partition,
                                     List<?> items) throws IOException {
        return produceBatch(producer, batchId, topic, partition, items);
    }

    public BatchReceipt publishBatch(Object batchRequest) throws IOException {
        return produceBatch(batchRequest);
    }

    public BatchReceipt publishBatch(String producer, long batchId, String topic, int partition,
                                     byte[] key, List<?> payloads) throws IOException {
        return produceBatch(producer, batchId, topic, partition, key, payloads);
    }

    public BatchReceipt publishBatch(String producer, long batchId, String topic, int partition,
                                     List<byte[]> keys, List<byte[]> payloads) throws IOException {
        return produceBatch(producer, batchId, topic, partition, keys, payloads);
    }

    public BatchReceipt sendBatch(String producer, long batchId, String topic, int partition,
                                  List<?> items) throws IOException {
        return produceBatch(producer, batchId, topic, partition, items);
    }

    public BatchReceipt sendBatch(String producer, long batchId, String topic, int partition,
                                  byte[] key, List<?> payloads) throws IOException {
        return produceBatch(producer, batchId, topic, partition, key, payloads);
    }

    public BatchReceipt sendBatch(String producer, long batchId, String topic, int partition,
                                  List<byte[]> keys, List<byte[]> payloads) throws IOException {
        return produceBatch(producer, batchId, topic, partition, keys, payloads);
    }

    public BatchReceipt batchProduce(String producer, long batchId, String topic, int partition,
                                     List<?> items) throws IOException {
        return produceBatch(producer, batchId, topic, partition, items);
    }

    public BatchReceipt batchProduce(String producer, long batchId, String topic, int partition,
                                     byte[] key, List<?> payloads) throws IOException {
        return produceBatch(producer, batchId, topic, partition, key, payloads);
    }

    private static BatchItem toBatchItem(Object value) {
        if (value instanceof BatchItem item) {
            return item;
        }
        if (value instanceof tidemq.ingress.ProduceItem item) {
            return new BatchItem(item.key(), item.payload());
        }
        if (value instanceof byte[] payload) {
            return new BatchItem(EMPTY_KEY, payload);
        }
        if (value instanceof String text) {
            return new BatchItem(EMPTY_KEY, text.getBytes(java.nio.charset.StandardCharsets.UTF_8));
        }
        if (value instanceof java.nio.ByteBuffer buffer) {
            java.nio.ByteBuffer duplicate = buffer.duplicate();
            byte[] payload = new byte[duplicate.remaining()];
            duplicate.get(payload);
            return new BatchItem(EMPTY_KEY, payload);
        }
        if (value instanceof ProduceItem item) {
            return new BatchItem(item.key(), item.payload());
        }
        if (value instanceof BatchMessage item) {
            return new BatchItem(item.key(), item.payload());
        }
        if (value instanceof Message item) {
            return new BatchItem(item.key(), item.payload());
        }
        if (value instanceof ProduceMessage item) {
            return new BatchItem(item.key(), item.payload());
        }
        if (value instanceof Record item) {
            return new BatchItem(item.key(), item.payload());
        }
        if (value instanceof java.util.Map<?, ?> map) {
            Object key = map.get("key");
            Object payload = map.containsKey("payload") ? map.get("payload") : map.get("value");
            if (key instanceof byte[] keyBytes && payload instanceof byte[] payloadBytes) {
                return new BatchItem(keyBytes, payloadBytes);
            }
        }
        try {
            byte[] key = invokeBytes(value, "key", "getKey", "keyBytes");
            byte[] payload = invokeBytes(value, "payload", "getPayload", "value", "getValue",
                    "body", "getBody", "data", "getData", "content", "getContent", "bytes");
            if (payload != null) {
                return new BatchItem(key == null ? EMPTY_KEY : key, payload);
            }
        } catch (ReflectiveOperationException ignored) {
            // Fall through to argument error.
        }
        throw new IllegalArgumentException("unsupported producer batch item: "
                + (value == null ? "null" : value.getClass().getName()));
    }

    private static byte[] invokeBytes(Object target, String... names) throws ReflectiveOperationException {
        Class<?> type = target.getClass();
        for (String name : names) {
            try {
                var method = type.getMethod(name);
                Object result = method.invoke(target);
                if (result instanceof byte[] bytes) {
                    return bytes;
                }
            } catch (NoSuchMethodException ignored) {
                // Try next accessor spelling.
            }
        }
        return null;
    }

    public GroupEpoch join(String group, String member, Set<String> topics) throws IOException {
        return request(Opcode.JOIN, output -> {
            Binary.writeText(output, group);
            Binary.writeText(output, member);
            WireValues.writeNames(output, topics);
        }, WireValues::readEpoch);
    }

    public GroupEpoch assignment() throws IOException {
        return request(Opcode.ASSIGN, EMPTY, WireValues::readEpoch);
    }

    public List<DeliveryLease> reserve(long epoch, int limit) throws IOException {
        return request(Opcode.PULL, output -> {
            output.writeLong(epoch);
            output.writeInt(limit);
        }, WireValues::readLeases);
    }

    public List<DeliveryLease.Acceptance> accept(long epoch, List<DeliveryLease.Ref> refs) throws IOException {
        return request(Opcode.ACCEPT, output ->
                WireValues.writeSettlement(output, new WireValues.Settlement(epoch, refs)), WireValues::readAcceptances);
    }

    public List<Received> pull(long epoch, int limit) throws IOException {
        requests.lock();
        try {
            List<DeliveryLease> leases = reserve(epoch, limit);
            if (leases.isEmpty()) {
                return List.of();
            }
            List<DeliveryLease.Ref> refs = leases.stream().map(DeliveryLease::ref).toList();
            var receipts = accept(epoch, refs);
            if (receipts.size() != leases.size()) {
                throw new IOException("broker did not acknowledge every requested acceptance");
            }
            var received = new java.util.ArrayList<Received>(leases.size());
            for (int i = 0; i < leases.size(); i++) {
                if (!refs.get(i).equals(receipts.get(i).ref())) {
                    throw new IOException("acceptance does not match delivery");
                }
                received.add(new Received(leases.get(i), receipts.get(i).expiresAtMs()));
            }
            return List.copyOf(received);
        } finally {
            requests.unlock();
        }
    }

    public Map<TopicPartition, Long> ack(long epoch, List<DeliveryLease.Ref> refs) throws IOException {
        return request(Opcode.ACK, output ->
                WireValues.writeSettlement(output, new WireValues.Settlement(epoch, refs)), WireValues::readFrontiers);
    }

    public void leave() throws IOException {
        request(Opcode.LEAVE, EMPTY, input -> null);
    }

    public Map<String, Integer> topics() throws IOException {
        return request(Opcode.TOPICS, EMPTY, WireValues::readTopics);
    }

    public SegmentStore.PartitionView partition(TopicPartition partition, int limit) throws IOException {
        return request(Opcode.PARTITION, output -> {
            MessageV1.writePartition(output, partition);
            output.writeInt(limit);
        }, WireValues::readPartition);
    }

    public GroupDirectory.View group(String group) throws IOException {
        return request(Opcode.GROUP, output -> Binary.writeText(output, group), WireValues::readGroup);
    }

    public DeadLetters.Page deadLetters(String group, long cursor, int limit) throws IOException {
        return request(Opcode.DEAD_LETTERS, output -> {
            Binary.writeText(output, group);
            output.writeLong(cursor);
            output.writeInt(limit);
        }, WireValues::readDeadLetters);
    }

    public int retire() throws IOException {
        return request(Opcode.RETIRE, EMPTY, DataInputStream::readInt);
    }

    public RetentionPreview retention(TopicPartition partition) throws IOException {
        return request(Opcode.RETENTION_PREVIEW, output -> MessageV1.writePartition(output, partition),
                WireValues::readRetention);
    }

    public FlowView flow(String group) throws IOException {
        return request(Opcode.FLOW, output -> Binary.writeText(output, group), WireValues::readFlow);
    }

    private <T> T request(Opcode opcode, Binary.Writer arguments, Binary.Reader<T> result)
            throws IOException {
        requests.lock();
        try {
            return exchange(opcode, welcome.maxFrame(), arguments, result);
        } catch (BrokerException response) {
            throw response;
        } catch (IOException transport) {
            try {
                socket.close();
            } catch (IOException close) {
                transport.addSuppressed(close);
            }
            throw transport;
        } finally {
            requests.unlock();
        }
    }

    private <T> T exchange(Opcode opcode, int frameLimit, Binary.Writer arguments, Binary.Reader<T> result)
            throws IOException {
        if (lastRequest == Long.MAX_VALUE) {
            throw new IOException("request id space exhausted");
        }
        byte[] body = Binary.encode(arguments);
        if (body.length > frameLimit - Frame.HEADER_BYTES) {
            throw new BrokerException(BrokerException.Code.CAPACITY, "request exceeds negotiated frame");
        }
        long request = ++lastRequest;
        new Frame(request, opcode.wire, body).write(out, frameLimit);
        Frame reply = Frame.read(in, frameLimit);
        if (reply == null) {
            throw new EOFException("connection closed before replying to " + opcode);
        }
        if (reply.requestId() != request) {
            throw new IOException("reply request id does not match request");
        }
        if (reply.opcode() == Opcode.ERROR) {
            throw Binary.decode(reply.body(), WireValues::readError);
        }
        if (reply.opcode() != opcode.response()) {
            throw new IOException("reply opcode does not match " + opcode);
        }
        try {
            return Binary.decode(reply.body(), result);
        } catch (IllegalArgumentException malformed) {
            throw new IOException("invalid reply body", malformed);
        }
    }

    @Override
    public void close() throws IOException {
        socket.close();
    }
}
