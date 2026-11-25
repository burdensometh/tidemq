package tidemq.wire;

import tidemq.Binary;
import tidemq.BrokerException;
import tidemq.Settings;
import tidemq.deadletter.DeadLetters;
import tidemq.delivery.DeliveryLease;
import tidemq.delivery.FlowView;
import tidemq.group.GroupDirectory;
import tidemq.group.GroupEpoch;
import tidemq.ingress.ProduceReceipt;
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
