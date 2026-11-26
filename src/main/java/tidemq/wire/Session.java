package tidemq.wire;

import tidemq.Binary;
import tidemq.Boundary;
import tidemq.Broker;
import tidemq.BrokerException;
import tidemq.Settings;
import tidemq.segment.MessageV1;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.net.Socket;

final class Session {
    private static final System.Logger LOG = System.getLogger(Session.class.getName());
    private static final byte[] EMPTY = new byte[0];

    private final long id;
    private final Socket socket;
    private final Broker broker;
    private final Settings settings;
    private final Boundary boundary;
    private int maxFrame = Settings.MIN_FRAME_BYTES;
    private boolean negotiated;
    private long lastRequest;
    private Broker.Member member;

    Session(long id, Socket socket, Broker broker, Settings settings, Boundary boundary) {
        this.id = id;
        this.socket = socket;
        this.broker = broker;
        this.settings = settings;
        this.boundary = boundary;
    }

    void run() {
        try (socket) {
            socket.setSoTimeout(settings.readTimeoutMs());
            socket.setTcpNoDelay(true);
            var in = new DataInputStream(new BufferedInputStream(socket.getInputStream()));
            var out = new DataOutputStream(new BufferedOutputStream(socket.getOutputStream()));
            Frame frame;
            while ((frame = Frame.read(in, maxFrame)) != null) {
                Frame response = dispatch(frame);
                if (response.opcode() != Opcode.ERROR) {
                    boundary.reached(Boundary.Point.FRAME_BEFORE_REPLY);
                }
                response.write(out, maxFrame);
                if (!negotiated || frame.requestId() < lastRequest) {
                    break;
                }
            }
        } catch (IOException ended) {
            LOG.log(System.Logger.Level.DEBUG, "connection " + id + " ended: " + ended.getMessage());
        } finally {
            if (member != null) {
                try {
                    broker.leave(member);
                } catch (IOException ended) {
                    LOG.log(System.Logger.Level.DEBUG, "cannot release membership for connection " + id, ended);
                }
            }
        }
    }

    private Frame dispatch(Frame frame) throws IOException {
        try {
            if (frame.requestId() <= lastRequest) {
                throw new BrokerException(BrokerException.Code.BAD_REQUEST, "request ids must increase");
            }
            lastRequest = frame.requestId();
            Opcode opcode = Opcode.find(frame.opcode());
            if (!negotiated && opcode != Opcode.HELLO) {
                throw new BrokerException(BrokerException.Code.BAD_REQUEST, "HELLO must be first");
            }
            byte[] result = execute(opcode, frame.body());
            if (result.length > room()) {
                throw new BrokerException(BrokerException.Code.CAPACITY, "response exceeds negotiated frame");
            }
            return new Frame(frame.requestId(), opcode.response(), result);
        } catch (BrokerException error) {
            return new Frame(frame.requestId(), Opcode.ERROR, WireValues.error(error));
        } catch (IllegalArgumentException invalid) {
            var error = new BrokerException(BrokerException.Code.BAD_REQUEST, invalid.getMessage());
            return new Frame(frame.requestId(), Opcode.ERROR, WireValues.error(error));
        }
    }

    private byte[] execute(Opcode opcode, byte[] body) throws IOException {
        return switch (opcode) {
            case HELLO -> hello(body);
            case PING -> {
                expectEmpty(body);
                yield EMPTY;
            }
            case PRODUCE -> produce(body);
            case JOIN -> join(body);
            case ASSIGN -> {
                expectEmpty(body);
                var epoch = broker.assignment(requireMember());
                yield Binary.encode(out -> WireValues.writeEpoch(out, epoch));
            }
            case PULL -> pull(body);
            case ACCEPT -> accept(body);
            case RENEW -> renew(body);
            case ACK -> acknowledge(body);
            case LEAVE -> {
                expectEmpty(body);
                broker.leave(requireMember());
                member = null;
                yield EMPTY;
            }
            case TOPICS -> {
                expectEmpty(body);
                var view = broker.topics();
                yield Binary.encode(out -> WireValues.writeTopics(out, view));
            }
            case PARTITION -> partition(body);
            case GROUP -> {
                String name = read(body, in -> WireValues.identifier(in, "group"));
                var view = broker.group(name);
                yield Binary.encode(out -> WireValues.writeGroup(out, view));
            }
            case DEAD_LETTERS -> deadLetters(body);
            case RETIRE -> {
                expectEmpty(body);
                int retired = broker.retire();
                yield Binary.encode(out -> out.writeInt(retired));
            }
            case RETENTION_PREVIEW -> {
                var partition = read(body, MessageV1::readPartition);
                var preview = broker.previewRetention(partition);
                yield Binary.encode(out -> WireValues.writeRetention(out, preview));
            }
            case FLOW -> {
                String name = read(body, input -> WireValues.identifier(input, "group"));
                int maximalName = Integer.BYTES + Settings.MAX_IDENTIFIER;
                int header = maximalName + 40;
                int maximalMember = maximalName + 20;
                int memberLimit = Math.min(WireValues.MAX_MEMBERS, (room() - header) / maximalMember);
                var view = broker.flow(name, memberLimit);
                yield Binary.encode(out -> WireValues.writeFlow(out, view));
            }
        };
    }

    private byte[] hello(byte[] body) throws IOException {
        if (negotiated) {
            throw new BrokerException(BrokerException.Code.BAD_REQUEST, "HELLO already completed");
        }
        var hello = read(body, in -> new WireValues.Hello(in.readInt(), in.readInt(), in.readInt()));
        if (hello.minVersion() < 1 || hello.maxVersion() < hello.minVersion()
                || hello.maxFrame() < Settings.MIN_FRAME_BYTES || hello.maxFrame() > Settings.MAX_FRAME_BYTES) {
            throw new BrokerException(BrokerException.Code.BAD_REQUEST, "invalid negotiation range or frame limit");
        }
        if (hello.minVersion() > WireValues.VERSION || hello.maxVersion() < WireValues.VERSION) {
            throw new BrokerException(BrokerException.Code.UNSUPPORTED_VERSION, "broker supports protocol v1");
        }
        maxFrame = Math.min(settings.frameBytes(), hello.maxFrame());
        negotiated = true;
        return Binary.encode(out -> {
            out.writeInt(WireValues.VERSION);
            out.writeInt(maxFrame);
            out.writeInt(settings.maxBatch());
        });
    }

    private byte[] produce(byte[] body) throws IOException {
        var produce = read(body, in -> WireValues.readPublication(in, settings.messageBytes()));
        var receipt = broker.produce(produce.producer(), produce.sequence(), produce.topic(),
                produce.partition(), produce.key(), produce.value());
        return Binary.encode(out -> WireValues.writeReceipt(out, receipt));
    }

    private byte[] join(byte[] body) throws IOException {
        var join = read(body, in -> new WireValues.Join(WireValues.identifier(in, "group"),
                WireValues.identifier(in, "member"), WireValues.readNames(in)));
        var candidate = new Broker.Member(join.group(), join.member(), id);
        if (member != null && !member.equals(candidate)) {
            throw new BrokerException(BrokerException.Code.GROUP_CONFLICT, "leave before joining another membership");
        }
        var topics = broker.topics();
        long maximumBytes = 20L + join.group().length() + join.member().length();
        for (String topic : join.topics()) {
            int partitions = topics.getOrDefault(topic, 0);
            maximumBytes += (8L + topic.length()) * partitions;
        }
        if (maximumBytes > room()) {
            throw new BrokerException(BrokerException.Code.CAPACITY, "frame cannot carry the full group assignment");
        }
        var epoch = broker.join(candidate, join.topics());
        member = candidate;
        return Binary.encode(out -> WireValues.writeEpoch(out, epoch));
    }

    private byte[] pull(byte[] body) throws IOException {
        var pull = read(body, in -> new WireValues.Pull(in.readLong(), in.readInt()));
        var leases = broker.pull(requireMember(), pull.epoch(), pull.limit(), room() - Integer.BYTES);
        return Binary.encode(out -> WireValues.writeLeases(out, leases));
    }

    private byte[] accept(byte[] body) throws IOException {
        var settlement = read(body, WireValues::readSettlement);
        if (WireValues.acceptanceSize(settlement.refs()) > room()) {
            throw new BrokerException(BrokerException.Code.CAPACITY, "acceptance receipts exceed negotiated frame");
        }
        var result = broker.accept(requireMember(), settlement.epoch(), settlement.refs());
        return Binary.encode(out -> WireValues.writeAcceptances(out, result));
    }

    private byte[] renew(byte[] body) throws IOException {
        var settlement = read(body, WireValues::readSettlement);
        if (WireValues.acceptanceSize(settlement.refs()) > room()) {
            throw new BrokerException(BrokerException.Code.CAPACITY, "renewal receipts exceed negotiated frame");
        }
        var result = broker.renew(requireMember(), settlement.epoch(), settlement.refs());
        return Binary.encode(out -> WireValues.writeAcceptances(out, result));
    }

    private byte[] acknowledge(byte[] body) throws IOException {
        var settlement = read(body, WireValues::readSettlement);
        var result = broker.ack(requireMember(), settlement.epoch(), settlement.refs());
        return Binary.encode(out -> WireValues.writeFrontiers(out, result));
    }

    private byte[] partition(byte[] body) throws IOException {
        var query = read(body, in -> new WireValues.PartitionQuery(MessageV1.readPartition(in), in.readInt()));
        if (query.limit() < 1 || query.limit() > WireValues.MAX_SEGMENTS) {
            throw new BrokerException(BrokerException.Code.BAD_REQUEST, "invalid segment view limit");
        }
        int base = 32 + query.partition().topic().length();
        int limit = Math.min(query.limit(), (room() - base) / 41);
        var view = broker.partition(query.partition(), limit);
        return Binary.encode(out -> WireValues.writePartition(out, view));
    }

    private byte[] deadLetters(byte[] body) throws IOException {
        var query = read(body, in -> new WireValues.DeadLetterQuery(
                WireValues.identifier(in, "group"), in.readLong(), in.readInt()));
        var page = broker.deadLetters(query.group(), query.cursor(), query.limit(), room() - 12);
        return Binary.encode(out -> WireValues.writeDeadLetters(out, page));
    }

    private Broker.Member requireMember() throws BrokerException {
        if (member == null) {
            throw new BrokerException(BrokerException.Code.GROUP_CONFLICT, "join a group first");
        }
        return member;
    }

    private int room() {
        return maxFrame - Frame.HEADER_BYTES;
    }

    private static void expectEmpty(byte[] body) throws BrokerException {
        if (body.length != 0) {
            throw new BrokerException(BrokerException.Code.BAD_REQUEST, "request must have an empty body");
        }
    }

    private static <T> T read(byte[] body, Binary.Reader<T> parser) throws BrokerException {
        try {
            return Binary.decode(body, parser);
        } catch (IOException | IllegalArgumentException malformed) {
            throw new BrokerException(BrokerException.Code.BAD_REQUEST, "malformed request body");
        }
    }
}
