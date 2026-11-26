package tidemq.deadletter;

import tidemq.Binary;
import tidemq.Boundary;
import tidemq.BrokerException;
import tidemq.Settings;
import tidemq.segment.Message;
import tidemq.segment.MessageV1;
import tidemq.segment.RecordFile;
import tidemq.segment.TopicPartition;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.NavigableMap;
import java.util.TreeMap;

public final class DeadLetters {
    private static final int MAGIC = 0x544D444C; // TMDL

    public record Source(String group, TopicPartition partition, long offset) {}
    public record Page(long nextSequence, List<DeadLetterReceipt> receipts) {}

    @FunctionalInterface
    public interface Visitor {
        void accept(DeadLetterReceipt receipt) throws IOException;
    }

    private final Boundary boundary;
    private final RecordFile file;
    private final Map<Source, Long> bySource = new HashMap<>();
    private final Map<String, NavigableMap<Long, Long>> byGroup = new HashMap<>();
    private long nextSequence;

    public DeadLetters(Path dir, Boundary boundary, long now) throws IOException {
        this.boundary = boundary;
        Path path = dir.resolve("deadletter.v1.journal");
        if (Files.exists(path)) {
            file = RecordFile.open(path, MAGIC, 0, true, (position, body) -> index(position, decode(body)));
        } else {
            file = RecordFile.create(path, new RecordFile.Header(MAGIC, 0, now));
        }
    }

    private void index(long position, DeadLetterReceipt receipt) throws IOException {
        Message message = receipt.message();
        Source source = new Source(receipt.group(), message.partition(), message.offset());
        if (receipt.sequence() != nextSequence || bySource.containsKey(source) || nextSequence == Long.MAX_VALUE) {
            throw new IOException("duplicate or out-of-sequence dead-letter record at byte " + position);
        }
        bySource.put(source, position);
        byGroup.computeIfAbsent(receipt.group(), key -> new TreeMap<>()).put(receipt.sequence(), position);
        nextSequence++;
    }

    public DeadLetterReceipt archive(String group, Message message, int attempts, long now) throws IOException {
        Source source = new Source(group, message.partition(), message.offset());
        Long position = bySource.get(source);
        if (position != null) {
            return decode(file.read(position));
        }
        if (nextSequence == Long.MAX_VALUE) {
            throw new IOException("dead-letter sequence exhausted");
        }
        var receipt = new DeadLetterReceipt(nextSequence, group, attempts, now, message);
        long appended = file.append(encode(receipt), () -> boundary.reached(Boundary.Point.DEADLETTER_BEFORE_FORCE));
        index(appended, receipt);
        boundary.reached(Boundary.Point.DEADLETTER_AFTER_FORCE);
        return receipt;
    }

    public Page read(String group, long from, int limit, int byteBudget) throws IOException {
        Settings.requireIdentifier(group, "group");
        if (from < 0 || limit < 1 || limit > 256 || byteBudget < 0) {
            throw new BrokerException(BrokerException.Code.BAD_REQUEST, "invalid dead-letter page");
        }
        var groupIndex = byGroup.get(group);
        if (groupIndex == null) {
            return new Page(Math.max(from, nextSequence), List.of());
        }
        var results = new ArrayList<DeadLetterReceipt>();
        long cursor = Math.max(from, nextSequence);
        try (RecordFile.Reader reader = file.reader()) {
            for (var entry : groupIndex.tailMap(from, true).entrySet()) {
                if (results.size() == limit) {
                    cursor = entry.getKey();
                    break;
                }
                DeadLetterReceipt receipt = decode(reader.read(entry.getValue()));
                int encodedBytes = receipt.wireBytes();
                if (encodedBytes > byteBudget) {
                    if (results.isEmpty()) {
                        throw new BrokerException(BrokerException.Code.CAPACITY,
                                "dead-letter record exceeds negotiated frame");
                    }
                    cursor = entry.getKey();
                    break;
                }
                results.add(receipt);
                byteBudget -= encodedBytes;
            }
        }
        return new Page(cursor, List.copyOf(results));
    }

    public void forEach(Visitor visitor) throws IOException {
        file.forEach((position, body) -> visitor.accept(decode(body)));
    }

    public static void write(java.io.DataOutputStream out, DeadLetterReceipt receipt) throws IOException {
        out.writeLong(receipt.sequence());
        Binary.writeText(out, receipt.group());
        out.writeInt(receipt.attempts());
        out.writeLong(receipt.terminalAtMs());
        MessageV1.writePartition(out, receipt.message().partition());
        MessageV1.write(out, receipt.message());
    }

    public static DeadLetterReceipt read(java.io.DataInputStream in) throws IOException {
        long sequence = in.readLong();
        String group = Binary.readText(in, Settings.MAX_IDENTIFIER);
        int attempts = in.readInt();
        long terminalAt = in.readLong();
        TopicPartition partition = MessageV1.readPartition(in);
        return new DeadLetterReceipt(sequence, group, attempts, terminalAt, MessageV1.read(in, partition));
    }

    private static DeadLetterReceipt decode(byte[] bytes) throws IOException {
        try {
            return Binary.decode(bytes, DeadLetters::read);
        } catch (IllegalArgumentException invalid) {
            throw new IOException("invalid persisted dead-letter record", invalid);
        }
    }

    private static byte[] encode(DeadLetterReceipt receipt) throws IOException {
        return Binary.encode(out -> write(out, receipt));
    }
}
