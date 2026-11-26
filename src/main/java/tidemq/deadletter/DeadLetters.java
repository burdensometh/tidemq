package tidemq.deadletter;

import tidemq.Binary;
import tidemq.Boundary;
import tidemq.BrokerException;
import tidemq.Settings;
import tidemq.segment.Disk;
import tidemq.segment.Message;
import tidemq.segment.MessageV1;
import tidemq.segment.RecordFile;
import tidemq.segment.TopicPartition;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.NavigableMap;
import java.util.TreeMap;

/**
 * Durable dead-letter store.
 *
 * <p>Metadata and payload live in separate append logs. A
 * {@code deadletter.v2.meta} record holds the full receipt metadata plus a
 * reference into {@code deadletter.v2.data}; the payload log stores the raw key
 * and payload bytes. A page can therefore be built, budgeted and advanced by
 * reading metadata alone, and a payload is fetched only when its row is
 * actually returned.
 *
 * <p>An existing {@code deadletter.v1.journal} is migrated losslessly on open.
 * Migration preserves sequence numbers, sources, attempt counts, terminal
 * timestamps and payload bytes, and is resumable: a partially copied prefix is
 * compared record-for-record against the v1 log and only the missing suffix is
 * appended. The v1 log is removed only after the v2 records and a durable
 * completion marker are in place, so a crash at any point leaves a prefix that
 * the next open continues from.
 */
public final class DeadLetters implements AutoCloseable {
    private static final int MAGIC_V1 = 0x544D444C;   // TMDL
    private static final int MAGIC_META = 0x544D444D; // TMDM
    private static final int MAGIC_DATA = 0x544D4444; // TMDD
    public static final String V1_FILE = "deadletter.v1.journal";
    public static final String META_FILE = "deadletter.v2.meta";
    public static final String DATA_FILE = "deadletter.v2.data";
    private static final String MARKER_FILE = "deadletter.v2.complete";
    private static final String MARKER_TEXT = "TideMQ deadletter v2\n";

    public record Source(String group, TopicPartition partition, long offset) {}
    public record Page(long nextSequence, List<DeadLetterReceipt> receipts) {}

    @FunctionalInterface
    public interface Visitor {
        void accept(DeadLetterReceipt receipt) throws IOException;
    }

    /**
     * Metadata for one dead letter. Payload bytes are not part of it; they live
     * at {@code dataPosition} in the payload log with the recorded lengths and
     * checksum.
     */
    private record Meta(
            long sequence,
            String group,
            int attempts,
            long terminalAtMs,
            TopicPartition partition,
            long messageOffset,
            long appendedAtMs,
            String producer,
            long producerSequence,
            int keyLength,
            int payloadLength,
            long dataPosition,
            int dataChecksum) {

        Meta {
            Settings.requireIdentifier(group, "group");
            Settings.requireIdentifier(producer, "producer");
            if (sequence < 0 || attempts < 1 || attempts > 1000 || terminalAtMs < 0
                    || messageOffset < 0 || appendedAtMs < 0 || producerSequence < 0
                    || keyLength < 0 || keyLength > Settings.MAX_KEY_BYTES
                    || payloadLength < 0 || payloadLength > MessageV1.MAX_PAYLOAD
                    || dataPosition < RecordFile.HEADER_BYTES) {
                throw new IllegalArgumentException("invalid dead-letter metadata");
            }
        }

        Source source() {
            return new Source(group, partition, messageOffset);
        }

        /** Wire size of the equivalent receipt, computed without reading the payload. */
        int wireBytes() {
            return 8 + 4 + group.length() + 4 + 8 + 8 + partition.topic().length()
                    + MessageV1.wireBodyBytes(producer.length(), keyLength, payloadLength);
        }    }

    private final Path directory;
    private final Boundary boundary;
    private final RecordFile meta;
    private final RecordFile data;
    private final Map<Source, Long> bySource = new HashMap<>();
    private final Map<String, NavigableMap<Long, Long>> byGroup = new HashMap<>();
    private final NavigableMap<Long, Long> bySequence = new TreeMap<>();
    private long nextSequence;

    public DeadLetters(Path dir, Boundary boundary, long now) throws IOException {
        this.directory = dir;
        this.boundary = boundary;
        Path dataPath = dir.resolve(DATA_FILE);
        Path metaPath = dir.resolve(META_FILE);
        data = Files.exists(dataPath)
                ? RecordFile.open(dataPath, MAGIC_DATA, 0, true, (position, body) -> {})
                : RecordFile.create(dataPath, new RecordFile.Header(MAGIC_DATA, 0, now));
        meta = Files.exists(metaPath)
                ? RecordFile.open(metaPath, MAGIC_META, 0, true,
                        (position, body) -> index(position, decodeMeta(body)))
                : RecordFile.create(metaPath, new RecordFile.Header(MAGIC_META, 0, now));
        verifyJoint();
        migrateIfNeeded();
    }

    /** Confirms every metadata row can be read together with its payload. */
    private void verifyJoint() throws IOException {
        for (long position : bySequence.values()) {
            readMessage(decodeMeta(meta.read(position)));
        }
    }

    private void migrateIfNeeded() throws IOException {
        Path legacy = directory.resolve(V1_FILE);
        if (Files.exists(legacy)) {
            migrate(legacy);
            writeMarker();
            Files.deleteIfExists(legacy);
            Disk.forceDirectory(directory);
        } else if (!Files.exists(directory.resolve(MARKER_FILE))) {
            writeMarker();
        }
    }

    /**
     * Copies a v1 journal into the v2 store. Safe to call repeatedly: rows
     * already present in v2 are compared for full equality and the remaining
     * suffix is appended in source order.
     */
    private void migrate(Path legacy) throws IOException {
        var seen = new long[] {0};
        RecordFile.open(legacy, MAGIC_V1, 0, true, (position, body) -> {
            DeadLetterReceipt receipt = decodeV1(body, position);
            if (receipt.sequence() != seen[0]) {
                throw new IOException("v1 dead-letter sequence is not contiguous at byte " + position);
            }
            if (receipt.sequence() < nextSequence) {
                Long existing = bySequence.get(receipt.sequence());
                if (existing == null || !toReceipt(decodeMeta(meta.read(existing))).equals(receipt)) {
                    throw new IOException("v2 dead-letter disagrees with v1 at sequence " + receipt.sequence());
                }
            } else if (receipt.sequence() == nextSequence) {
                if (bySource.containsKey(receipt.source())) {
                    throw new IOException("duplicate v1 dead-letter source " + receipt.source());
                }
                append(receipt);
            } else {
                throw new IOException("v2 dead letters are missing the prefix before sequence " + receipt.sequence());
            }
            seen[0]++;
        });
        if (nextSequence > seen[0]) {
            throw new IOException("v2 dead-letter store has records not present in v1");
        }
    }

    private void writeMarker() throws IOException {
        Disk.replace(directory.resolve(MARKER_FILE), MARKER_TEXT.getBytes(StandardCharsets.UTF_8), () -> {});
    }

    private void index(long position, Meta record) throws IOException {
        Source source = record.source();
        if (record.sequence() != nextSequence || bySource.containsKey(source)
                || nextSequence == Long.MAX_VALUE) {
            throw new IOException("duplicate or out-of-sequence dead-letter metadata at byte " + position);
        }
        requireReadable(record);
        bySource.put(source, position);
        byGroup.computeIfAbsent(record.group(), key -> new TreeMap<>()).put(record.sequence(), position);
        bySequence.put(record.sequence(), position);
        nextSequence++;
    }

    /** Cheap bound check that the referenced payload record fully exists in the data log. */
    private void requireReadable(Meta record) throws IOException {
        long needed = (long) RecordFile.RECORD_OVERHEAD + Integer.BYTES + record.keyLength() + record.payloadLength();
        long position = record.dataPosition();
        if (position < RecordFile.HEADER_BYTES || position > data.size() || needed > data.size() - position) {
            throw new IOException("dead-letter payload reference outside data log at sequence " + record.sequence());
        }
    }

    public DeadLetterReceipt archive(String group, Message message, int attempts, long now) throws IOException {
        Source source = new Source(group, message.partition(), message.offset());
        Long position = bySource.get(source);
        if (position != null) {
            return toReceipt(decodeMeta(meta.read(position)));
        }
        if (nextSequence == Long.MAX_VALUE) {
            throw new IOException("dead-letter sequence exhausted");
        }
        var receipt = new DeadLetterReceipt(nextSequence, group, attempts, now, message);
        append(receipt);
        return receipt;
    }

    /**
     * Forces the payload, then forces the metadata, then reads the pair back
     * through the normal lookup path. The caller completes the group offset
     * only after this returns, so an interrupted write can never complete an
     * offset without a jointly readable dead letter.
     */
    private void append(DeadLetterReceipt receipt) throws IOException {
        Message message = receipt.message();
        byte[] payloadBody = encodePayload(message);
        int checksum = Disk.checksum(payloadBody);
        long dataPosition = data.append(payloadBody,
                () -> boundary.reached(Boundary.Point.DEADLETTER_BEFORE_FORCE));
        boundary.reached(Boundary.Point.DEADLETTER_AFTER_PAYLOAD_FORCE);
        var record = new Meta(receipt.sequence(), receipt.group(), receipt.attempts(), receipt.terminalAtMs(),
                message.partition(), message.offset(), message.appendedAtMs(), message.producer(), message.sequence(),
                message.keySize(), message.payloadSize(), dataPosition, checksum);
        long metaPosition = meta.append(encodeMeta(record), () -> {});
        Meta persisted = decodeMeta(meta.read(metaPosition));
        if (!toReceipt(persisted).equals(receipt)) {
            throw new IOException("dead-letter metadata and payload are not jointly readable at sequence "
                    + receipt.sequence());
        }
        index(metaPosition, persisted);
        boundary.reached(Boundary.Point.DEADLETTER_AFTER_FORCE);
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
        for (var entry : groupIndex.tailMap(from, true).entrySet()) {
            if (results.size() == limit) {
                cursor = entry.getKey();
                break;
            }
            Meta record = decodeMeta(meta.read(entry.getValue()));
            int encodedBytes = record.wireBytes();
            if (encodedBytes > byteBudget) {
                if (results.isEmpty()) {
                    throw new BrokerException(BrokerException.Code.CAPACITY, "dead-letter record exceeds negotiated frame");
                }
                cursor = entry.getKey();
                break;
            }
            results.add(toReceipt(record));
            byteBudget -= encodedBytes;
        }
        return new Page(cursor, List.copyOf(results));
    }

    public void forEach(Visitor visitor) throws IOException {
        for (long position : bySequence.values()) {
            visitor.accept(toReceipt(decodeMeta(meta.read(position))));
        }
    }

    private DeadLetterReceipt toReceipt(Meta record) throws IOException {
        Message message = readMessage(record);
        return new DeadLetterReceipt(record.sequence(), record.group(), record.attempts(),
                record.terminalAtMs(), message);
    }

    private Message readMessage(Meta record) throws IOException {
        byte[] body = data.read(record.dataPosition());
        if (Disk.checksum(body) != record.dataChecksum()) {
            throw new IOException("dead-letter payload checksum mismatch at byte " + record.dataPosition());
        }
        try {
            return Binary.decode(body, in -> {
                if (in.readInt() != record.keyLength()) {
                    throw new IOException("dead-letter key length disagrees with metadata");
                }
                byte[] key = Binary.readExact(in, record.keyLength());
                byte[] payload = Binary.readExact(in, record.payloadLength());
                return new Message(record.partition(), record.messageOffset(), record.appendedAtMs(),
                        record.producer(), record.producerSequence(), key, payload);
            });
        } catch (IllegalArgumentException invalid) {
            throw new IOException("invalid persisted dead-letter payload", invalid);
        }
    }

    private static byte[] encodePayload(Message message) throws IOException {
        return Binary.encode(out -> {
            out.writeInt(message.keySize());
            out.write(message.key());
            out.write(message.payload());
        });
    }

    private static byte[] encodeMeta(Meta record) throws IOException {
        return Binary.encode(out -> {
            out.writeLong(record.sequence());
            Binary.writeText(out, record.group());
            out.writeInt(record.attempts());
            out.writeLong(record.terminalAtMs());
            MessageV1.writePartition(out, record.partition());
            out.writeLong(record.messageOffset());
            out.writeLong(record.appendedAtMs());
            Binary.writeText(out, record.producer());
            out.writeLong(record.producerSequence());
            out.writeInt(record.keyLength());
            out.writeInt(record.payloadLength());
            out.writeLong(record.dataPosition());
            out.writeInt(record.dataChecksum());
        });
    }

    private static Meta decodeMeta(byte[] bytes) throws IOException {
        try {
            return Binary.decode(bytes, in -> new Meta(
                    in.readLong(),
                    Binary.readText(in, Settings.MAX_IDENTIFIER),
                    in.readInt(),
                    in.readLong(),
                    MessageV1.readPartition(in),
                    in.readLong(),
                    in.readLong(),
                    Binary.readText(in, Settings.MAX_IDENTIFIER),
                    in.readLong(),
                    in.readInt(),
                    in.readInt(),
                    in.readLong(),
                    in.readInt()));
        } catch (IllegalArgumentException invalid) {
            throw new IOException("invalid persisted dead-letter metadata", invalid);
        }
    }

    private static DeadLetterReceipt decodeV1(byte[] bytes, long position) throws IOException {
        try {
            return Binary.decode(bytes, DeadLetters::read);
        } catch (IllegalArgumentException invalid) {
            throw new IOException("invalid persisted v1 dead-letter record at byte " + position, invalid);
        }
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

    @Override
    public void close() {
        // Both logs are opened per operation; nothing is held open between calls.
    }
}
