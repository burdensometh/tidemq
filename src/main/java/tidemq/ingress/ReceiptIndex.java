package tidemq.ingress;

import tidemq.Binary;
import tidemq.Settings;
import tidemq.segment.Message;
import tidemq.segment.MessageV1;
import tidemq.segment.RecordFile;
import tidemq.segment.TopicPartition;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

final class ReceiptIndex {
    private static final int MAGIC = 0x544D5052; // TMPR
    private static final int HASH_BYTES = 32;

    record Identity(String producer, long sequence) {
        Identity {
            Settings.requireIdentifier(producer, "producer");
            if (sequence < 0) {
                throw new IllegalArgumentException("producer sequence must be nonnegative");
            }
        }
    }

    record Entry(ProduceReceipt receipt, byte[] hash) {
        Identity identity() {
            return new Identity(receipt.producer(), receipt.sequence());
        }
    }

    private final Map<Identity, Entry> entries = new HashMap<>();
    private final RecordFile file;

    ReceiptIndex(Path path, long now) throws IOException {
        if (Files.exists(path)) {
            file = RecordFile.open(path, MAGIC, 0, true, (position, bytes) -> {
                Entry entry = decode(bytes);
                if (entries.putIfAbsent(entry.identity(), entry) != null) {
                    throw new IOException("duplicate producer identity in receipt index at byte " + position);
                }
            });
        } else {
            file = RecordFile.create(path, new RecordFile.Header(MAGIC, 0, now));
        }
    }

    Entry lookup(Identity identity) {
        return entries.get(identity);
    }

    ProduceReceipt record(Message message, byte[] hash) throws IOException {
        var receipt = new ProduceReceipt(message.producer(), message.sequence(), message.partition(),
                message.offset(), message.appendedAtMs(), false);
        Entry entry = new Entry(receipt, hash);
        file.append(encode(entry), () -> {});
        entries.put(entry.identity(), entry);
        return receipt;
    }

    void reconcile(Message message) throws IOException {
        var identity = new Identity(message.producer(), message.sequence());
        byte[] hash = fingerprint(message.partition(), message.key(), message.payload());
        Entry entry = lookup(identity);
        if (entry == null) {
            record(message, hash);
            return;
        }
        ProduceReceipt receipt = entry.receipt();
        if (!receipt.partition().equals(message.partition()) || receipt.offset() != message.offset()
                || receipt.appendedAtMs() != message.appendedAtMs() || !Arrays.equals(entry.hash(), hash)) {
            throw new IOException("producer receipt disagrees with retained message " + identity);
        }
    }

    void verifyRanges(tidemq.segment.SegmentStore segments) throws IOException {
        for (Entry entry : entries.values()) {
            ProduceReceipt receipt = entry.receipt();
            if (receipt.offset() >= segments.range(receipt.partition()).nextOffset()) {
                throw new IOException("producer receipt points past end of " + receipt.partition());
            }
        }
    }

    static byte[] fingerprint(TopicPartition partition, byte[] key, byte[] payload) throws IOException {
        MessageDigest digest;
        try {
            digest = MessageDigest.getInstance("SHA-256");
        } catch (NoSuchAlgorithmException impossible) {
            throw new IllegalStateException("Java runtime is missing SHA-256", impossible);
        }
        byte[] prefix = Binary.encode(out -> {
            MessageV1.writePartition(out, partition);
            Binary.writeBytes(out, key);
        });
        digest.update(prefix);
        return digest.digest(payload);
    }

    private static byte[] encode(Entry entry) throws IOException {
        return Binary.encode(out -> {
            ProduceReceipt receipt = entry.receipt();
            Binary.writeText(out, receipt.producer());
            out.writeLong(receipt.sequence());
            MessageV1.writePartition(out, receipt.partition());
            out.writeLong(receipt.offset());
            out.writeLong(receipt.appendedAtMs());
            out.write(entry.hash());
        });
    }

    private static Entry decode(byte[] bytes) throws IOException {
        try {
            return Binary.decode(bytes, in -> {
                var identity = new Identity(Binary.readText(in, Settings.MAX_IDENTIFIER), in.readLong());
                TopicPartition partition = MessageV1.readPartition(in);
                long offset = in.readLong();
                long timestamp = in.readLong();
                if (offset < 0 || timestamp < 0) {
                    throw new IOException("negative producer receipt position or timestamp");
                }
                var receipt = new ProduceReceipt(identity.producer(), identity.sequence(), partition,
                        offset, timestamp, false);
                return new Entry(receipt, Binary.readExact(in, HASH_BYTES));
            });
        } catch (IllegalArgumentException invalid) {
            throw new IOException("invalid producer receipt", invalid);
        }
    }
}
