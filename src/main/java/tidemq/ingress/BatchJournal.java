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
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Intent/commit records for atomic producer batches.
 *
 * <p>An intent is forced before segment writes. Recovery can therefore tell a
 * complete batch from a partial prefix: complete matching bytes are committed,
 * while a partial/mismatching tail is truncated back to the planned base.
 */
final class BatchJournal {
    static final String FILE_NAME = "produce-batch.v1.journal";
    private static final int MAGIC = 0x544D4231; // TMB1
    private static final int HASH_BYTES = 32;
    private static final int MAX_RECORD_COUNT = 256;
    private static final byte INTENT = 1;
    private static final byte COMMIT = 2;

    record Stored(TopicPartition partition, long baseOffset, int count, long appendedAtMs,
                  byte[] hash, boolean committed) {
        Stored {
            hash = hash.clone();
        }

        @Override
        public byte[] hash() {
            return hash.clone();
        }
    }

    private final Map<ReceiptIndex.Identity, Stored> entries = new HashMap<>();
    private final RecordFile file;

    BatchJournal(Path path, long now) throws IOException {
        if (Files.exists(path)) {
            file = RecordFile.open(path, MAGIC, 0, true, (position, bytes) -> {
                var decoded = decode(bytes);
                merge(decoded.identity(), decoded.stored(), position);
            });
        } else {
            file = RecordFile.create(path, new RecordFile.Header(MAGIC, 0, now));
        }
    }

    Stored lookup(ReceiptIndex.Identity identity) {
        return entries.get(identity);
    }

    BatchReceipt committedReceipt(ReceiptIndex.Identity identity) {
        Stored stored = entries.get(identity);
        if (stored == null || !stored.committed()) {
            return null;
        }
        return toReceipt(identity, stored, false);
    }

    BatchReceipt replay(ReceiptIndex.Identity identity) {
        BatchReceipt receipt = committedReceipt(identity);
        return receipt == null ? null : receipt.asReplay();
    }

    boolean covers(TopicPartition partition, long offset) {
        for (Stored stored : entries.values()) {
            if (stored.committed() && stored.partition().equals(partition)
                    && offset >= stored.baseOffset() && offset < stored.baseOffset() + stored.count()) {
                return true;
            }
        }
        return false;
    }

    void recordIntent(ReceiptIndex.Identity identity, TopicPartition partition, long baseOffset,
                      int count, long appendedAtMs, byte[] hash) throws IOException {
        Stored stored = new Stored(partition, baseOffset, count, appendedAtMs, hash, false);
        file.append(encode(INTENT, identity, stored), () -> {});
        entries.put(identity, stored);
    }

    BatchReceipt commit(ReceiptIndex.Identity identity, Stored intent) throws IOException {
        Stored committed = new Stored(intent.partition(), intent.baseOffset(), intent.count(),
                intent.appendedAtMs(), intent.hash(), true);
        file.append(encode(COMMIT, identity, committed), () -> {});
        entries.put(identity, committed);
        return toReceipt(identity, committed, false);
    }

    BatchReceipt commitExisting(ReceiptIndex.Identity identity) throws IOException {
        Stored stored = entries.get(identity);
        if (stored == null) {
            throw new IOException("missing batch intent for " + identity);
        }
        if (stored.committed()) {
            return toReceipt(identity, stored, false);
        }
        return commit(identity, stored);
    }

    List<Map.Entry<ReceiptIndex.Identity, Stored>> pending() {
        return records().stream().filter(entry -> !entry.getValue().committed()).toList();
    }

    List<Map.Entry<ReceiptIndex.Identity, Stored>> records() {
        return List.copyOf(entries.entrySet());
    }

    private void merge(ReceiptIndex.Identity identity, Stored stored, long position) throws IOException {
        Stored previous = entries.get(identity);
        if (previous == null) {
            entries.put(identity, stored);
            return;
        }
        if (!sameIntent(previous, stored)) {
            throw new IOException("conflicting batch journal records for " + identity
                    + " at byte " + position);
        }
        if (stored.committed()) {
            entries.put(identity, stored);
        }
    }

    private static boolean sameIntent(Stored left, Stored right) {
        return left.partition().equals(right.partition())
                && left.baseOffset() == right.baseOffset()
                && left.count() == right.count()
                && left.appendedAtMs() == right.appendedAtMs()
                && MessageDigest.isEqual(left.hash(), right.hash());
    }

    static byte[] fingerprint(TopicPartition partition, List<BatchItem> items) throws IOException {
        byte[] body = Binary.encode(out -> {
            MessageV1.writePartition(out, partition);
            out.writeInt(items.size());
            for (BatchItem item : items) {
                Binary.writeBytes(out, item.key());
                Binary.writeBytes(out, item.payload());
            }
        });
        return sha256(body);
    }

    static byte[] fingerprintMessages(List<Message> messages) throws IOException {
        if (messages.isEmpty()) {
            throw new IllegalArgumentException("batch fingerprint requires messages");
        }
        TopicPartition partition = messages.getFirst().partition();
        byte[] body = Binary.encode(out -> {
            MessageV1.writePartition(out, partition);
            out.writeInt(messages.size());
            for (Message message : messages) {
                if (!partition.equals(message.partition())) {
                    throw new IOException("batch spans partitions");
                }
                Binary.writeBytes(out, message.key());
                Binary.writeBytes(out, message.payload());
            }
        });
        return sha256(body);
    }

    private static byte[] sha256(byte[] body) {
        try {
            return MessageDigest.getInstance("SHA-256").digest(body);
        } catch (NoSuchAlgorithmException impossible) {
            throw new IllegalStateException("Java runtime is missing SHA-256", impossible);
        }
    }

    private static BatchReceipt toReceipt(ReceiptIndex.Identity identity, Stored stored, boolean replayed) {
        return new BatchReceipt(identity.producer(), identity.sequence(), stored.partition(),
                stored.baseOffset(), stored.count(), stored.appendedAtMs(), replayed);
    }

    private static byte[] encode(byte status, ReceiptIndex.Identity identity, Stored stored) throws IOException {
        return Binary.encode(out -> {
            out.writeByte(status);
            Binary.writeText(out, identity.producer());
            out.writeLong(identity.sequence());
            MessageV1.writePartition(out, stored.partition());
            out.writeLong(stored.baseOffset());
            out.writeInt(stored.count());
            out.writeLong(stored.appendedAtMs());
            out.write(stored.hash());
        });
    }

    private record Decoded(ReceiptIndex.Identity identity, Stored stored) {}

    private static Decoded decode(byte[] bytes) throws IOException {
        try {
            return Binary.decode(bytes, in -> {
                byte status = in.readByte();
                if (status != INTENT && status != COMMIT) {
                    throw new IOException("invalid batch journal status " + status);
                }
                var identity = new ReceiptIndex.Identity(
                        Binary.readText(in, Settings.MAX_IDENTIFIER), in.readLong());
                TopicPartition partition = MessageV1.readPartition(in);
                long baseOffset = in.readLong();
                int count = Binary.readCount(in, MAX_RECORD_COUNT);
                long appendedAtMs = in.readLong();
                byte[] hash = Binary.readExact(in, HASH_BYTES);
                if (baseOffset < 0 || count <= 0 || appendedAtMs < 0) {
                    throw new IOException("invalid batch position, count or timestamp");
                }
                var stored = new Stored(partition, baseOffset, count, appendedAtMs, hash, status == COMMIT);
                return new Decoded(identity, stored);
            });
        } catch (IllegalArgumentException invalid) {
            throw new IOException("invalid producer batch record", invalid);
        }
    }
}
