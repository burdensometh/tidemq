package tidemq.segment;

import tidemq.Settings;

import java.util.Arrays;
import java.util.Objects;

public record Message(
        TopicPartition partition,
        long offset,
        long appendedAtMs,
        String producer,
        long sequence,
        byte[] key,
        byte[] payload) {
    public Message {
        Objects.requireNonNull(partition);
        Settings.requireIdentifier(producer, "producer");
        if (offset < 0 || offset == Long.MAX_VALUE || sequence < 0 || appendedAtMs < 0) {
            throw new IllegalArgumentException("invalid offset, sequence or append time");
        }
        if (key.length > Settings.MAX_KEY_BYTES) {
            throw new IllegalArgumentException("message key is too large");
        }
        key = key.clone();
        payload = payload.clone();
    }

    @Override
    public byte[] key() {
        return key.clone();
    }

    @Override
    public byte[] payload() {
        return payload.clone();
    }

    public int keySize() {
        return key.length;
    }

    public int payloadSize() {
        return payload.length;
    }

    @Override
    public boolean equals(Object other) {
        return other instanceof Message message
                && partition.equals(message.partition)
                && offset == message.offset
                && appendedAtMs == message.appendedAtMs
                && producer.equals(message.producer)
                && sequence == message.sequence
                && Arrays.equals(key, message.key)
                && Arrays.equals(payload, message.payload);
    }

    @Override
    public int hashCode() {
        int result = Objects.hash(partition, offset, appendedAtMs, producer, sequence);
        result = 31 * result + Arrays.hashCode(key);
        return 31 * result + Arrays.hashCode(payload);
    }
}
