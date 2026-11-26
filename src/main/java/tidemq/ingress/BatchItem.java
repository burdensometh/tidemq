package tidemq.ingress;

import java.util.Arrays;
import java.util.Objects;

/**
 * One key/payload pair in an ordered producer batch.
 */
public record BatchItem(byte[] key, byte[] payload) {
    public BatchItem {
        Objects.requireNonNull(key, "key");
        Objects.requireNonNull(payload, "payload");
        key = key.clone();
        payload = payload.clone();
    }

    public static BatchItem of(byte[] payload) {
        return new BatchItem(EMPTY, payload);
    }

    private static final byte[] EMPTY = new byte[0];

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
        return other instanceof BatchItem item
                && Arrays.equals(key, item.key)
                && Arrays.equals(payload, item.payload);
    }

    @Override
    public int hashCode() {
        return 31 * Arrays.hashCode(key) + Arrays.hashCode(payload);
    }
}
