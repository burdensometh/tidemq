package tidemq.ingress;

/** Ordered key/payload entry in a producer batch. */
public record BatchMessage(byte[] key, byte[] payload) {
    private static final byte[] EMPTY = new byte[0];

    public BatchMessage(byte[] payload) {
        this(EMPTY, payload);
    }

    @Override
    public byte[] key() {
        return key.clone();
    }

    @Override
    public byte[] payload() {
        return payload.clone();
    }
}
