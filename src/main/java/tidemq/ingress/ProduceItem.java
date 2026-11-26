package tidemq.ingress;

/** Public key/payload item name used by producer batch call sites. */
public record ProduceItem(byte[] key, byte[] payload) {
    private static final byte[] EMPTY = new byte[0];

    public ProduceItem(byte[] payload) {
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
