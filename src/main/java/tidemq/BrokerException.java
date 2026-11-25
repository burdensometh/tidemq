package tidemq;

import java.io.IOException;

public final class BrokerException extends IOException {
    public enum Code {
        BAD_REQUEST(1),
        UNSUPPORTED_VERSION(2),
        UNKNOWN_TOPIC(3),
        PRODUCER_CONFLICT(4),
        GROUP_CONFLICT(5),
        STALE_EPOCH(6),
        UNKNOWN_LEASE(7),
        CAPACITY(8),
        STORAGE_FAILURE(9);

        private final int wire;

        Code(int wire) {
            this.wire = wire;
        }

        public int wire() {
            return wire;
        }
    }

    private final Code code;

    public BrokerException(Code code, String message) {
        super(message);
        this.code = code;
    }

    public Code code() {
        return code;
    }
}
