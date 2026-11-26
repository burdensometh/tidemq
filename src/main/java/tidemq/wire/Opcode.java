package tidemq.wire;

import tidemq.BrokerException;

import java.util.HashMap;
import java.util.Map;

enum Opcode {
    HELLO(1),
    PING(2),
    PRODUCE(3),
    JOIN(4),
    ASSIGN(5),
    PULL(6),
    ACCEPT(7),
    ACK(8),
    LEAVE(9),
    TOPICS(10),
    PARTITION(11),
    GROUP(12),
    DEAD_LETTERS(13),
    RETIRE(14),
    RETENTION_PREVIEW(15),
    FLOW(16),
    PRODUCE_BATCH(17);

    static final int SUCCESS = 0x80;
    static final int ERROR = 0x7f;
    private static final Map<Integer, Opcode> LOOKUP = new HashMap<>();

    static {
        for (Opcode opcode : values()) {
            LOOKUP.put(opcode.wire, opcode);
        }
    }

    final int wire;

    Opcode(int wire) {
        this.wire = wire;
    }

    int response() {
        return wire | SUCCESS;
    }

    static Opcode find(int value) throws BrokerException {
        Opcode opcode = LOOKUP.get(value);
        if (opcode == null) {
            throw new BrokerException(BrokerException.Code.BAD_REQUEST, "unknown request opcode");
        }
        return opcode;
    }
}
