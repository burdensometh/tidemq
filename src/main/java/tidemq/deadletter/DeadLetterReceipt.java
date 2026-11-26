package tidemq.deadletter;

import tidemq.Settings;
import tidemq.segment.Message;

public record DeadLetterReceipt(long sequence, String group, int attempts, long terminalAtMs, Message message) {
    public DeadLetterReceipt {
        Settings.requireIdentifier(group, "group");
        if (sequence < 0 || attempts < 1 || attempts > 1000 || terminalAtMs < 0) {
            throw new IllegalArgumentException("invalid dead-letter receipt");
        }
    }

    /** The group partition offset this dead letter terminates. */
    public DeadLetters.Source source() {
        return new DeadLetters.Source(group, message.partition(), message.offset());
    }

    public int wireBytes() {
        return 8 + 4 + group.length() + 4 + 8 + 8 + message.partition().topic().length()
                + tidemq.segment.MessageV1.wireBodyBytes(message);
    }
}
