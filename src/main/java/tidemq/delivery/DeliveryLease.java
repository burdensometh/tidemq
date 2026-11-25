package tidemq.delivery;

import tidemq.segment.Message;
import tidemq.segment.MessageV1;
import tidemq.segment.TopicPartition;

public record DeliveryLease(long id, long epoch, long reserveUntilMs, int attempt, Message message) {
    public record Ref(long id, TopicPartition partition, long offset) {}
    public record Acceptance(Ref ref, long expiresAtMs) {}

    public Ref ref() {
        return new Ref(id, message.partition(), message.offset());
    }

    public int wireBytes() {
        return 28 + 8 + message.partition().topic().length() + MessageV1.wireBodyBytes(message);
    }
}
