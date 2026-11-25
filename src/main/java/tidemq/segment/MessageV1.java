package tidemq.segment;

import tidemq.Binary;
import tidemq.Settings;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;

public final class MessageV1 {
    public static final int MAX_PAYLOAD = 8 * 1024 * 1024;

    private MessageV1() {}

    public static void writePartition(DataOutputStream out, TopicPartition partition) throws IOException {
        Binary.writeText(out, partition.topic());
        out.writeInt(partition.partition());
    }

    public static TopicPartition readPartition(DataInputStream in) throws IOException {
        return new TopicPartition(Binary.readText(in, Settings.MAX_IDENTIFIER), in.readInt());
    }

    public static void write(DataOutputStream out, Message message) throws IOException {
        out.writeLong(message.offset());
        out.writeLong(message.appendedAtMs());
        Binary.writeText(out, message.producer());
        out.writeLong(message.sequence());
        Binary.writeBytes(out, message.key());
        Binary.writeBytes(out, message.payload());
    }

    public static Message read(DataInputStream in, TopicPartition partition) throws IOException {
        return new Message(partition, in.readLong(), in.readLong(),
                Binary.readText(in, Settings.MAX_IDENTIFIER), in.readLong(),
                Binary.readBytes(in, Settings.MAX_KEY_BYTES), Binary.readBytes(in, MAX_PAYLOAD));
    }

    public static int wireBodyBytes(Message message) {
        return 36 + message.producer().length() + message.keySize() + message.payloadSize();
    }
}
