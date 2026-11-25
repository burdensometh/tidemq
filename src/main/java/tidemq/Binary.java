package tidemq;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.EOFException;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.charset.CodingErrorAction;
import java.nio.charset.StandardCharsets;

public final class Binary {
    private Binary() {}

    @FunctionalInterface
    public interface Writer {
        void write(DataOutputStream out) throws IOException;
    }

    @FunctionalInterface
    public interface Reader<T> {
        T read(DataInputStream in) throws IOException;
    }

    public static byte[] encode(Writer writer) throws IOException {
        var bytes = new ByteArrayOutputStream();
        try (var out = new DataOutputStream(bytes)) {
            writer.write(out);
        }
        return bytes.toByteArray();
    }

    public static <T> T decode(byte[] bytes, Reader<T> reader) throws IOException {
        try (var in = new DataInputStream(new ByteArrayInputStream(bytes))) {
            T result = reader.read(in);
            if (in.read() != -1) {
                throw new IOException("unexpected trailing bytes");
            }
            return result;
        }
    }

    public static void writeText(DataOutputStream out, String text) throws IOException {
        byte[] bytes = text.getBytes(StandardCharsets.UTF_8);
        out.writeInt(bytes.length);
        out.write(bytes);
    }

    public static String readText(DataInputStream in, int maxBytes) throws IOException {
        byte[] bytes = readBytes(in, maxBytes);
        return StandardCharsets.UTF_8.newDecoder()
                .onMalformedInput(CodingErrorAction.REPORT)
                .onUnmappableCharacter(CodingErrorAction.REPORT)
                .decode(ByteBuffer.wrap(bytes)).toString();
    }

    public static void writeBytes(DataOutputStream out, byte[] bytes) throws IOException {
        out.writeInt(bytes.length);
        out.write(bytes);
    }

    public static byte[] readBytes(DataInputStream in, int maximum) throws IOException {
        return readExact(in, readCount(in, maximum));
    }

    public static byte[] readExact(DataInputStream in, int length) throws IOException {
        byte[] result = in.readNBytes(length);
        if (result.length != length) {
            throw new EOFException("truncated binary value");
        }
        return result;
    }

    public static int readCount(DataInputStream in, int maximum) throws IOException {
        int count = in.readInt();
        if (count < 0 || count > maximum) {
            throw new IOException("invalid binary length or count " + count);
        }
        return count;
    }
}
