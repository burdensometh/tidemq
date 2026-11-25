package tidemq.wire;

import tidemq.Binary;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;

record Frame(long requestId, int opcode, byte[] body) {
    static final int HEADER_BYTES = Long.BYTES + 1;

    static Frame read(DataInputStream input, int maximum) throws IOException {
        int leading = input.read();
        if (leading < 0) {
            return null;
        }
        int length = (leading << 24) | (input.readUnsignedByte() << 16)
                | (input.readUnsignedByte() << 8) | input.readUnsignedByte();
        if (length < HEADER_BYTES || length > maximum) {
            throw new IOException("frame length outside negotiated bounds");
        }
        long requestId = input.readLong();
        if (requestId <= 0) {
            throw new IOException("request ids must be positive");
        }
        int opcode = input.readUnsignedByte();
        return new Frame(requestId, opcode, Binary.readExact(input, length - HEADER_BYTES));
    }

    void write(DataOutputStream out, int maximum) throws IOException {
        int length = HEADER_BYTES + body.length;
        if (length > maximum) {
            throw new IOException("outbound frame exceeds negotiated limit");
        }
        out.writeInt(length);
        out.writeLong(requestId);
        out.writeByte(opcode);
        out.write(body);
        out.flush();
    }
}
