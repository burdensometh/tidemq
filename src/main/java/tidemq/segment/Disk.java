package tidemq.segment;

import java.io.EOFException;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.nio.file.StandardOpenOption;
import java.util.zip.CRC32C;

public final class Disk {
    private Disk() {}

    @FunctionalInterface
    public interface Commit {
        void apply() throws IOException;
    }

    public static int checksum(byte[] bytes) {
        return checksum(bytes, 0, bytes.length);
    }

    public static int checksum(byte[] bytes, int offset, int length) {
        CRC32C crc = new CRC32C();
        crc.update(bytes, offset, length);
        return (int) crc.getValue();
    }

    public static ByteBuffer read(FileChannel file, long position, int length) throws IOException {
        ByteBuffer bytes = ByteBuffer.allocate(length);
        while (bytes.hasRemaining()) {
            int consumed = file.read(bytes, position + bytes.position());
            if (consumed < 0) {
                throw new EOFException("truncated file at byte " + position);
            }
        }
        return bytes.flip();
    }

    public static void write(FileChannel file, ByteBuffer bytes) throws IOException {
        while (bytes.hasRemaining()) {
            file.write(bytes);
        }
    }

    public static void forceDirectory(Path directory) throws IOException {
        try (FileChannel channel = FileChannel.open(directory, StandardOpenOption.READ)) {
            channel.force(true);
        }
    }

    public static void makeDirectory(Path directory) throws IOException {
        if (Files.isDirectory(directory)) {
            return;
        }
        Path parent = directory.getParent();
        if (parent != null) {
            makeDirectory(parent);
        }
        try {
            Files.createDirectory(directory);
        } catch (java.nio.file.FileAlreadyExistsException exists) {
            if (!Files.isDirectory(directory)) {
                throw exists;
            }
        }
        if (parent != null) {
            forceDirectory(parent);
        }
    }

    public static void replace(Path path, byte[] bytes, Commit beforeRename) throws IOException {
        Path pending = Files.createTempFile(path.getParent(), ".replace-", ".tmp");
        try {
            try (FileChannel file = FileChannel.open(pending, StandardOpenOption.WRITE)) {
                write(file, ByteBuffer.wrap(bytes));
                file.force(true);
            }
            beforeRename.apply();
            Files.move(pending, path, StandardCopyOption.ATOMIC_MOVE, StandardCopyOption.REPLACE_EXISTING);
            forceDirectory(path.getParent());
        } finally {
            Files.deleteIfExists(pending);
        }
    }
}
