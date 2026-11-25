package tidemq;

import tidemq.segment.Disk;

import java.io.IOException;
import java.nio.channels.FileChannel;
import java.nio.channels.FileLock;
import java.nio.channels.OverlappingFileLockException;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;

final class DataDirectory implements AutoCloseable {
    private final FileChannel channel;
    private final FileLock lock;

    private DataDirectory(FileChannel channel, FileLock lock) {
        this.channel = channel;
        this.lock = lock;
    }

    static DataDirectory claim(Path root) throws IOException {
        Disk.makeDirectory(root);
        var channel = FileChannel.open(root.resolve("broker.lock"),
                StandardOpenOption.CREATE, StandardOpenOption.WRITE);
        try {
            FileLock lock = channel.tryLock();
            if (lock == null) {
                throw new IOException("data directory already in use: " + root);
            }
            return new DataDirectory(channel, lock);
        } catch (OverlappingFileLockException busy) {
            channel.close();
            throw new IOException("data directory already in use: " + root, busy);
        } catch (IOException | RuntimeException failure) {
            try {
                channel.close();
            } catch (IOException close) {
                failure.addSuppressed(close);
            }
            throw failure;
        }
    }

    @Override
    public void close() throws IOException {
        try (channel) {
            lock.release();
        }
    }
}
