package tidemq;

import tidemq.segment.Disk;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Arrays;

final class StorageLayout {
    private static final String MARKER = "storage.v1";
    private static final byte[] VERSION = "TideMQ broker storage v1\n".getBytes(StandardCharsets.UTF_8);

    private final Path root;
    private final boolean initialized;

    StorageLayout(Path root) throws IOException {
        this.root = root;
        Path marker = root.resolve(MARKER);
        initialized = Files.exists(marker);
        if (!initialized) {
            return;
        }
        if (!Files.isRegularFile(marker) || Files.size(marker) != VERSION.length
                || !Arrays.equals(Files.readAllBytes(marker), VERSION)) {
            throw new IOException("unsupported storage marker: " + marker);
        }
        require(root, "segments/topology.v1");
        require(root, "produce.v1.journal");
        // The dead-letter store may still be a pre-upgrade v1 journal; the
        // broker migrates it on open. Either form is a valid initialized layout.
        boolean legacy = Files.isRegularFile(root.resolve("deadletter.v1.journal"));
        boolean current = Files.isRegularFile(root.resolve("deadletter.v2.meta"))
                && Files.isRegularFile(root.resolve("deadletter.v2.data"));
        if (!legacy && !current) {
            throw new IOException("initialized data directory is missing dead-letter storage");
        }
        if (!Files.isDirectory(root.resolve("groups"))) {
            throw new IOException("initialized data directory is missing groups/");
        }
    }

    private static void require(Path root, String name) throws IOException {
        if (!Files.isRegularFile(root.resolve(name))) {
            throw new IOException("initialized data directory is missing " + name);
        }
    }

    void publish() throws IOException {
        if (!initialized) {
            Disk.replace(root.resolve(MARKER), VERSION, () -> {});
        }
    }
}
