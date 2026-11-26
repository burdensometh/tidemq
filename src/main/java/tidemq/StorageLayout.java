package tidemq;

import tidemq.segment.Disk;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.List;

final class StorageLayout {
    private static final String MARKER = "storage.v1";
    private static final byte[] VERSION = "TideMQ broker storage v1\n".getBytes(StandardCharsets.UTF_8);
    private static final List<String> REQUIRED_FILES = List.of(
            "segments/topology.v1", "route.v1.journal", "produce.v1.journal", "deadletter.v1.journal");

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
        for (String file : REQUIRED_FILES) {
            if (!Files.isRegularFile(root.resolve(file))) {
                throw new IOException("initialized data directory is missing " + file);
            }
        }
        if (!Files.isDirectory(root.resolve("groups"))) {
            throw new IOException("initialized data directory is missing groups/");
        }
    }

    void publish() throws IOException {
        if (!initialized) {
            Disk.replace(root.resolve(MARKER), VERSION, () -> {});
        }
    }
}
