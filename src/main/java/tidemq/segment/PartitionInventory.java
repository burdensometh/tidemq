package tidemq.segment;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;

/**
 * Reads canonical v1 segment files in base-offset order. Only a broker opening
 * its current data directory requests active-tail repair.
 */
public final class PartitionInventory {
    public record Entry(Path path, Segment.View segment) {}
    public record View(TopicPartition partition, long low, long end, long records, long bytes,
                       List<Entry> segments) {
        public View {
            segments = List.copyOf(segments);
        }
    }

    private record Opened(Path path, Segment segment) {}

    private PartitionInventory() {}

    private static List<Path> paths(Path directory) throws IOException {
        try (var entries = Files.list(directory)) {
            return entries.filter(path -> path.getFileName().toString().endsWith(Segment.EXTENSION))
                    .sorted().toList();
        }
    }

    private static List<Opened> scan(Path dir, TopicPartition partition, boolean repairLast) throws IOException {
        List<Path> paths = paths(dir);
        var loaded = new ArrayList<Opened>(paths.size());
        Segment previous = null;
        for (int i = 0; i < paths.size(); i++) {
            Path path = paths.get(i);
            boolean last = i == paths.size() - 1;
            // Only the broker (repairLast) may open sealed segments through their
            // persisted index; offline verification always replays every record.
            // The active tail is always scanned so an incomplete record is repaired.
            Segment segment = last || !repairLast
                    ? Segment.open(path, partition, last && repairLast)
                    : Segment.openSealed(path, partition, true);
            if (previous != null && previous.endOffset() != segment.baseOffset()) {
                throw new IOException("segment offset gap in " + partition + " between "
                        + previous.endOffset() + " and " + segment.baseOffset());
            }
            if (!last && segment.endOffset() == segment.baseOffset()) {
                throw new IOException("sealed segment is empty in " + partition + ": " + path);
            }
            loaded.add(new Opened(path, segment));
            previous = segment;
        }
        return loaded;
    }

    static List<Segment> openActive(Path dir, TopicPartition partition) throws IOException {
        return scan(dir, partition, true).stream().map(Opened::segment).toList();
    }

    public static View verify(Path dir, TopicPartition partition) throws IOException {
        List<Opened> files = scan(dir, partition, false);
        if (files.isEmpty()) {
            throw new IOException("no v1 segments in " + dir);
        }
        long low = files.getFirst().segment().baseOffset();
        long end = files.getLast().segment().endOffset();
        var entries = new ArrayList<Entry>(files.size());
        long bytes = 0;
        for (int i = 0; i < files.size(); i++) {
            Opened file = files.get(i);
            Segment.View segment = file.segment().view(i == files.size() - 1);
            entries.add(new Entry(file.path(), segment));
            bytes = Math.addExact(bytes, segment.sizeBytes());
        }
        return new View(partition, low, end, end - low, bytes, entries);
    }
}
