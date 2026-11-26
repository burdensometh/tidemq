package tidemq.segment;

import tidemq.Boundary;

import java.io.IOException;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.NavigableMap;
import java.util.TreeMap;

final class PartitionLog {
    private final TopicPartition partition;
    private final Path directory;
    private final int segmentBytes;
    private final Boundary boundary;
    private final NavigableMap<Long, Segment> segments = new TreeMap<>();

    PartitionLog(TopicPartition partition, Path directory, int segmentBytes, Boundary boundary,
                 long now, boolean initialized) throws IOException {
        this.partition = partition;
        this.directory = directory;
        this.segmentBytes = segmentBytes;
        this.boundary = boundary;
        Disk.makeDirectory(directory);
        for (Segment segment : PartitionInventory.openActive(directory, partition)) {
            segments.put(segment.baseOffset(), segment);
        }
        if (segments.isEmpty()) {
            if (initialized) {
                throw new IOException("missing all segments in initialized partition " + partition);
            }
            createSegment(0, now);
        }
    }

    TopicPartition topicPartition() {
        return partition;
    }

    private Segment last() {
        return segments.lastEntry().getValue();
    }

    private Segment createSegment(long offset, long now) throws IOException {
        Segment segment = Segment.create(directory, partition, offset, now);
        segments.put(offset, segment);
        return segment;
    }

    Message append(String producer, long sequence, byte[] key, byte[] value, long now) throws IOException {
        Segment active = last();
        long offset = active.endOffset();
        if (offset == Long.MAX_VALUE) {
            throw new IOException("offset space exhausted for " + partition);
        }
        Message message = new Message(partition, offset, now, producer, sequence, key, value);
        if (!active.canFit(message, segmentBytes)) {
            active = createSegment(offset, now);
        }
        active.append(message, boundary);
        return message;
    }

    Message read(long offset) throws IOException {
        var candidate = segments.floorEntry(offset);
        if (candidate == null || offset >= candidate.getValue().endOffset()) {
            throw new IOException("offset " + offset + " outside current range of " + partition);
        }
        return candidate.getValue().get(offset);
    }

    long low() {
        return segments.firstKey();
    }

    long end() {
        return last().endOffset();
    }

    List<Segment.View> views(int maximum) {
        var views = new ArrayList<Segment.View>();
        Segment active = last();
        for (Segment segment : segments.descendingMap().values()) {
            views.add(segment.view(segment == active));
            if (views.size() == maximum) {
                break;
            }
        }
        return List.copyOf(views.reversed());
    }

    int segmentCount() {
        return segments.size();
    }

    void forEach(Segment.MessageVisitor visitor) throws IOException {
        for (Segment segment : segments.values()) {
            segment.forEach(visitor);
        }
    }

    int retire(long frontier, long now, long age) throws IOException {
        int deleted = 0;
        while (segments.size() > 1) {
            var oldest = segments.firstEntry();
            if (!oldest.getValue().eligibleForRetirement(frontier, now, age)) {
                break;
            }
            oldest.getValue().remove();
            segments.pollFirstEntry();
            deleted++;
        }
        return deleted;
    }

    RetentionPreview preview(long frontier, long now, long age, int groupCount, List<String> sample) {
        int eligible = 0;
        long bytes = 0;
        Segment active = last();
        for (Segment segment : segments.values()) {
            boolean isActive = segment == active;
            if (isActive || !segment.eligibleForRetirement(frontier, now, age)) {
                Segment.View kept = segment.view(isActive);
                long last = kept.latestAppendMs();
                long remainingAge = now < last ? cappedAdd(last - now, age) : Math.max(0, age - (now - last));
                return new RetentionPreview(partition, now, age, frontier, groupCount, sample,
                        eligible, bytes, kept.baseOffset(), kept.baseOffset(), kept.endOffset(), isActive,
                        kept.endOffset() > frontier, remainingAge);
            }
            eligible++;
            bytes = cappedAdd(bytes, segment.view(false).sizeBytes());
        }
        throw new IllegalStateException("partition has no active segment");
    }

    private static long cappedAdd(long left, long right) {
        return left + Math.min(right, Long.MAX_VALUE - left);
    }
}
