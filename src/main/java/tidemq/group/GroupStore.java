package tidemq.group;

import tidemq.Binary;
import tidemq.Boundary;
import tidemq.Settings;
import tidemq.segment.Disk;
import tidemq.segment.MessageV1;
import tidemq.segment.SegmentStore;
import tidemq.segment.TopicPartition;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.TreeMap;
import java.util.TreeSet;

final class GroupStore {
    private static final int MAGIC = 0x544D4750; // TMGP
    private static final int VERSION = 1;
    private static final int MAX_SNAPSHOT_BYTES = 10 * 1024 * 1024;
    private static final int MAX_AHEAD = 8192;
    private static final String SUFFIX = ".group";

    private final Path directory;
    private final Boundary boundary;
    private final SegmentStore segments;

    GroupStore(Path directory, Boundary boundary, SegmentStore segments) throws IOException {
        this.directory = directory;
        this.boundary = boundary;
        this.segments = segments;
        Disk.makeDirectory(directory);
    }

    List<GroupState> load(int maximum) throws IOException {
        try (var paths = Files.list(directory)) {
            var states = new ArrayList<GroupState>();
            for (Path path : paths.filter(item -> item.getFileName().toString().endsWith(SUFFIX))
                    .sorted().limit(maximum + 1L).toList()) {
                if (states.size() == maximum) {
                    throw new IOException("durable group count exceeds limit " + maximum);
                }
                states.add(loadOne(path));
            }
            return states;
        }
    }

    private GroupState loadOne(Path path) throws IOException {
        if (Files.size(path) > MAX_SNAPSHOT_BYTES) {
            throw new IOException("group snapshot exceeds size limit: " + path);
        }
        try {
            return Binary.decode(Files.readAllBytes(path), in -> {
                if (in.readInt() != MAGIC || in.readInt() != VERSION) {
                    throw new IOException("invalid v1 group header");
                }
                byte[] body = Binary.readBytes(in, MAX_SNAPSHOT_BYTES);
                if (in.readInt() != Disk.checksum(body)) {
                    throw new IOException("group snapshot checksum mismatch");
                }
                GroupState group = decode(body);
                if (!path.equals(pathFor(group.name()))) {
                    throw new IOException("group name disagrees with filename");
                }
                return group;
            });
        } catch (IllegalArgumentException | IOException invalid) {
            throw new IOException("reading " + path + ": " + invalid.getMessage(), invalid);
        }
    }

    private Path pathFor(String name) {
        return directory.resolve(name + SUFFIX);
    }

    void save(GroupState group, boolean generationChange) throws IOException {
        byte[] body = encode(group);
        byte[] snapshot = Binary.encode(out -> {
            out.writeInt(MAGIC);
            out.writeInt(VERSION);
            Binary.writeBytes(out, body);
            out.writeInt(Disk.checksum(body));
        });
        Disk.replace(pathFor(group.name()), snapshot, () -> {
            if (generationChange) {
                boundary.reached(Boundary.Point.GROUP_BEFORE_REPLACE);
            }
        });
        if (generationChange) {
            boundary.reached(Boundary.Point.GROUP_AFTER_REPLACE);
        }
    }

    private static byte[] encode(GroupState group) throws IOException {
        return Binary.encode(out -> {
            Binary.writeText(out, group.name());
            out.writeLong(group.epoch());
            out.writeInt(group.subscriptions().size());
            for (String topic : group.subscriptions()) {
                Binary.writeText(out, topic);
            }
            out.writeInt(group.frontiers().size());
            for (var entry : group.frontiers().entrySet()) {
                MessageV1.writePartition(out, entry.getKey());
                AckFrontier frontier = entry.getValue();
                out.writeLong(frontier.next());
                out.writeInt(frontier.completedAhead().size());
                for (long offset : frontier.completedAhead()) {
                    out.writeLong(offset);
                }
                out.writeInt(frontier.retrying().size());
                for (var attempt : frontier.retrying().entrySet()) {
                    out.writeLong(attempt.getKey());
                    out.writeInt(attempt.getValue());
                }
            }
        });
    }

    private GroupState decode(byte[] body) throws IOException {
        return Binary.decode(body, in -> {
            String name = Settings.requireIdentifier(Binary.readText(in, Settings.MAX_IDENTIFIER), "group");
            long epoch = in.readLong();
            if (epoch <= 0) {
                throw new IOException("invalid durable group epoch");
            }
            var topics = new TreeSet<String>();
            int subscriptionCount = Binary.readCount(in, 16);
            for (int i = 0; i < subscriptionCount; i++) {
                String topic = Binary.readText(in, Settings.MAX_IDENTIFIER);
                segments.partitionCount(topic);
                if (!topics.add(topic)) {
                    throw new IOException("duplicate persisted subscription");
                }
            }
            if (topics.isEmpty()) {
                throw new IOException("group snapshot has no subscriptions");
            }
            var frontiers = new TreeMap<TopicPartition, AckFrontier>();
            int partitions = Binary.readCount(in, 64);
            for (int i = 0; i < partitions; i++) {
                TopicPartition partition = MessageV1.readPartition(in);
                AckFrontier frontier = readFrontier(in, segments.range(partition));
                if (!topics.contains(partition.topic()) || frontiers.putIfAbsent(partition, frontier) != null) {
                    throw new IOException("invalid persisted partition in group");
                }
            }
            // Partitions added by an online expansion after this snapshot was
            // stored start at watermark zero and are adopted on the next save.
            for (String topic : topics) {
                for (int number = 0; number < segments.partitionCount(topic); number++) {
                    frontiers.putIfAbsent(new TopicPartition(topic, number),
                            new AckFrontier(segments.range(new TopicPartition(topic, number)).retainedFrom()));
                }
            }
            return new GroupState(name, topics, frontiers, epoch);
        });
    }

    private static AckFrontier readFrontier(java.io.DataInputStream in, SegmentStore.Range range) throws IOException {
        long next = in.readLong();
        if (next < range.retainedFrom() || next > range.nextOffset()) {
            throw new IOException("group frontier outside retained partition bounds");
        }
        var completed = new TreeSet<Long>();
        int completions = Binary.readCount(in, MAX_AHEAD);
        for (int j = 0; j < completions; j++) {
            long offset = in.readLong();
            if (offset <= next || !inWindow(offset, next, range) || !completed.add(offset)) {
                throw new IOException("invalid out-of-order acknowledgement");
            }
        }
        var attempts = new TreeMap<Long, Integer>();
        int retryCount = Binary.readCount(in, MAX_AHEAD);
        for (int j = 0; j < retryCount; j++) {
            long offset = in.readLong();
            int attempt = in.readInt();
            if (!inWindow(offset, next, range) || attempt < 1 || attempt > 1000
                    || completed.contains(offset) || attempts.putIfAbsent(offset, attempt) != null) {
                throw new IOException("invalid retry counter");
            }
        }
        return new AckFrontier(next, completed, attempts);
    }

    private static boolean inWindow(long offset, long next, SegmentStore.Range range) {
        return offset >= next && offset < range.nextOffset() && offset - next < MAX_AHEAD;
    }
}
