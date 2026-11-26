package tidemq.segment;

import tidemq.Boundary;
import tidemq.BrokerException;
import tidemq.Settings;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;

public final class SegmentStore {
    public record Range(long retainedFrom, long nextOffset) {}
    public record PartitionView(TopicPartition partition, Range range, int segmentCount, List<Segment.View> recent) {}

    private final Map<TopicPartition, PartitionLog> logs;
    private final Map<String, Integer> topics;
    private final long retentionAgeMs;

    public SegmentStore(Settings settings, Boundary boundary, long now) throws IOException {
        this.topics = settings.topics();
        this.retentionAgeMs = settings.retentionAgeMs();
        Path root = settings.dataDirectory().resolve("segments");
        Disk.makeDirectory(root);
        Path topology = root.resolve("topology.v1");
        byte[] expected = topologyBytes(topics);
        boolean initialized = Files.exists(topology);
        if (initialized && !Arrays.equals(expected, Files.readAllBytes(topology))) {
            throw new IOException("configured route topology disagrees with " + topology);
        }
        var opened = new TreeMap<TopicPartition, PartitionLog>();
        for (var entry : topics.entrySet()) {
            for (int number = 0; number < entry.getValue(); number++) {
                TopicPartition partition = new TopicPartition(entry.getKey(), number);
                Path path = root.resolve(partition.topic()).resolve(Integer.toString(number));
                opened.put(partition, new PartitionLog(partition, path, settings.segmentBytes(),
                        boundary, now, initialized));
            }
        }
        if (!initialized) {
            Disk.replace(topology, expected, () -> {});
        }
        logs = Collections.unmodifiableMap(opened);
    }

    private static byte[] topologyBytes(Map<String, Integer> topics) {
        var text = new StringBuilder("TideMQ topology v1\n");
        topics.forEach((topic, count) -> text.append(topic).append(':').append(count).append('\n'));
        return text.toString().getBytes(StandardCharsets.UTF_8);
    }

    public int partitionCount(String topic) throws BrokerException {
        Integer count = topics.get(topic);
        if (count == null) {
            throw new BrokerException(BrokerException.Code.UNKNOWN_TOPIC, "unknown topic " + topic);
        }
        return count;
    }

    private PartitionLog log(TopicPartition partition) throws BrokerException {
        PartitionLog log = logs.get(partition);
        if (log == null) {
            throw new BrokerException(BrokerException.Code.UNKNOWN_TOPIC, "unknown partition " + partition);
        }
        return log;
    }

    public Map<String, Integer> topics() {
        return topics;
    }

    public Set<TopicPartition> partitions() {
        return logs.keySet();
    }

    public Message append(TopicPartition partition, String producer, long sequence,
                          byte[] key, byte[] payload, long now) throws IOException {
        return log(partition).append(producer, sequence, key, payload, now);
    }

    public Message read(TopicPartition partition, long offset) throws IOException {
        return log(partition).read(offset);
    }

    public Reader reader() {
        return new Reader();
    }

    /**
     * Reuses one open channel per touched segment across a whole delivery
     * selection, so both projects and checkpoints sharing this store keep
     * their own view and never reopen a segment for each record.
     */
    public final class Reader implements AutoCloseable {
        private final Map<TopicPartition, PartitionLog.Reader> logs = new HashMap<>();

        public Message read(TopicPartition partition, long offset) throws IOException {
            return forPartition(partition).read(offset);
        }

        PartitionLog.Reader forPartition(TopicPartition partition) throws BrokerException {
            PartitionLog log = log(partition);
            PartitionLog.Reader reader = logs.get(partition);
            if (reader == null) {
                reader = log.reader();
                logs.put(partition, reader);
            }
            return reader;
        }

        @Override
        public void close() throws IOException {
            IOException first = null;
            for (PartitionLog.Reader reader : logs.values()) {
                try {
                    reader.close();
                } catch (IOException error) {
                    if (first == null) {
                        first = error;
                    } else {
                        first.addSuppressed(error);
                    }
                }
            }
            logs.clear();
            if (first != null) {
                throw first;
            }
        }
    }

    public Range range(TopicPartition partition) throws BrokerException {
        var log = log(partition);
        return new Range(log.low(), log.end());
    }

    public PartitionView view(TopicPartition partition, int maximum) throws BrokerException {
        var log = log(partition);
        return new PartitionView(partition, range(partition), log.segmentCount(), log.views(maximum));
    }

    public void forEach(Segment.MessageVisitor visitor) throws IOException {
        for (PartitionLog log : logs.values()) {
            log.forEach(visitor);
        }
    }

    public int retire(TopicPartition partition, long frontier, long now) throws IOException {
        return log(partition).retire(frontier, now, retentionAgeMs);
    }

    public RetentionPreview preview(TopicPartition partition, long frontier, long now, int groupCount,
                                    List<String> limitingGroups) throws BrokerException {
        return log(partition).preview(frontier, now, retentionAgeMs, groupCount, limitingGroups);
    }
}
