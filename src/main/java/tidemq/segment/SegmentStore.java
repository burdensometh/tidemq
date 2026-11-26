package tidemq.segment;

import tidemq.Boundary;
import tidemq.BrokerException;
import tidemq.Settings;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;

public final class SegmentStore {
    public record Range(long retainedFrom, long nextOffset) {}
    public record PartitionView(TopicPartition partition, Range range, int segmentCount, List<Segment.View> recent) {}
    public record Expansion(String topic, int previousPartitions, int partitionCount,
                            RouteGeneration generation, List<Long> boundaries) {
        public Expansion {
            boundaries = List.copyOf(boundaries);
        }
    }

    private final Settings settings;
    private final Boundary boundary;
    private final Path root;
    private final RouteJournal routes;
    private final Map<TopicPartition, PartitionLog> logs;
    private final Map<String, Integer> topics;
    private final long retentionAgeMs;
    private final boolean initialized;
    public SegmentStore(Settings settings, Boundary boundary, long now) throws IOException {
        this.settings = settings;
        this.boundary = boundary;
        this.retentionAgeMs = settings.retentionAgeMs();
        root = settings.dataDirectory().resolve("segments");
        Disk.makeDirectory(root);
        Path topology = root.resolve("topology.v1");
        this.initialized = Files.exists(topology);
        routes = RouteJournal.open(settings.dataDirectory(), boundary, now);
        if (routes.isInitialized()) {
            reconcileConfiguration();
        } else {
            if (initialized) {
                // Upgrade of a data directory created before routing generations
                // were tracked: the fixed topology snapshot is the only authority.
                Map<String, Integer> onDisk = parseTopology(topology);
                if (!onDisk.equals(settings.topics())) {
                    throw new IOException("configured route topology disagrees with " + topology);
                }
            }
            routes.seed(settings.topics());
        }
        var opened = new TreeMap<TopicPartition, PartitionLog>();
        var counts = new TreeMap<String, Integer>();
        for (var entry : routes.generations().entrySet()) {
            String topic = entry.getKey();
            int count = entry.getValue().getLast().partitionCount();
            counts.put(topic, count);
            for (int number = 0; number < count; number++) {
                TopicPartition partition = new TopicPartition(topic, number);
                Path path = root.resolve(topic).resolve(Integer.toString(number));
                opened.put(partition, new PartitionLog(partition, path, settings.segmentBytes(),
                        boundary, now, initialized));
            }
        }
        this.logs = Collections.synchronizedMap(opened);
        this.topics = Collections.synchronizedMap(counts);
        // The topology file is a derived human-readable snapshot of the journal.
        Disk.replace(topology, topologyBytes(counts), () -> {});
    }

    private static Map<String, Integer> parseTopology(Path topology) throws IOException {
        var counts = new TreeMap<String, Integer>();
        for (String line : Files.readAllLines(topology, StandardCharsets.UTF_8)) {
            if (line.isBlank() || line.startsWith("TideMQ topology")) {
                continue;
            }
            int separator = line.lastIndexOf(':');
            if (separator < 0) {
                throw new IOException("malformed topology snapshot " + topology);
            }
            counts.put(line.substring(0, separator), Integer.parseInt(line.substring(separator + 1).strip()));
        }
        return counts;
    }

    private void reconcileConfiguration() throws IOException {
        Map<String, List<RouteGeneration>> persisted = routes.generations();
        for (var configured : settings.topics().entrySet()) {
            List<RouteGeneration> history = persisted.get(configured.getKey());
            if (history == null) {
                throw new IOException("configured topic " + configured.getKey() + " is absent from route journal");
            }
            if (history.getFirst().partitionCount() != configured.getValue()) {
                throw new IOException("configured initial partition count for " + configured.getKey()
                        + " disagrees with route journal");
            }
        }
        for (String topic : persisted.keySet()) {
            if (!settings.topics().containsKey(topic)) {
                throw new IOException("route journal topic " + topic + " is missing from configuration");
            }
        }
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
        synchronized (topics) {
            return Map.copyOf(topics);
        }
    }

    public Set<TopicPartition> partitions() {
        return Set.copyOf(logs.keySet());
    }

    public List<RouteGeneration> routeHistory(String topic) throws BrokerException {
        return routes.history(topic);
    }

    /**
     * Adds partitions to an existing topic while the broker keeps serving. The
     * captured end offsets become the durable old-route/new-route boundary.
     * Callers run inside the broker transition lock, so no produce can append
     * between reading the boundaries and committing the new routing generation.
     */
    public Expansion expand(String topic, int targetCount, long now) throws IOException {
        int current = partitionCount(topic);
        if (targetCount <= current) {
            throw new BrokerException(BrokerException.Code.BAD_REQUEST,
                    "partition count for " + topic + " must grow beyond " + current);
        }
        int totalAfter;
        synchronized (topics) {
            totalAfter = 0;
            for (var entry : topics.entrySet()) {
                totalAfter += entry.getKey().equals(topic) ? targetCount : entry.getValue();
            }
        }
        if (targetCount > Settings.MAX_PARTITIONS || totalAfter > Settings.MAX_PARTITIONS) {
            throw new BrokerException(BrokerException.Code.CAPACITY, "partition limit reached");
        }
        var boundaries = new ArrayList<Long>(current);
        for (int number = 0; number < current; number++) {
            boundaries.add(log(new TopicPartition(topic, number)).end());
        }
        var created = new ArrayList<PartitionLog>(targetCount - current);
        for (int number = current; number < targetCount; number++) {
            TopicPartition partition = new TopicPartition(topic, number);
            Path path = root.resolve(topic).resolve(Integer.toString(number));
            created.add(new PartitionLog(partition, path, settings.segmentBytes(), boundary, now, false));
        }
        RouteGeneration generation = routes.appendExpansion(topic, targetCount, boundaries);
        for (PartitionLog partitionLog : created) {
            logs.put(partitionLog.topicPartition(), partitionLog);
        }
        topics.put(topic, targetCount);
        Disk.replace(root.resolve("topology.v1"), topologyBytes(topics()), () -> {});
        return new Expansion(topic, current, targetCount, generation, boundaries);
    }

    public Message append(TopicPartition partition, String producer, long sequence,
                          byte[] key, byte[] payload, long now) throws IOException {
        return log(partition).append(producer, sequence, key, payload, now);
    }

    public Message read(TopicPartition partition, long offset) throws IOException {
        return log(partition).read(offset);
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
                                    List<String> limitingGroups) throws IOException {
        return log(partition).preview(frontier, now, retentionAgeMs, groupCount, limitingGroups);
    }
}
