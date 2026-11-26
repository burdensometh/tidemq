package tidemq.segment;

import tidemq.Binary;
import tidemq.Boundary;
import tidemq.BrokerException;
import tidemq.Settings;

import java.io.DataInputStream;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

/**
 * Append-only history of topic routing generations. The first record for a
 * topic captures its configured initial topology; every following record is an
 * online expansion. The journal is the durable commit point of an expansion.
 *
 * <p>An expansion is persisted in two forced steps: the generation is first
 * written with its committed flag clear, then rewritten with the flag set. A
 * crash in the first step leaves an uncommitted trailing record that startup
 * truncates, so the expansion never takes effect unless both forces completed.
 * Automatic routing only ever observes committed generations, and the captured
 * partition boundaries survive process restarts.
 */
public final class RouteJournal {
    private static final int MAGIC = 0x544D5252; // TMRR

    private final Path path;
    private final Boundary boundary;
    private final RecordFile file;
    private final Map<String, List<RouteGeneration>> generations = new TreeMap<>();

    private RouteJournal(Path path, Boundary boundary, RecordFile file) {
        this.path = path;
        this.boundary = boundary;
        this.file = file;
    }

    public static RouteJournal open(Path directory, Boundary boundary, long now) throws IOException {
        Path path = directory.resolve("route.v1.journal");
        RecordFile file;
        if (Files.exists(path)) {
            file = RecordFile.open(path, MAGIC, 0, true, (position, body) -> {});
        } else {
            file = RecordFile.create(path, new RecordFile.Header(MAGIC, 0, now));
        }
        var journal = new RouteJournal(path, boundary, file);
        if (Files.exists(path)) {
            var rollback = new long[]{-1};
            file.forEach((position, body) -> {
                RouteRecord record = decode(body);
                if (record.committed()) {
                    journal.replay(record);
                } else if (rollback[0] < 0) {
                    // First uncommitted record: the expansion's commit force never
                    // finished. Remember where its tail begins for truncation.
                    rollback[0] = position;
                }
            });
            if (rollback[0] >= 0) {
                file.truncate(rollback[0]);
            }
        }
        return journal;
    }

    private void replay(RouteRecord record) throws IOException {
        List<RouteGeneration> history = generations.get(record.topic());
        RouteGeneration previous = history == null ? null : history.getLast();
        if (previous != null) {
            if (record.generation().partitionCount() <= previous.partitionCount()) {
                throw new IOException("routing generation for " + record.topic() + " must add partitions");
            }
            if (record.generation().boundaries().size() != previous.partitionCount()) {
                throw new IOException("routing boundaries for " + record.topic()
                        + " must cover every previously existing partition");
            }
        } else if (!record.generation().boundaries().isEmpty()) {
            throw new IOException("initial routing generation for " + record.topic() + " cannot carry boundaries");
        }
        generations.computeIfAbsent(record.topic(), key -> new ArrayList<>()).add(record.generation());
    }

    public boolean isInitialized() {
        return !generations.isEmpty();
    }

    public Map<String, List<RouteGeneration>> generations() {
        var copy = new TreeMap<String, List<RouteGeneration>>();
        generations.forEach((topic, history) -> copy.put(topic, List.copyOf(history)));
        return Collections.unmodifiableMap(copy);
    }

    public List<RouteGeneration> history(String topic) throws BrokerException {
        List<RouteGeneration> history = generations.get(topic);
        if (history == null) {
            throw new BrokerException(BrokerException.Code.UNKNOWN_TOPIC, "unknown topic " + topic);
        }
        return List.copyOf(history);
    }

    public int partitionCount(String topic) throws BrokerException {
        return history(topic).getLast().partitionCount();
    }

    /** Writes the configured starting topology. Only valid for a fresh journal. */
    public void seed(Map<String, Integer> initialTopics) throws IOException {
        if (!generations.isEmpty()) {
            throw new IllegalStateException("route journal already initialized");
        }
        for (var entry : new TreeMap<>(initialTopics).entrySet()) {
            var record = new RouteRecord(entry.getKey(),
                    new RouteGeneration(entry.getValue(), List.of()), true);
            file.append(encode(record), () -> boundary.reached(Boundary.Point.ROUTE_BEFORE_FORCE));
            boundary.reached(Boundary.Point.ROUTE_AFTER_FORCE);
            generations.computeIfAbsent(record.topic(), key -> new ArrayList<>()).add(record.generation());
        }
    }

    /**
     * Validates, persists and publishes one expansion generation.
     * {@code boundaries} holds the end offset of every partition that existed
     * before the expansion, in partition order. The generation only becomes
     * visible after its committed record is forced.
     */
    public RouteGeneration appendExpansion(String topic, int newCount, List<Long> boundaries) throws IOException {
        RouteGeneration previous = history(topic).getLast();
        if (newCount <= previous.partitionCount() || newCount > Settings.MAX_PARTITIONS) {
            throw new BrokerException(BrokerException.Code.BAD_REQUEST,
                    "new partition count must be within " + (previous.partitionCount() + 1)
                            + ".." + Settings.MAX_PARTITIONS);
        }
        if (boundaries.size() != previous.partitionCount()) {
            throw new BrokerException(BrokerException.Code.BAD_REQUEST,
                    "routing boundaries must cover every previously existing partition");
        }
        var generation = new RouteGeneration(newCount, boundaries);
        // Stage one: the generation is durable but explicitly uncommitted.
        file.append(encode(new RouteRecord(topic, generation, false)),
                () -> boundary.reached(Boundary.Point.ROUTE_BEFORE_FORCE));
        // Stage two: the same generation is marked committed and forced.
        file.append(encode(new RouteRecord(topic, generation, true)),
                () -> boundary.reached(Boundary.Point.ROUTE_BEFORE_FORCE));
        boundary.reached(Boundary.Point.ROUTE_AFTER_FORCE);
        generations.get(topic).add(generation);
        return generation;
    }

    private record RouteRecord(String topic, RouteGeneration generation, boolean committed) {}

    private static byte[] encode(RouteRecord record) throws IOException {
        return Binary.encode(out -> {
            Binary.writeText(out, record.topic());
            RouteGeneration.write(out, record.generation());
            out.writeBoolean(record.committed());
        });
    }

    private static RouteRecord decode(byte[] bytes) throws IOException {
        try {
            return Binary.decode(bytes, (DataInputStream in) -> {
                String topic = Binary.readText(in, Settings.MAX_IDENTIFIER);
                Settings.requireIdentifier(topic, "topic");
                RouteGeneration generation = RouteGeneration.read(in);
                boolean committed = in.readBoolean();
                return new RouteRecord(topic, generation, committed);
            });
        } catch (IllegalArgumentException invalid) {
            throw new IOException("invalid persisted routing generation", invalid);
        }
    }
}
