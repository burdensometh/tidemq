package tidemq;

import java.io.IOException;
import java.io.Reader;
import java.net.InetAddress;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Collections;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.TreeMap;
import java.util.regex.Pattern;

public record Settings(
        InetAddress address,
        int port,
        Path dataDirectory,
        Map<String, Integer> topics,
        int segmentBytes,
        int messageBytes,
        int frameBytes,
        int maxConnections,
        int readTimeoutMs,
        int maxMembers,
        int maxBatch,
        int maxInflight,
        int deliveryWindow,
        long leaseMs,
        long reservationMs,
        int maxAttempts,
        long retentionAgeMs,
        long retentionIntervalMs) {
    public static final int MAX_IDENTIFIER = 64;
    public static final int MAX_KEY_BYTES = 256;
    public static final int MAX_PARTITIONS = 64;
    public static final int MIN_FRAME_BYTES = 1024;
    public static final int MAX_FRAME_BYTES = 64 * 1024 * 1024;
    private static final Pattern IDENTIFIER = Pattern.compile("[A-Za-z0-9][A-Za-z0-9_-]{0,63}");
    private static final Set<String> KEYS = Set.of(
            "listen.host", "listen.port", "data.directory", "topics",
            "segment.bytes", "message.max.bytes", "frame.max.bytes",
            "session.max.connections", "session.read.timeout.ms", "group.max.members",
            "delivery.max.batch", "delivery.max.inflight", "delivery.window",
            "delivery.lease.ms", "delivery.reservation.ms", "delivery.max.attempts",
            "retention.min.age.ms", "retention.interval.ms");

    public Settings {
        topics = Collections.unmodifiableMap(new TreeMap<>(topics));
        dataDirectory = dataDirectory.toAbsolutePath().normalize();
        if (!address.isLoopbackAddress()) {
            throw new IllegalArgumentException("listen.host must resolve to a loopback address");
        }
        range("listen.port", port, 0, 65535);
        range("topics", topics.size(), 1, 16);
        int partitionCount = 0;
        for (var entry : topics.entrySet()) {
            requireIdentifier(entry.getKey(), "topic");
            range("partitions per topic", entry.getValue(), 1, 64);
            partitionCount += entry.getValue();
        }
        range("total partitions", partitionCount, 1, 64);
        range("segment.bytes", segmentBytes, 1024, 256 * 1024 * 1024);
        range("message.max.bytes", messageBytes, 1, 8 * 1024 * 1024);
        range("frame.max.bytes", frameBytes, MIN_FRAME_BYTES, MAX_FRAME_BYTES);
        range("session.max.connections", maxConnections, 1, 512);
        range("session.read.timeout.ms", readTimeoutMs, 100, 600_000);
        range("group.max.members", maxMembers, 1, 256);
        range("delivery.max.batch", maxBatch, 1, 256);
        range("delivery.max.inflight", maxInflight, maxBatch, 4096);
        range("delivery.window", deliveryWindow, maxInflight, 8192);
        range("delivery.lease.ms", leaseMs, 1, 86_400_000);
        range("delivery.reservation.ms", reservationMs, 1, 86_400_000);
        range("delivery.max.attempts", maxAttempts, 1, 1000);
        range("retention.min.age.ms", retentionAgeMs, 0, 31_536_000_000L);
        range("retention.interval.ms", retentionIntervalMs, 10, 3_600_000);
        if (messageBytes + 1024 > Math.min(segmentBytes, frameBytes)) {
            throw new IllegalArgumentException("message.max.bytes requires 1024 bytes of segment and frame overhead");
        }
    }

    public static Settings load(Path path) throws IOException {
        Properties p = new Properties();
        try (Reader reader = Files.newBufferedReader(path, StandardCharsets.UTF_8)) {
            p.load(reader);
        }
        for (String key : p.stringPropertyNames()) {
            if (!KEYS.contains(key)) {
                throw new IllegalArgumentException("unknown configuration key " + key);
            }
        }
        return new Settings(
                InetAddress.getByName(p.getProperty("listen.host", "127.0.0.1")),
                number(p, "listen.port", 7410),
                Path.of(p.getProperty("data.directory", "var/tidemq")),
                parseTopics(p.getProperty("topics", "orders:3,inventory:2")),
                number(p, "segment.bytes", 4_194_304),
                number(p, "message.max.bytes", 262_144),
                number(p, "frame.max.bytes", 8_388_608),
                number(p, "session.max.connections", 128),
                number(p, "session.read.timeout.ms", 30_000),
                number(p, "group.max.members", 64),
                number(p, "delivery.max.batch", 32),
                number(p, "delivery.max.inflight", 128),
                number(p, "delivery.window", 512),
                wideNumber(p, "delivery.lease.ms", 5000),
                wideNumber(p, "delivery.reservation.ms", 3000),
                number(p, "delivery.max.attempts", 3),
                wideNumber(p, "retention.min.age.ms", 60_000),
                wideNumber(p, "retention.interval.ms", 5000));
    }

    private static Map<String, Integer> parseTopics(String value) {
        Map<String, Integer> topics = new TreeMap<>();
        for (String item : value.split(",", -1)) {
            String[] pair = item.strip().split(":", -1);
            if (pair.length != 2) {
                throw new IllegalArgumentException("topics must use name:partitions entries");
            }
            String topic = requireIdentifier(pair[0].strip(), "topic");
            if (topics.putIfAbsent(topic, Integer.parseInt(pair[1].strip())) != null) {
                throw new IllegalArgumentException("duplicate topic " + topic);
            }
        }
        return topics;
    }

    private static int number(Properties p, String key, int fallback) {
        return Math.toIntExact(wideNumber(p, key, fallback));
    }

    private static long wideNumber(Properties p, String key, long fallback) {
        return Long.parseLong(p.getProperty(key, Long.toString(fallback)).strip());
    }

    private static void range(String key, long value, long minimum, long maximum) {
        if (value < minimum || value > maximum) {
            throw new IllegalArgumentException(key + " must be within " + minimum + ".." + maximum);
        }
    }

    public static String requireIdentifier(String value, String label) {
        if (value == null || !IDENTIFIER.matcher(value).matches()) {
            throw new IllegalArgumentException("invalid " + label + " identifier");
        }
        return value;
    }
}
