package tidemq;

import tidemq.wire.TcpBroker;
import tidemq.wire.TideClient;

import java.io.IOException;
import java.net.InetAddress;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Clock;
import java.time.Instant;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.util.Comparator;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

final class TestSupport {
    interface Action {
        void run() throws Exception;
    }

    private TestSupport() {}

    static Settings settings(Path root) {
        return settings(root, Map.of("orders", 1), 1000);
    }

    static Settings settings(Path root, Map<String, Integer> topics, long age) {
        return new Settings(InetAddress.getLoopbackAddress(), 0, root, topics,
                4096, 2048, 64 * 1024, 64, 10_000, 16,
                16, 32, 128, 1000, 300, 2, age, 60_000);
    }

    static TideClient connect(TcpBroker server) throws IOException {
        return TideClient.connect(InetAddress.getLoopbackAddress().getHostAddress(), server.port());
    }

    static byte[] bytes(String value) {
        return value.getBytes(StandardCharsets.UTF_8);
    }

    static void eq(Object expected, Object observed) {
        if (!Objects.equals(expected, observed)) {
            throw new AssertionError("expected " + expected + ", got " + observed);
        }
    }

    static void check(boolean predicate, String detail) {
        if (!predicate) {
            throw new AssertionError(detail);
        }
    }

    static void code(BrokerException.Code expected, Action action) throws Exception {
        try {
            action.run();
        } catch (BrokerException error) {
            eq(expected, error.code());
            return;
        }
        throw new AssertionError("expected " + expected);
    }

    static void io(Action action) throws Exception {
        try {
            action.run();
        } catch (IOException expected) {
            return;
        }
        throw new AssertionError("expected IOException");
    }

    static final class Scratch implements AutoCloseable {
        final Path root = Files.createTempDirectory("tidemq-");

        Scratch() throws IOException {}

        @Override
        public void close() throws IOException {
            try (var tree = Files.walk(root)) {
                for (Path path : tree.sorted(Comparator.reverseOrder()).toList()) {
                    Files.delete(path);
                }
            }
        }
    }

    static final class Time extends Clock {
        private final AtomicLong time = new AtomicLong(1_700_000_000_000L);

        void advance(long milliseconds) {
            time.addAndGet(milliseconds);
        }

        @Override
        public ZoneId getZone() {
            return ZoneOffset.UTC;
        }

        @Override
        public Clock withZone(ZoneId zone) {
            if (!getZone().equals(zone)) {
                throw new IllegalArgumentException("test clock uses UTC");
            }
            return this;
        }

        @Override
        public Instant instant() {
            return Instant.ofEpochMilli(millis());
        }

        @Override
        public long millis() {
            return time.get();
        }
    }

    static final class Fault implements Boundary {
        private final AtomicReference<Point> pending = new AtomicReference<>();

        void arm(Point point) {
            check(pending.compareAndSet(null, point), "another fault is armed");
        }

        @Override
        public void reached(Point point) throws IOException {
            if (pending.compareAndSet(point, null)) {
                throw new IOException("test boundary " + point);
            }
        }
    }
}
