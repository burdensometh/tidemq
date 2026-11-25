package tidemq;

import tidemq.delivery.DeliveryLease;
import tidemq.segment.TopicPartition;
import tidemq.wire.TcpBroker;
import tidemq.wire.TideClient;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static tidemq.TestSupport.*;

public final class HotPathBench {
    private static final TopicPartition ORDERS = new TopicPartition("orders", 0);
    private static final Set<String> TOPICS = Set.of("orders");
    private static final int SCANS = 40;

    public static void main(String[] args) throws Exception {
        int messages = args.length == 0 ? 1024 : Integer.parseInt(args[0]);
        if (args.length > 1 || messages < 256 || messages > 100_000) {
            throw new IllegalArgumentException("expected message count in 256..100000");
        }
        System.out.printf("java=%s vm=%s cpus=%d messages=%d%n",
                System.getProperty("java.version"), System.getProperty("java.vm.name"),
                Runtime.getRuntime().availableProcessors(), messages);
        batch(messages);
        slowConsumer();
        retention(messages);
        System.out.println("BENCH PASS");
    }

    private static void batch(int messages) throws Exception {
        try (var tmp = new Scratch();
             var server = TcpBroker.start(settings(tmp.root), new Time(), Boundary.NONE);
             var producer = connect(server)) {
            BrokerTest.seed(producer, messages, new byte[256]);
            try (var consumer = connect(server)) {
                long warm = consumer.join("warm", "m", TOPICS).number();
                drain(consumer, warm, Math.min(messages, 256), null);
                consumer.leave();
                long measured = consumer.join("measure", "m", TOPICS).number();
                var durations = new ArrayList<Long>();
                long start = System.nanoTime();
                drain(consumer, measured, messages, durations);
                long elapsed = System.nanoTime() - start;
                eq((long) messages, consumer.group("measure").frontiers().get(ORDERS).next());
                report("batch-pull+accept+ack", durations);
                System.out.printf("batch_messages=%d messages_per_second=%.1f%n",
                        messages, messages * 1_000_000_000d / elapsed);
            }
        }
    }

    private static void slowConsumer() throws Exception {
        try (var tmp = new Scratch();
             var server = TcpBroker.start(settings(tmp.root), new Time(), Boundary.NONE);
             var producer = connect(server); var consumer = connect(server)) {
            BrokerTest.seed(producer, 256, new byte[256]);
            long epoch = consumer.join("slow", "m", TOPICS).number();
            var first = consumer.pull(epoch, 1);
            eq(List.of(0L), BrokerTest.offsets(first));
            var outOfOrder = new ArrayList<Long>();
            drain(consumer, epoch, 127, outOfOrder);
            var frontier = consumer.group("slow").frontiers().get(ORDERS);
            eq(0L, frontier.next());
            eq(127, frontier.ahead());
            var blocked = new ArrayList<Long>();
            for (int i = 0; i < SCANS; i++) {
                long t = System.nanoTime();
                eq(List.of(), consumer.pull(epoch, 16));
                blocked.add(System.nanoTime() - t);
            }
            eq(128L, BrokerTest.ack(consumer, epoch, first).get(ORDERS));
            drain(consumer, epoch, 128, null);
            eq(256L, consumer.group("slow").frontiers().get(ORDERS).next());
            report("slow-consumer-out-of-order-batch", outOfOrder);
            report("slow-consumer-window-blocked-pull", blocked);
        }
    }

    private static void retention(int messages) throws Exception {
        try (var tmp = new Scratch();
             var server = TcpBroker.start(settings(tmp.root, Map.of("orders", 1), 0), new Time(), Boundary.NONE);
             var p = connect(server)) {
            p.join("behind", "m", TOPICS);
            p.leave();
            BrokerTest.seed(p, messages, new byte[1450]);
            int initial = p.partition(ORDERS, 1).segmentCount();
            var pinned = new ArrayList<Long>();
            for (int i = 0; i < SCANS; i++) {
                long t = System.nanoTime();
                eq(0, p.retire());
                pinned.add(System.nanoTime() - t);
            }
            long epoch = p.join("behind", "m", TOPICS).number();
            drain(p, epoch, messages, null);
            long t = System.nanoTime();
            int retired = p.retire();
            long took = System.nanoTime() - t;
            eq(initial - 1, retired);
            eq(1, p.partition(ORDERS, 1).segmentCount());
            report("retention-pinned-scan", pinned);
            System.out.printf("retention_drop segments=%d elapsed_ms=%.3f%n", retired, took / 1_000_000d);
        }
    }

    private static void drain(TideClient client, long epoch, int count, List<Long> durations) throws Exception {
        int remaining = count;
        while (remaining != 0) {
            long t = System.nanoTime();
            var batch = client.pull(epoch, Math.min(16, remaining));
            check(!batch.isEmpty(), "empty batch before workload finished");
            var refs = batch.stream().map(TideClient.Received::lease).map(DeliveryLease::ref).toList();
            client.ack(epoch, refs);
            if (durations != null) {
                durations.add(System.nanoTime() - t);
            }
            remaining -= batch.size();
        }
    }

    private static void report(String label, List<Long> durations) {
        var ascending = durations.stream().sorted().toList();
        System.out.printf("%s samples=%d p50_us=%.1f p95_us=%.1f p99_us=%.1f%n", label, ascending.size(),
                percentile(ascending, 0.5), percentile(ascending, 0.95), percentile(ascending, 0.99));
    }

    private static double percentile(List<Long> ordered, double fraction) {
        int index = (int) Math.ceil(ordered.size() * fraction) - 1;
        return ordered.get(index) / 1000d;
    }
}
