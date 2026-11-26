package tidemq;

import tidemq.delivery.DeliveryLease;
import tidemq.ingress.Ingress;
import tidemq.segment.Segment;
import tidemq.segment.TopicPartition;
import tidemq.wire.TcpBroker;
import tidemq.wire.TideClient;
import tidemq.wire.GroupConsumer;
import tidemq.wire.ResumableProducer;

import java.io.EOFException;
import java.net.InetAddress;
import java.nio.file.Files;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import static tidemq.BrokerException.Code.*;
import static tidemq.TestSupport.*;

public final class BrokerTest {
    private static final TopicPartition P0 = new TopicPartition("orders", 0);
    private static final Set<String> ORDERS = Set.of("orders");

    record Case(String name, Action body) {}

    public static void main(String[] selected) throws Exception {
        var tests = List.of(
                new Case("first-frame-version-and-routed-produce", BrokerTest::negotiation),
                new Case("reconnect-after-lost-production-reply", BrokerTest::lostPublishReply),
                new Case("sdk-retries-the-same-producer-identity", BrokerTest::sdkProducer),
                new Case("concurrent-idempotent-publish", BrokerTest::concurrentProduce),
                new Case("append-before-force-unacknowledged-retry", () -> recoverAppend(Boundary.Point.SEGMENT_BEFORE_FORCE)),
                new Case("durable-append-before-receipt-reconciled", () -> recoverAppend(Boundary.Point.SEGMENT_AFTER_FORCE)),
                new Case("stale-epoch-and-replaced-connection", BrokerTest::epochFence),
                new Case("out-of-order-ack-survives-restart", BrokerTest::ackGap),
                new Case("unobserved-pull-does-not-use-attempt", BrokerTest::lostPullReply),
                new Case("sdk-reconnects-without-reusing-old-batch", BrokerTest::sdkConsumer),
                new Case("frame-budget-does-not-create-lease", BrokerTest::smallFrame),
                new Case("accept-is-idempotent-with-fixed-deadline", BrokerTest::idempotentAccept),
                new Case("lease-timeout-retries-and-terminal-delivery", BrokerTest::retry),
                new Case("retry-backoff-delays-redelivery-by-consumed-attempts", BrokerTest::retryBackoff),
                new Case("retry-backoff-waiting-offset-does-not-block-window", BrokerTest::waitingDoesNotBlockWindow),
                new Case("retry-backoff-survives-reconnect-and-epoch-change", BrokerTest::backoffSurvivesReconnect),
                new Case("retry-backoff-survives-connection-drop", BrokerTest::backoffSurvivesMidLeaseDisconnect),
                new Case("retry-backoff-survives-restart-without-shortening", BrokerTest::backoffSurvivesRestart),
                new Case("reservation-expiry-does-not-backoff", BrokerTest::reservationNoBackoff),
                new Case("terminal-dead-letter-still-follows-backoff", BrokerTest::terminalAfterBackoff),
                new Case("dead-letter-before-force-reconciled", () -> recoverDeadLetter(Boundary.Point.DEADLETTER_BEFORE_FORCE)),
                new Case("dead-letter-force-before-frontier-reconciled", () -> recoverDeadLetter(Boundary.Point.DEADLETTER_AFTER_FORCE)),
                new Case("group-before-replace-preserves-prior-epoch", BrokerTest::abortEpoch),
                new Case("group-commit-before-epoch-reply-reconciled", BrokerTest::recoverEpoch),
                new Case("blocked-retention-and-reentry-after-advance", BrokerTest::retention),
                new Case("legacy-v1-group-snapshot-loads", BrokerTest::legacyGroupSnapshot),
                new Case("retry-backoff-configuration", BrokerTest::retryBackoffConfig),
                new Case("single-process-lock-and-fixed-routes", BrokerTest::lockAndTopology),
                new Case("legacy-v1-normal-incomplete-and-corrupt", Fixtures::check));
        List<String> filters = Arrays.asList(selected);
        int passed = 0;
        for (Case test : tests) {
            if (filters.isEmpty() || filters.contains(test.name())) {
                test.body().run();
                System.out.println("PASS " + test.name());
                passed++;
            }
        }
        if (passed == 0) {
            throw new IllegalArgumentException("no matching tests");
        }
        System.out.println("PASS total=" + passed);
    }

    private static void negotiation() throws Exception {
        try (var tmp = new Scratch();
             var server = TcpBroker.start(settings(tmp.root, Map.of("orders", 3), 1000), new Time(), Boundary.NONE)) {
            String host = InetAddress.getLoopbackAddress().getHostAddress();
            code(UNSUPPORTED_VERSION, () -> TideClient.connect(host, server.port(), 5000, 8192, 2, 2));
            try (var client = TideClient.connect(host, server.port(), 5000, 8192, 1, 2)) {
                eq(1, client.version());
                eq(8192, client.maxFrame());
                eq(Map.of("orders", 3), client.topics());
                client.ping();
                byte[] key = bytes("order-7");
                var receipt = client.produce("p", 0, "orders", Ingress.ROUTE_BY_KEY, key, bytes("created"));
                eq(new TopicPartition("orders", Math.floorMod(Arrays.hashCode(key), 3)), receipt.partition());
                code(UNKNOWN_TOPIC, () -> client.produce("p", 1, "orders", 3, key, bytes("created")));
            }
        }
    }

    private static void lostPublishReply() throws Exception {
        try (var tmp = new Scratch()) {
            var fault = new Fault();
            try (var server = TcpBroker.start(settings(tmp.root), new Time(), fault);
                 var first = connect(server);
                 var retry = connect(server)) {
                fault.arm(Boundary.Point.FRAME_BEFORE_REPLY);
                try {
                    publish(first, "p", 4, bytes("committed"));
                    throw new AssertionError("producer should observe EOF");
                } catch (EOFException expected) {
                    // The durable result preceded the lost reply.
                }
                var replay = publish(retry, "p", 4, bytes("committed"));
                check(replay.replayed(), "reconnect must recover the receipt");
                eq(0L, replay.offset());
                eq(1L, retry.partition(P0, 1).range().nextOffset());
                code(PRODUCER_CONFLICT, () -> publish(retry, "p", 4, bytes("different")));
                eq(1L, publish(retry, "p", 5, bytes("next")).offset());
            }
        }
    }

    private static void concurrentProduce() throws Exception {
        try (var tmp = new Scratch();
             var server = TcpBroker.start(settings(tmp.root), new Time(), Boundary.NONE);
             var threads = Executors.newVirtualThreadPerTaskExecutor()) {
            int producers = 10;
            var ready = new CountDownLatch(producers);
            var start = new CountDownLatch(1);
            var results = new java.util.ArrayList<java.util.concurrent.Future<Boolean>>();
            for (int i = 0; i < producers; i++) {
                results.add(threads.submit(() -> {
                    try (var client = connect(server)) {
                        ready.countDown();
                        check(start.await(5, TimeUnit.SECONDS), "publish barrier expired");
                        var receipt = publish(client, "shared", 0, bytes("same identity"));
                        eq(0L, receipt.offset());
                        return receipt.replayed();
                    }
                }));
            }
            boolean allReady = ready.await(5, TimeUnit.SECONDS);
            start.countDown();
            check(allReady, "clients did not connect");
            int replays = 0;
            for (var result : results) {
                replays += result.get(5, TimeUnit.SECONDS) ? 1 : 0;
            }
            eq(producers - 1, replays);
        }
    }

    private static void sdkProducer() throws Exception {
        var fault = new Fault();
        var connections = new java.util.concurrent.atomic.AtomicInteger();
        try (var tmp = new Scratch(); var server = TcpBroker.start(settings(tmp.root), new Time(), fault);
             var producer = new ResumableProducer("sdk", 9, 2, () -> {
                 var client = connect(server);
                 if (connections.incrementAndGet() == 1) {
                     fault.arm(Boundary.Point.FRAME_BEFORE_REPLY);
                 }
                 return client;
             })) {
            var replay = producer.send("orders", 0, bytes("order"), bytes("same"));
            check(replay.replayed(), "automatic reconnect should replay the same identity");
            eq(9L, replay.sequence());
            eq(0L, replay.offset());
            eq(2, connections.get());
            check(producer.pendingSequence().isEmpty(), "receipt cleared pending send");
            eq(10L, producer.send("orders", 0, bytes("order"), bytes("new")).sequence());
        }
    }

    private static void recoverAppend(Boundary.Point point) throws Exception {
        try (var tmp = new Scratch()) {
            var fault = new Fault();
            var time = new Time();
            var config = settings(tmp.root);
            try (var server = TcpBroker.start(config, time, fault); var client = connect(server)) {
                fault.arm(point);
                code(STORAGE_FAILURE, () -> publish(client, "p", 0, bytes("durable")));
                code(STORAGE_FAILURE, client::topics);
            }
            try (var server = TcpBroker.start(config, time, Boundary.NONE); var client = connect(server)) {
                var receipt = publish(client, "p", 0, bytes("durable"));
                if (point == Boundary.Point.SEGMENT_AFTER_FORCE) {
                    check(receipt.replayed(), "segment recovery must restore missing receipt");
                }
                eq(0L, receipt.offset());
                eq(1L, client.partition(P0, 1).range().nextOffset());
            }
        }
    }

    private static void epochFence() throws Exception {
        try (var tmp = new Scratch();
             var server = TcpBroker.start(settings(tmp.root, Map.of("orders", 3), 1000), new Time(), Boundary.NONE);
             var old = connect(server); var second = connect(server); var replacement = connect(server)) {
            publish(old, "p", 0, bytes("first"));
            var initial = old.join("g", "a", ORDERS);
            var delivered = old.pull(initial.number(), 3);
            eq(1, delivered.size());
            var joined = second.join("g", "b", ORDERS);
            eq(List.of(new TopicPartition("orders", 1)), joined.assignment());
            code(STALE_EPOCH, () -> ack(old, initial.number(), delivered));
            var newEpoch = replacement.join("g", "a", ORDERS);
            check(newEpoch.number() > joined.number(), "same member reconnect must fence old connection");
            code(STALE_EPOCH, old::assignment);
            old.leave();
            eq(newEpoch.number(), replacement.assignment().number());
            var redelivery = replacement.pull(newEpoch.number(), 3);
            eq(2, redelivery.getFirst().lease().attempt());
            eq(1L, ack(replacement, newEpoch.number(), redelivery).get(P0));
        }
    }

    private static void ackGap() throws Exception {
        try (var tmp = new Scratch()) {
            var time = new Time();
            var config = settings(tmp.root);
            long oldGeneration;
            try (var server = TcpBroker.start(config, time, Boundary.NONE); var client = connect(server)) {
                seed(client, 3, bytes("message"));
                oldGeneration = client.join("g", "m", ORDERS).number();
                var batch = client.pull(oldGeneration, 3);
                eq(List.of(0L, 1L, 2L), offsets(batch));
                eq(0L, ack(client, oldGeneration, batch.subList(1, 3)).get(P0));
                eq(2, client.group("g").frontiers().get(P0).ahead());
                client.leave();
            }
            try (var server = TcpBroker.start(config, time, Boundary.NONE); var client = connect(server)) {
                long epoch = client.join("g", "m", ORDERS).number();
                check(epoch > oldGeneration, "reopen must not reuse a generation");
                var batch = client.pull(epoch, 8);
                eq(List.of(0L), offsets(batch));
                eq(2, batch.getFirst().lease().attempt());
                eq(3L, ack(client, epoch, batch).get(P0));
                eq(List.of(), client.pull(epoch, 8));
            }
        }
    }

    private static void lostPullReply() throws Exception {
        try (var tmp = new Scratch()) {
            var fault = new Fault();
            try (var server = TcpBroker.start(settings(tmp.root), new Time(), fault);
                 var first = connect(server); var next = connect(server)) {
                publish(first, "p", 0, bytes("reply interrupted"));
                long epoch = first.join("g", "m", ORDERS).number();
                fault.arm(Boundary.Point.FRAME_BEFORE_REPLY);
                io(() -> first.reserve(epoch, 2));
                long newEpoch = next.join("g", "m", ORDERS).number();
                var deliveries = next.pull(newEpoch, 2);
                eq(1, deliveries.getFirst().lease().attempt());
                ack(next, newEpoch, deliveries);
            }
        }
    }

    private static void smallFrame() throws Exception {
        try (var tmp = new Scratch();
             var server = TcpBroker.start(settings(tmp.root), new Time(), Boundary.NONE);
             var producer = connect(server);
             var small = TideClient.connect(InetAddress.getLoopbackAddress().getHostAddress(), server.port(), 5000, 1024);
             var larger = connect(server)) {
            publish(producer, "p", 0, new byte[1450]);
            long epoch = small.join("g", "m", ORDERS).number();
            code(CAPACITY, () -> small.pull(epoch, 4));
            eq(0, small.group("g").frontiers().get(P0).retrying());
            long fresh = larger.join("g", "m", ORDERS).number();
            var received = larger.pull(fresh, 4);
            eq(1, received.getFirst().lease().attempt());
            eq(1450, received.getFirst().lease().message().payloadSize());
            ack(larger, fresh, received);
        }
    }

    private static void sdkConsumer() throws Exception {
        var fault = new Fault();
        try (var tmp = new Scratch(); var server = TcpBroker.start(settings(tmp.root), new Time(), fault);
             var producer = connect(server); var consumer = new GroupConsumer("g", "m", ORDERS, () -> connect(server))) {
            seed(producer, 2, bytes("work"));
            var old = consumer.poll(1);
            eq(List.of(0L), offsets(old.messages()));
            fault.arm(Boundary.Point.FRAME_BEFORE_REPLY);
            io(() -> consumer.poll(1));
            var active = consumer.poll(2);
            check(active.epoch() > old.epoch(), "new connection did not advance epoch");
            code(STALE_EPOCH, () -> consumer.acknowledge(old));
            eq(List.of(0L, 1L), offsets(active.messages()));
            eq(List.of(2, 1), active.messages().stream().map(value -> value.lease().attempt()).toList());
            eq(2L, consumer.acknowledge(active).get(P0));
        }
    }

    private static void idempotentAccept() throws Exception {
        var time = new Time();
        try (var tmp = new Scratch();
             var server = TcpBroker.start(settings(tmp.root), time, Boundary.NONE);
             var client = connect(server)) {
            publish(client, "p", 0, bytes("accepted"));
            long epoch = client.join("g", "m", ORDERS).number();
            var refs = client.reserve(epoch, 1).stream().map(DeliveryLease::ref).toList();
            eq(1, client.flow("g").reserved());
            eq(0, client.flow("g").accepted());
            code(UNKNOWN_LEASE, () -> client.ack(epoch, refs));
            var accepted = client.accept(epoch, refs);
            eq(0, client.flow("g").reserved());
            eq(1, client.flow("g").accepted());
            time.advance(100);
            eq(accepted, client.accept(epoch, refs));
            eq(1, client.group("g").frontiers().get(P0).retrying());
            eq(1L, client.ack(epoch, refs).get(P0));
        }
    }

    private static void retryBackoff() throws Exception {
        var time = new Time();
        try (var tmp = new Scratch();
             var server = TcpBroker.start(settings(tmp.root, 100, 250), time, Boundary.NONE);
             var client = connect(server)) {
            publish(client, "p", 0, bytes("backoff"));
            long epoch = client.join("g", "m", ORDERS).number();
            var first = client.pull(epoch, 1);
            eq(1, first.getFirst().lease().attempt());
            // Accepting starts the lease; no reservation-only attempt is charged yet.
            time.advance(1000);
            // The consumed accepted attempt anchors a 100ms backoff at the lease deadline.
            eq(List.of(), client.pull(epoch, 1));
            eq(1, client.group("g").frontiers().get(P0).retrying());
            time.advance(99);
            eq(List.of(), client.pull(epoch, 1));
            time.advance(1);
            var second = client.pull(epoch, 1);
            eq(2, second.getFirst().lease().attempt());
            time.advance(1000);
            eq(List.of(), client.pull(epoch, 1));
            // A second consumed attempt doubles the base delay to 200ms.
            time.advance(199);
            eq(List.of(), client.pull(epoch, 1));
            time.advance(1);
            var third = client.pull(epoch, 1);
            eq(3, third.getFirst().lease().attempt());
            // The third attempt reaches the configured maximum; its loss is terminal at once,
            // with no backoff, exactly as before the feature.
            time.advance(1000);
            eq(List.of(), client.pull(epoch, 1));
            var page = client.deadLetters("g", 0, 4);
            eq(1, page.receipts().size());
            eq(3, page.receipts().getFirst().attempts());
            eq(1L, client.group("g").frontiers().get(P0).next());
        }
    }

    private static void waitingDoesNotBlockWindow() throws Exception {
        var time = new Time();
        try (var tmp = new Scratch();
             var server = TcpBroker.start(settings(tmp.root, 500, 500), time, Boundary.NONE);
             var client = connect(server)) {
            seed(client, 2, bytes("message"));
            long epoch = client.join("g", "m", ORDERS).number();
            var batch = client.pull(epoch, 2);
            eq(List.of(0L, 1L), offsets(batch));
            // Acknowledge offset 1 out of order before any lease expires, leaving offset 0 as the
            // unfinished head that will be lost.
            ack(client, epoch, batch.subList(1, 2));
            eq(0L, client.group("g").frontiers().get(P0).next());
            // Offset 0 is lost and starts a backoff anchored at its 1000ms lease deadline.
            time.advance(1000);
            eq(List.of(), client.pull(epoch, 8));
            eq(1, client.group("g").frontiers().get(P0).retrying());
            // The waiting head offset must not starve a fresh offset in the same window.
            eq(2L, publish(client, "p", 2, bytes("fresh")).offset());
            time.advance(299);
            var fresh = client.pull(epoch, 8);
            eq(List.of(2L), offsets(fresh));
            eq(1, fresh.getFirst().lease().attempt());
            ack(client, epoch, fresh);
            time.advance(201);
            var head = client.pull(epoch, 8);
            eq(List.of(0L), offsets(head));
            eq(2, head.getFirst().lease().attempt());
            ack(client, epoch, head);
            eq(List.of(), client.pull(epoch, 8));
            eq(3L, client.group("g").frontiers().get(P0).next());
            eq(0, client.group("g").frontiers().get(P0).retrying());
        }
    }

    private static void backoffSurvivesReconnect() throws Exception {
        var time = new Time();
        try (var tmp = new Scratch();
             var server = TcpBroker.start(settings(tmp.root, 1000, 1000), time, Boundary.NONE);
             var first = connect(server); var second = connect(server)) {
            publish(first, "p", 0, bytes("survives"));
            long epoch = first.join("g", "m", ORDERS).number();
            first.pull(epoch, 1);
            // Let the lease expire deterministically: pull at t+1100 sweeps it and anchors the
            // 1000ms backoff at the t+1000 lease deadline, so it is due at t+2000.
            time.advance(1000);
            eq(List.of(), first.pull(epoch, 1));
            eq(1, first.group("g").frontiers().get(P0).retrying());
            // A same-member reconnect advances the epoch and replaces delivery state. It must
            // neither reset the backoff to zero nor shorten the t+2000 deadline.
            time.advance(100);
            first.close();
            var reconnect = second.join("g", "m", ORDERS);
            check(reconnect.number() > epoch, "reconnect must advance the epoch");
            eq(List.of(), second.pull(reconnect.number(), 1));
            eq(1, second.group("g").frontiers().get(P0).retrying());
            time.advance(899);
            eq(List.of(), second.pull(reconnect.number(), 1));
            time.advance(1);
            var retry = second.pull(reconnect.number(), 1);
            eq(2, retry.getFirst().lease().attempt());
            eq(List.of(0L), offsets(retry));
            ack(second, reconnect.number(), retry);
        }
    }

    private static void backoffSurvivesMidLeaseDisconnect() throws Exception {
        var time = new Time();
        try (var tmp = new Scratch();
             var server = TcpBroker.start(settings(tmp.root, 1000, 1000), time, Boundary.NONE);
             var first = connect(server); var second = connect(server)) {
            publish(first, "p", 0, bytes("drop"));
            long epoch = first.join("g", "m", ORDERS).number();
            first.pull(epoch, 1);
            // Sweep the expired lease at t+1000, anchoring a 1000ms backoff at the lease deadline
            // so the offset is due at t+2000.
            time.advance(1000);
            eq(List.of(), first.pull(epoch, 1));
            eq(1, first.group("g").frontiers().get(P0).retrying());
            // Drop the connection: the persisted deadline must survive the reconnect and epoch
            // change rather than restarting the wait or releasing the offset early.
            first.close();
            var reconnect = second.join("g", "m", ORDERS);
            eq(List.of(), second.pull(reconnect.number(), 1));
            eq(1, second.group("g").frontiers().get(P0).retrying());
            time.advance(999);
            eq(List.of(), second.pull(reconnect.number(), 1));
            time.advance(1);
            var retry = second.pull(reconnect.number(), 1);
            eq(2, retry.getFirst().lease().attempt());
            ack(second, reconnect.number(), retry);
        }
    }

    private static void backoffSurvivesRestart() throws Exception {
        var time = new Time();
        try (var scratch = new Scratch()) {
            var rooted = settings(scratch.root, 1000, 1000);
            try (var server = TcpBroker.start(rooted, time, Boundary.NONE); var client = connect(server)) {
                publish(client, "p", 0, bytes("restart"));
                long epoch = client.join("g", "m", ORDERS).number();
                client.pull(epoch, 1);
                // Sweep the expired lease at t+1000; the backoff is anchored at the same instant,
                // so the deadline is t+2000 and is persisted in the group snapshot.
                time.advance(1000);
                eq(List.of(), client.pull(epoch, 1));
                eq(1, client.group("g").frontiers().get(P0).retrying());
                time.advance(300);
                client.close();
            }
            // Restart with 700ms of the original 1000ms backoff left: the deadline survives and
            // is neither reset nor shortened.
            try (var server = TcpBroker.start(rooted, time, Boundary.NONE); var client = connect(server)) {
                long epoch = client.join("g", "m", ORDERS).number();
                eq(List.of(), client.pull(epoch, 1));
                eq(1, client.group("g").frontiers().get(P0).retrying());
                time.advance(699);
                eq(List.of(), client.pull(epoch, 1));
                time.advance(1);
                var retry = client.pull(epoch, 1);
                eq(2, retry.getFirst().lease().attempt());
                ack(client, epoch, retry);
            }
        }
    }

    private static void reservationNoBackoff() throws Exception {
        var time = new Time();
        try (var tmp = new Scratch();
             var server = TcpBroker.start(settings(tmp.root, 5000, 5000), time, Boundary.NONE);
             var client = connect(server)) {
            publish(client, "p", 0, bytes("reserve"));
            long epoch = client.join("g", "m", ORDERS).number();
            // Reserve without accepting: the attempt counter must stay at zero.
            client.reserve(epoch, 1);
            eq(0, client.group("g").frontiers().get(P0).retrying());
            time.advance(1000);
            // The expired reservation frees the slot with no backoff: the very next pull is
            // immediately available at attempt 1.
            var retry = client.pull(epoch, 1);
            eq(1, retry.getFirst().lease().attempt());
            ack(client, epoch, retry);
            eq(0, client.group("g").frontiers().get(P0).retrying());
        }
    }

    private static void terminalAfterBackoff() throws Exception {
        var time = new Time();
        try (var tmp = new Scratch();
             var server = TcpBroker.start(settings(tmp.root, 100, 100, 2), time, Boundary.NONE);
             var client = connect(server)) {
            publish(client, "p", 0, bytes("terminal"));
            long epoch = client.join("g", "m", ORDERS).number();
            client.pull(epoch, 1);
            time.advance(1000);
            eq(List.of(), client.pull(epoch, 1));
            time.advance(100);
            var second = client.pull(epoch, 1);
            eq(2, second.getFirst().lease().attempt());
            // The second attempt reaches the maximum. Expiry is terminal immediately — no extra
            // waiting state — and the dead letter is written once.
            time.advance(1000);
            eq(List.of(), client.pull(epoch, 1));
            var page = client.deadLetters("g", 0, 4);
            eq(1, page.receipts().size());
            eq(2, page.receipts().getFirst().attempts());
            eq(1L, client.group("g").frontiers().get(P0).next());
            eq(0, client.group("g").frontiers().get(P0).retrying());
        }
    }

    private static void retry() throws Exception {
        var time = new Time();
        try (var tmp = new Scratch();
             var server = TcpBroker.start(settings(tmp.root), time, Boundary.NONE);
             var client = connect(server)) {
            publish(client, "p", 0, bytes("unhandled"));
            long epoch = client.join("g", "m", ORDERS).number();
            var first = client.pull(epoch, 1);
            eq(1, first.getFirst().lease().attempt());
            time.advance(1000);
            code(UNKNOWN_LEASE, () -> ack(client, epoch, first));
            var next = client.pull(epoch, 1);
            eq(2, next.getFirst().lease().attempt());
            time.advance(1000);
            eq(List.of(), client.pull(epoch, 1));
            var page = client.deadLetters("g", 0, 1);
            eq(1, page.receipts().size());
            eq(2, page.receipts().getFirst().attempts());
            eq(next.getFirst().lease().message(), page.receipts().getFirst().message());
            eq(1L, client.group("g").frontiers().get(P0).next());
            eq(List.of(), client.deadLetters("g", page.nextSequence(), 1).receipts());
        }
    }

    private static void recoverDeadLetter(Boundary.Point point) throws Exception {
        try (var tmp = new Scratch()) {
            var fault = new Fault();
            var time = new Time();
            var config = settings(tmp.root);
            try (var server = TcpBroker.start(config, time, fault); var client = connect(server)) {
                publish(client, "p", 0, bytes("unhandled"));
                long epoch = client.join("g", "m", ORDERS).number();
                client.pull(epoch, 1);
                time.advance(1000);
                client.pull(epoch, 1);
                fault.arm(point);
                time.advance(1000);
                code(STORAGE_FAILURE, () -> client.pull(epoch, 1));
            }
            try (var server = TcpBroker.start(config, time, Boundary.NONE); var client = connect(server)) {
                long epoch = client.join("g", "m", ORDERS).number();
                eq(List.of(), client.pull(epoch, 1));
                eq(1L, client.group("g").frontiers().get(P0).next());
                eq(1, client.deadLetters("g", 0, 8).receipts().size());
            }
        }
    }

    private static void abortEpoch() throws Exception {
        try (var tmp = new Scratch()) {
            var fault = new Fault();
            var time = new Time();
            var config = settings(tmp.root);
            try (var server = TcpBroker.start(config, time, fault); var first = connect(server); var second = connect(server)) {
                eq(1L, first.join("g", "a", ORDERS).number());
                fault.arm(Boundary.Point.GROUP_BEFORE_REPLACE);
                code(STORAGE_FAILURE, () -> second.join("g", "b", ORDERS));
            }
            try (var server = TcpBroker.start(config, time, Boundary.NONE); var client = connect(server)) {
                eq(1L, client.group("g").epoch());
                eq(2L, client.join("g", "a", ORDERS).number());
            }
        }
    }

    private static void recoverEpoch() throws Exception {
        try (var tmp = new Scratch()) {
            var fault = new Fault();
            var time = new Time();
            var config = settings(tmp.root);
            try (var server = TcpBroker.start(config, time, fault); var client = connect(server)) {
                fault.arm(Boundary.Point.GROUP_AFTER_REPLACE);
                code(STORAGE_FAILURE, () -> client.join("g", "m", ORDERS));
            }
            try (var server = TcpBroker.start(config, time, Boundary.NONE); var client = connect(server)) {
                eq(1L, client.group("g").epoch());
                eq(Map.of(), client.group("g").assignments());
                eq(2L, client.join("g", "m", ORDERS).number());
            }
        }
    }

    private static void legacyGroupSnapshot() throws Exception {
        var time = new Time();
        try (var tmp = new Scratch()) {
            var config = settings(tmp.root);
            try (var server = TcpBroker.start(config, time, Boundary.NONE); var client = connect(server)) {
                publish(client, "p", 0, bytes("legacy"));
                long epoch = client.join("g", "m", ORDERS).number();
                ack(client, epoch, client.pull(epoch, 1));
            }
            // The broker wrote a v2 snapshot. Rewrite it as the v1 layout by dropping the
            // per-partition retry-deadline count for the single subscribed partition.
            java.nio.file.Path path = tmp.root.resolve("groups/g.group");
            byte[] snapshot = java.nio.file.Files.readAllBytes(path);
            try (var in = new java.io.DataInputStream(new java.io.ByteArrayInputStream(snapshot))) {
                int magic = in.readInt();
                int version = in.readInt();
                eq(0x544D4750, magic);
                eq(2, version);
                int bodyLength = in.readInt();
                byte[] body = in.readNBytes(bodyLength);
                eq(bodyLength, body.length);
                // A single partition ends with i32 retry-deadline count 0.
                byte[] legacyBody = java.util.Arrays.copyOf(body, body.length - 4);
                var rewritten = tidemq.Binary.encode(bytesOut -> {
                    bytesOut.writeInt(magic);
                    bytesOut.writeInt(1);
                    tidemq.Binary.writeBytes(bytesOut, legacyBody);
                    bytesOut.writeInt(tidemq.segment.Disk.checksum(legacyBody));
                });
                java.nio.file.Files.write(path, rewritten);
            }
            // Reopening must accept the v1 snapshot and preserve the frontier.
            try (var server = TcpBroker.start(config, time, Boundary.NONE); var client = connect(server)) {
                eq(1L, client.group("g").frontiers().get(P0).next());
                eq(0, client.group("g").frontiers().get(P0).retrying());
                long epoch = client.join("g", "m", ORDERS).number();
                eq(List.of(), client.pull(epoch, 8));
            }
        }
    }

    private static void retryBackoffConfig() throws Exception {
        try (var tmp = new Scratch()) {
            java.nio.file.Path properties = tmp.root.resolve("broker.properties");
            java.nio.file.Files.writeString(properties, """
                    listen.host=127.0.0.1
                    listen.port=0
                    data.directory=%s
                    topics=orders:1
                    delivery.retry.backoff.ms=100
                    delivery.retry.backoff.max.ms=1000
                    """.formatted(tmp.root.resolve("data")));
            var settings = Settings.load(properties);
            eq(100L, settings.retryDelay(1));
            eq(200L, settings.retryDelay(2));
            eq(400L, settings.retryDelay(3));
            eq(800L, settings.retryDelay(4));
            eq(1000L, settings.retryDelay(5));
            eq(1000L, settings.retryDelay(1000));
            eq(0L, settings.retryDelay(0));
            java.nio.file.Files.writeString(properties, """
                    listen.host=127.0.0.1
                    listen.port=0
                    data.directory=%s
                    topics=orders:1
                    delivery.retry.backoff.ms=0
                    """.formatted(tmp.root.resolve("disabled")));
            var disabled = Settings.load(properties);
            eq(0L, disabled.retryDelay(1));
            eq(0L, disabled.retryDelay(4));
            java.nio.file.Files.writeString(properties, """
                    listen.host=127.0.0.1
                    listen.port=0
                    data.directory=%s
                    topics=orders:1
                    delivery.retry.backoff.ms=500
                    delivery.retry.backoff.max.ms=100
                    """.formatted(tmp.root.resolve("invalid")));
            try {
                Settings.load(properties);
                throw new AssertionError("inverted backoff bounds must be rejected");
            } catch (IllegalArgumentException expected) {
                // The cap cannot be smaller than the base.
            }
        }
    }

    private static void retention() throws Exception {
        try (var tmp = new Scratch()) {
            var time = new Time();
            var config = settings(tmp.root);
            byte[] payload = new byte[1450];
            try (var server = TcpBroker.start(config, time, Boundary.NONE);
                 var fast = connect(server); var slow = connect(server)) {
                long fastEpoch = fast.join("fast", "m", ORDERS).number();
                slow.join("slow", "m", ORDERS);
                slow.leave();
                seed(fast, 7, payload);
                var before = fast.partition(P0, 20);
                eq(4, before.segmentCount());
                eq(7L, ack(fast, fastEpoch, fast.pull(fastEpoch, 16)).get(P0));
                time.advance(1000);
                var pinned = fast.retention(P0);
                eq(List.of("slow"), pinned.limitingGroups());
                eq(0, pinned.eligibleSegments());
                check(pinned.aboveGroupFrontier(), "segment must be pinned by group");
                eq(0, fast.retire());
                long slowEpoch = slow.join("slow", "m", ORDERS).number();
                var slowBatch = slow.pull(slowEpoch, 16);
                eq(List.of(0L, 1L, 2L, 3L, 4L, 5L, 6L), offsets(slowBatch));
                eq(7L, ack(slow, slowEpoch, slowBatch).get(P0));
                var ready = fast.retention(P0);
                eq(3, ready.eligibleSegments());
                eq(6L, ready.resultingLow());
                check(ready.keepsActive(), "preview must keep active segment");
                eq(4, fast.partition(P0, 20).segmentCount());
                eq(3, fast.retire());
                var after = fast.partition(P0, 20);
                eq(6L, after.range().retainedFrom());
                eq(1, after.segmentCount());
                check(!Files.exists(tmp.root.resolve("segments/orders/0").resolve(Segment.fileName(0))),
                        "retired segment is still on disk");
            }
            try (var server = TcpBroker.start(config, time, Boundary.NONE); var client = connect(server)) {
                check(publish(client, "p", 0, payload).replayed(), "retired producer receipt was lost");
                long start = client.join("new", "m", ORDERS).number();
                eq(List.of(6L), offsets(client.pull(start, 5)));
            }
        }
    }

    private static void lockAndTopology() throws Exception {
        try (var tmp = new Scratch()) {
            var config = settings(tmp.root);
            var time = new Time();
            try (var server = TcpBroker.start(config, time, Boundary.NONE); var client = connect(server)) {
                io(() -> TcpBroker.start(config, time, Boundary.NONE));
                client.ping();
            }
            var changed = settings(tmp.root, Map.of("orders", 2), 1000);
            io(() -> TcpBroker.start(changed, time, Boundary.NONE));
            try (var server = TcpBroker.start(config, time, Boundary.NONE); var client = connect(server)) {
                eq(Map.of("orders", 1), client.topics());
            }
        }
    }

    static List<Long> offsets(List<TideClient.Received> messages) {
        return messages.stream().map(item -> item.lease().message().offset()).toList();
    }

    static Map<TopicPartition, Long> ack(TideClient client, long epoch, List<TideClient.Received> data)
            throws Exception {
        return client.ack(epoch, data.stream().map(TideClient.Received::lease).map(DeliveryLease::ref).toList());
    }

    static void seed(TideClient client, int count, byte[] payload) throws Exception {
        for (int i = 0; i < count; i++) {
            publish(client, "p", i, payload);
        }
    }

    static tidemq.ingress.ProduceReceipt publish(TideClient client, String producer, long sequence, byte[] payload)
            throws Exception {
        return client.produce(producer, sequence, "orders", 0, bytes("order"), payload);
    }
}
