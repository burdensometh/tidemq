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
                new Case("consumer-terminates-accepted-delivery", BrokerTest::terminateDelivery),
                new Case("terminate-closes-only-its-own-hole", BrokerTest::terminateHole),
                new Case("terminate-requires-accepted-current-lease", BrokerTest::terminateGuards),
                new Case("terminate-before-deadletter-force-reconciled",
                        () -> recoverTerminate(Boundary.Point.DEADLETTER_BEFORE_FORCE)),
                new Case("terminate-before-group-snapshot-reconciled",
                        () -> recoverTerminate(Boundary.Point.DEADLETTER_AFTER_FORCE)),
                new Case("duplicate-terminate-rejected", BrokerTest::duplicateTerminate),
                new Case("dead-letter-before-force-reconciled", () -> recoverDeadLetter(Boundary.Point.DEADLETTER_BEFORE_FORCE)),
                new Case("dead-letter-force-before-frontier-reconciled", () -> recoverDeadLetter(Boundary.Point.DEADLETTER_AFTER_FORCE)),
                new Case("group-before-replace-preserves-prior-epoch", BrokerTest::abortEpoch),
                new Case("group-commit-before-epoch-reply-reconciled", BrokerTest::recoverEpoch),
                new Case("blocked-retention-and-reentry-after-advance", BrokerTest::retention),
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

    private static void terminateDelivery() throws Exception {
        try (var tmp = new Scratch();
             var server = TcpBroker.start(settings(tmp.root), new Time(), Boundary.NONE);
             var client = connect(server)) {
            seed(client, 2, bytes("poison"));
            long epoch = client.join("g", "m", ORDERS).number();
            var batch = client.pull(epoch, 2);
            eq(List.of(0L, 1L), offsets(batch));
            var poison = batch.getFirst();
            var frontiers = client.terminate(epoch, List.of(poison.lease().ref()));
            long next = frontiers.get(P0);
            eq(1L, next);
            var page = client.deadLetters("g", 0, 8);
            eq(1, page.receipts().size());
            var receipt = page.receipts().getFirst();
            eq(0L, receipt.sequence());
            eq(1, receipt.attempts());
            eq(poison.lease().message(), receipt.message());
            eq(P0, receipt.message().partition());
            eq(0L, receipt.message().offset());
            var view = client.group("g").frontiers().get(P0);
            eq(1L, view.next());
            eq(0, view.ahead());
            eq(1, view.retrying());
            // The terminated lease is released and never redelivered; the remaining offset ACKs
            // through its existing lease and advances the continuous watermark to the end.
            eq(2L, ack(client, epoch, batch.subList(1, 2)).get(P0));
            eq(List.of(), client.pull(epoch, 4));
        }
    }

    private static void terminateHole() throws Exception {
        try (var tmp = new Scratch();
             var server = TcpBroker.start(settings(tmp.root), new Time(), Boundary.NONE);
             var client = connect(server)) {
            seed(client, 3, bytes("work"));
            long epoch = client.join("g", "m", ORDERS).number();
            var batch = client.pull(epoch, 3);
            // Terminate the highest offset: it closes only its own hole, the watermark stays.
            var high = List.of(batch.get(2).lease().ref());
            long holeNext = client.terminate(epoch, high).get(P0);
            eq(0L, holeNext);
            eq(1, client.deadLetters("g", 0, 8).receipts().size());
            var afterHole = client.group("g").frontiers().get(P0);
            eq(0L, afterHole.next());
            eq(1, afterHole.ahead());
            eq(2, afterHole.retrying());
            // ACK the lowest offset: the continuous prefix reaches the hole at offset 1.
            long firstAck = ack(client, epoch, batch.subList(0, 1)).get(P0);
            eq(1L, firstAck);
            // Filling the last hole advances the watermark past every resolved offset.
            long secondAck = ack(client, epoch, batch.subList(1, 2)).get(P0);
            eq(3L, secondAck);
            eq(3L, client.group("g").frontiers().get(P0).next());
            eq(List.of(), client.pull(epoch, 4));
        }
    }

    private static void terminateGuards() throws Exception {
        try (var tmp = new Scratch();
             var server = TcpBroker.start(settings(tmp.root), new Time(), Boundary.NONE);
             var first = connect(server);
             var replacement = connect(server)) {
            publish(first, "p", 0, bytes("guarded"));
            long epoch = first.join("g", "m", ORDERS).number();
            // A reservation that was never accepted cannot be terminated.
            var reserved = first.reserve(epoch, 1);
            eq(1, reserved.size());
            code(UNKNOWN_LEASE, () -> first.terminate(epoch, List.of(reserved.getFirst().ref())));
            // Accepting the same delivery turns the reservation into an owned lease.
            var accepted = first.accept(epoch, List.of(reserved.getFirst().ref()));
            eq(1, accepted.size());
            // A membership change advances the generation; the old connection is fenced off.
            long fenced = replacement.join("g", "m", ORDERS).number();
            check(fenced > epoch, "reconnect must advance the generation");
            code(STALE_EPOCH, () -> first.terminate(epoch, List.of(reserved.getFirst().ref())));
            // The new generation redelivers the lost lease; only current accepted leases terminate.
            var redelivery = replacement.pull(fenced, 1);
            eq(1, redelivery.size());
            var bogus = new DeliveryLease.Ref(999_999L, P0, 0L);
            code(UNKNOWN_LEASE, () -> replacement.terminate(fenced, List.of(bogus)));
            var frontiers = replacement.terminate(fenced, List.of(redelivery.getFirst().lease().ref()));
            long next = frontiers.get(P0);
            eq(1L, next);
            // Repeating the same termination finds no active lease.
            code(UNKNOWN_LEASE, () -> replacement.terminate(fenced,
                    List.of(redelivery.getFirst().lease().ref())));
        }
    }

    private static void recoverTerminate(Boundary.Point point) throws Exception {
        try (var tmp = new Scratch()) {
            var fault = new Fault();
            var time = new Time();
            var config = settings(tmp.root);
            try (var server = TcpBroker.start(config, time, fault); var client = connect(server)) {
                seed(client, 2, bytes("fatal"));
                long epoch = client.join("g", "m", ORDERS).number();
                var batch = client.pull(epoch, 2);
                fault.arm(point);
                code(STORAGE_FAILURE,
                        () -> client.terminate(epoch, List.of(batch.getFirst().lease().ref())));
            }
            try (var server = TcpBroker.start(config, time, Boundary.NONE); var client = connect(server)) {
                long epoch = client.join("g", "m", ORDERS).number();
                // The source-bearing dead letter is queryable before any watermark movement.
                var page = client.deadLetters("g", 0, 8);
                eq(1, page.receipts().size());
                eq(0L, page.receipts().getFirst().message().offset());
                eq(P0, page.receipts().getFirst().message().partition());
                // Recovery closed the terminated hole; the remaining offset is still deliverable.
                var redelivery = client.pull(epoch, 4);
                eq(List.of(1L), offsets(redelivery));
                eq(2L, ack(client, epoch, redelivery).get(P0));
                eq(List.of(), client.pull(epoch, 4));
            }
        }
    }

    private static void duplicateTerminate() throws Exception {
        try (var tmp = new Scratch();
             var server = TcpBroker.start(settings(tmp.root), new Time(), Boundary.NONE);
             var client = connect(server)) {
            seed(client, 1, bytes("once"));
            long epoch = client.join("g", "m", ORDERS).number();
            var ref = client.pull(epoch, 1).getFirst().lease().ref();
            eq(1, client.terminate(epoch, List.of(ref)).size());
            code(UNKNOWN_LEASE, () -> client.terminate(epoch, List.of(ref)));
            eq(1, client.deadLetters("g", 0, 8).receipts().size());
        }
    }

    private static void abortEpoch() throws Exception {        try (var tmp = new Scratch()) {
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
