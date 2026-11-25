package tidemq;

import tidemq.delivery.DeliveryLease;
import tidemq.segment.TopicPartition;
import tidemq.wire.TideClient;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Set;

import static tidemq.TestSupport.*;

public final class BrokerDemo {
    private static final TopicPartition FIRST = new TopicPartition("orders", 0);
    private static final Set<String> ORDERS = Set.of("orders");

    public static void main(String[] args) throws Exception {
        String host = args[0];
        int port = Integer.parseInt(args[1]);
        try (var producerA = TideClient.connect(host, port);
             var producerB = TideClient.connect(host, port);
             var workerA = TideClient.connect(host, port);
             var workerB = TideClient.connect(host, port);
             var auditor = TideClient.connect(host, port)) {
            byte[] payload = new byte[1450];
            Arrays.fill(payload, (byte) 'a');
            produce(producerA, producerB, payload);
            workGroups(workerA, workerB, auditor);
            producerB.close();
            try (var reconnected = TideClient.connect(host, port)) {
                var replay = reconnected.produce("warehouse", 0, "orders", 0, bytes("key"), payload);
                check(replay.replayed(), "retired producer receipt unavailable");
                System.out.printf("producer-reconnect producer=%s offset=%d replayed=%s%n",
                        replay.producer(), replay.offset(), replay.replayed());
            }
        }
        System.out.println("DEMO PASS");
    }

    private static void produce(TideClient a, TideClient b, byte[] payload) throws Exception {
        long sequence = 0;
        for (int partition = 0; partition < 3; partition++) {
            int count = partition == 0 ? 6 : 2;
            for (int i = 0; i < count; i++, sequence++) {
                boolean warehouse = sequence % 2 == 0;
                String producer = warehouse ? "warehouse" : "store";
                var client = warehouse ? b : a;
                var receipt = client.produce(producer, sequence, "orders", partition, bytes("key"), payload);
                eq((long) i, receipt.offset());
                System.out.printf("produced producer=%s sequence=%d partition=%s offset=%d%n",
                        producer, sequence, receipt.partition(), receipt.offset());
            }
        }
    }

    private static void workGroups(TideClient a, TideClient b, TideClient audit) throws Exception {
        var initial = a.join("work", "a", ORDERS);
        var second = b.join("work", "b", ORDERS);
        long auditEpoch = audit.join("audit", "c", ORDERS).number();
        code(BrokerException.Code.STALE_EPOCH, () -> a.pull(initial.number(), 1));
        var current = a.assignment();
        System.out.printf("assignments epoch=%d a=%s b=%s audit=%s%n", current.number(),
                current.assignment(), second.assignment(), audit.assignment().assignment());

        var batchA = a.pull(current.number(), 16);
        List<TideClient.Received> process = new ArrayList<>();
        TideClient.Received poison = null;
        for (var delivery : batchA) {
            var message = delivery.lease().message();
            printDelivery("work", "a", delivery);
            if (message.partition().equals(FIRST) && message.offset() == 0) {
                poison = delivery;
            } else {
                process.add(delivery);
            }
        }
        check(poison != null, "poison message missing");
        var frontiers = BrokerTest.ack(a, current.number(), process);
        eq(0L, frontiers.get(FIRST));
        System.out.printf("out-of-order group=work partition=%s next=%d%n", FIRST, frontiers.get(FIRST));
        var batchB = b.pull(second.number(), 16);
        batchB.forEach(received -> printDelivery("work", "b", received));
        BrokerTest.ack(b, second.number(), batchB);
        waitFor(poison.expiresAtMs());
        var retry = a.pull(current.number(), 16);
        eq(1, retry.size());
        eq(2, retry.getFirst().lease().attempt());
        printDelivery("work", "a", retry.getFirst());
        waitFor(retry.getFirst().expiresAtMs());
        eq(List.of(), a.pull(current.number(), 16));
        var dead = a.deadLetters("work", 0, 16).receipts();
        eq(1, dead.size());
        System.out.printf("terminal group=work partition=%s offset=%d attempts=%d work_next=%d%n",
                dead.getFirst().message().partition(), dead.getFirst().message().offset(), dead.getFirst().attempts(),
                a.group("work").frontiers().get(FIRST).next());

        eq(0, a.retire());
        System.out.printf("retention blocked audit_next=%d p0_segments=%d%n",
                audit.group("audit").frontiers().get(FIRST).next(), a.partition(FIRST, 16).segmentCount());
        b.leave();
        long onlyA = a.assignment().number();
        long rejoin = b.join("work", "b", ORDERS).number();
        code(BrokerException.Code.STALE_EPOCH, () -> a.pull(onlyA, 1));
        check(rejoin > onlyA, "rejoin did not fence prior epoch");
        System.out.printf("rebalanced leave_epoch=%d rejoin_epoch=%d%n", onlyA, rejoin);

        var all = audit.pull(auditEpoch, 16);
        eq(10, all.size());
        all.forEach(received -> printDelivery("audit", "c", received));
        BrokerTest.ack(audit, auditEpoch, all);
        eq(2, a.retire());
        eq(4L, a.partition(FIRST, 16).range().retainedFrom());
        audit.leave();
        long afterRejoin = audit.join("audit", "c", ORDERS).number();
        eq(List.of(), audit.pull(afterRejoin, 16));
        System.out.printf("retention released p0_low=%d p0_segments=%d audit_next=%d%n",
                a.partition(FIRST, 16).range().retainedFrom(), a.partition(FIRST, 16).segmentCount(),
                audit.group("audit").frontiers().get(FIRST).next());
    }

    private static void waitFor(long deadline) throws InterruptedException {
        Thread.sleep(Math.max(0, deadline - System.currentTimeMillis()) + 30);
    }

    private static void printDelivery(String group, String member, TideClient.Received item) {
        DeliveryLease lease = item.lease();
        System.out.printf("delivered group=%s member=%s partition=%s offset=%d attempt=%d%n",
                group, member, lease.message().partition(), lease.message().offset(), lease.attempt());
    }
}
