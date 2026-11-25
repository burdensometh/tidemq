package tidemq;

import tidemq.group.GroupDirectory;
import tidemq.ingress.Ingress;
import tidemq.ingress.ProduceReceipt;
import tidemq.segment.Message;
import tidemq.segment.TopicPartition;
import tidemq.wire.GroupConsumer;
import tidemq.wire.ResumableProducer;
import tidemq.wire.TideClient;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.Base64;
import java.util.Set;
import java.util.TreeSet;

public final class ClientMain {
    private static final Base64.Encoder B64 = Base64.getEncoder();

    private ClientMain() {}

    public static void main(String[] args) {
        try {
            run(args, System.out);
        } catch (BrokerException failure) {
            System.err.printf("%s %s%n", failure.code(), failure.getMessage());
            System.exit(1);
        } catch (IllegalArgumentException failure) {
            System.err.printf("Usage error: %s%n", failure.getMessage());
            System.exit(2);
        } catch (IOException failure) {
            System.err.printf("I/O error: %s%n", failure.getMessage());
            System.exit(1);
        } catch (InterruptedException interrupted) {
            Thread.currentThread().interrupt();
            System.exit(130);
        }
    }

    private static void run(String[] args, PrintStream output) throws IOException, InterruptedException {
        if (args.length == 0 || args[0].equals("--help")) {
            output.print("""
                    java -cp build/tidemq.jar tidemq.ClientMain HOST PORT COMMAND
                      ping
                      topics
                      produce PRODUCER SEQUENCE TOPIC PARTITION|- KEY VALUE
                      produce-file PRODUCER SEQUENCE TOPIC PARTITION|- KEY PATH
                      produce-lines PRODUCER FIRST_SEQUENCE TOPIC PARTITION|- KEY
                      consume GROUP MEMBER TOPIC[,TOPIC...] COUNT [IDLE_POLLS]
                      group GROUP
                      flow GROUP
                      partition TOPIC PARTITION [SEGMENTS]
                      deadletters GROUP [CURSOR [PAGE_SIZE]]
                      retention TOPIC PARTITION
                      retire

                    PARTITION '-' routes by UTF-8 key. produce-lines reads one UTF-8 message per stdin line.
                    consume writes base64 records to stdout and acknowledges after flushing them.
                    """);
            return;
        }
        if (args.length < 3) {
            throw new IllegalArgumentException("HOST, PORT and COMMAND are required");
        }
        int port = bounded(args[1], 1, 65535, "port");
        var commandArgs = Arrays.copyOfRange(args, 3, args.length);
        if (args[2].equals("produce-lines")) {
            arity(args[2], commandArgs, 5, 5);
            produceLines(() -> TideClient.connect(args[0], port), commandArgs, output);
            return;
        }
        if (args[2].equals("consume")) {
            arity(args[2], commandArgs, 4, 5);
            int count = bounded(commandArgs[3], 1, Integer.MAX_VALUE, "message count");
            int idle = commandArgs.length == 5 ? bounded(commandArgs[4], 1, 3600, "idle polls") : 10;
            try (var consumer = new GroupConsumer(commandArgs[0], commandArgs[1], subscriptions(commandArgs[2]),
                    () -> TideClient.connect(args[0], port))) {
                consume(consumer, commandArgs[0], commandArgs[1], count, idle, output);
            }
            return;
        }
        try (TideClient client = TideClient.connect(args[0], port)) {
            execute(client, args[2], commandArgs, output);
        }
    }

    private static void execute(TideClient client, String command, String[] args, PrintStream out)
            throws IOException, InterruptedException {
        switch (command) {
            case "ping" -> {
                arity(command, args, 0, 0);
                client.ping();
                out.printf("ok version=%d max_frame=%d max_batch=%d%n",
                        client.version(), client.maxFrame(), client.maxBatch());
            }
            case "topics" -> {
                arity(command, args, 0, 0);
                client.topics().forEach((topic, count) -> out.printf("%s partitions=%d%n", topic, count));
            }
            case "produce", "produce-file" -> {
                arity(command, args, 6, 6);
                byte[] payload = command.equals("produce") ? utf8(args[5]) : readPayload(Path.of(args[5]));
                printReceipt(out, produce(client, args, nonnegative(args[1], "producer sequence"), payload));
            }
            case "group" -> {
                arity(command, args, 1, 1);
                printGroup(out, client.group(args[0]));
            }
            case "flow" -> {
                arity(command, args, 1, 1);
                var view = client.flow(args[0]);
                out.printf("group=%s epoch=%d observed_ms=%d window=%d inflight_limit=%d members=%d reserved=%d accepted=%d%n",
                        view.group(), view.epoch(), view.observedAtMs(), view.deliveryWindow(), view.maxInflightPerMember(),
                        view.members(), view.reserved(), view.accepted());
                for (var member : view.sample()) {
                    out.printf("member=%s partitions=%d reserved=%d accepted=%d earliest_deadline_ms=%d%n",
                            member.name(), member.partitions(), member.reserved(), member.accepted(), member.earliestDeadlineMs());
                }
            }
            case "partition" -> {
                arity(command, args, 2, 3);
                int limit = args.length == 3 ? bounded(args[2], 1, 128, "segment count") : 16;
                printPartition(out, client, new TopicPartition(args[0], Integer.parseInt(args[1])), limit);
            }
            case "deadletters" -> {
                arity(command, args, 1, 3);
                long cursor = args.length > 1 ? nonnegative(args[1], "cursor") : 0;
                int limit = args.length > 2 ? bounded(args[2], 1, 256, "page size") : 16;
                var page = client.deadLetters(args[0], cursor, limit);
                for (var receipt : page.receipts()) {
                    out.printf("deadletter=%d group=%s attempts=%d terminal_ms=%d ",
                            receipt.sequence(), receipt.group(), receipt.attempts(), receipt.terminalAtMs());
                    printMessage(out, receipt.message());
                }
                out.printf("cursor=%d count=%d%n", page.nextSequence(), page.receipts().size());
            }
            case "retire" -> {
                arity(command, args, 0, 0);
                out.printf("retired=%d%n", client.retire());
            }
            case "retention" -> {
                arity(command, args, 2, 2);
                printRetention(out, client, new TopicPartition(args[0], Integer.parseInt(args[1])));
            }
            default -> throw new IllegalArgumentException("unknown command " + command);
        }
    }

    private static byte[] readPayload(Path path) throws IOException {
        try (var input = Files.newInputStream(path)) {
            byte[] bytes = input.readNBytes(tidemq.segment.MessageV1.MAX_PAYLOAD + 1);
            if (bytes.length > tidemq.segment.MessageV1.MAX_PAYLOAD) {
                throw new IOException("payload file exceeds protocol limit");
            }
            return bytes;
        }
    }

    private static void produceLines(TideClient.Connector connect, String[] args, PrintStream out)
            throws IOException {
        long sequence = nonnegative(args[1], "first sequence");
        var input = new BufferedReader(new InputStreamReader(System.in, StandardCharsets.UTF_8));
        int route = route(args[3]);
        byte[] key = utf8(args[4]);
        try (var producer = new ResumableProducer(args[0], sequence, 2, connect)) {
            String line;
            while ((line = input.readLine()) != null) {
                printReceipt(out, producer.send(args[2], route, key, utf8(line)));
                checkOutput(out);
            }
        }
    }

    private static ProduceReceipt produce(TideClient client, String[] args, long sequence, byte[] bytes)
            throws IOException {
        return client.produce(args[0], sequence, args[2], route(args[3]), utf8(args[4]), bytes);
    }

    private static int route(String value) {
        return value.equals("-") ? Ingress.ROUTE_BY_KEY : Integer.parseInt(value);
    }

    private static void consume(GroupConsumer consumer, String group, String member,
                                int count, int emptyLimit, PrintStream out) throws IOException, InterruptedException {
        int empty = 0;
        int remaining = count;
        while (remaining > 0 && empty < emptyLimit) {
            GroupConsumer.Batch batch = consumer.poll(remaining);
            if (batch.isEmpty()) {
                empty++;
                Thread.sleep(100);
                continue;
            }
            for (var received : batch.messages()) {
                out.printf("group=%s member=%s epoch=%d attempt=%d ",
                        group, member, batch.epoch(), received.lease().attempt());
                printMessage(out, received.lease().message());
            }
            checkOutput(out);
            consumer.acknowledge(batch);
            remaining -= batch.messages().size();
            empty = 0;
        }
        if (remaining != 0) {
            throw new IOException("acknowledged " + (count - remaining) + " of " + count + " requested messages");
        }
    }

    private static void printReceipt(PrintStream out, ProduceReceipt receipt) {
        out.printf("producer=%s sequence=%d partition=%s offset=%d appended_ms=%d replayed=%s%n",
                receipt.producer(), receipt.sequence(), receipt.partition(), receipt.offset(), receipt.appendedAtMs(),
                receipt.replayed());
    }

    static void printMessage(PrintStream out, Message message) {
        out.printf("partition=%s offset=%d producer=%s sequence=%d key_b64=%s payload_b64=%s%n",
                message.partition(), message.offset(), message.producer(), message.sequence(),
                B64.encodeToString(message.key()), B64.encodeToString(message.payload()));
    }

    private static void printGroup(PrintStream out, GroupDirectory.View view) {
        out.printf("group=%s epoch=%d subscriptions=%s%n", view.name(), view.epoch(), view.subscriptions());
        view.assignments().forEach((member, assignment) -> out.printf("member=%s assignment=%s%n", member, assignment));
        view.frontiers().forEach((partition, frontier) -> out.printf("partition=%s next=%d ahead=%d retrying=%d%n",
                partition, frontier.next(), frontier.ahead(), frontier.retrying()));
    }

    private static void printPartition(PrintStream out, TideClient client, TopicPartition partition, int limit)
            throws IOException {
        var view = client.partition(partition, limit);
        out.printf("partition=%s retained_from=%d next_offset=%d total_segments=%d shown=%d%n",
                partition, view.range().retainedFrom(), view.range().nextOffset(), view.segmentCount(),
                view.recent().size());
        view.recent().forEach(segment -> out.printf(
                "base=%d end=%d bytes=%d created_ms=%d latest_ms=%d active=%s%n",
                segment.baseOffset(), segment.endOffset(), segment.sizeBytes(), segment.createdAtMs(),
                segment.latestAppendMs(), segment.active()));
    }

    private static void checkOutput(PrintStream output) throws IOException {
        output.flush();
        if (output.checkError()) {
            throw new IOException("could not write to stdout");
        }
    }

    private static void printRetention(PrintStream out, TideClient client, TopicPartition partition)
            throws IOException {
        var view = client.retention(partition);
        out.printf("partition=%s evaluated_ms=%d min_age_ms=%d allowed_end=%d limiting_groups=%d group_sample=%s%n",
                view.partition(), view.evaluatedAtMs(), view.minimumAgeMs(), view.allowedEndExclusive(),
                view.limitingGroupCount(), view.limitingGroups());
        out.printf("eligible_segments=%d eligible_bytes=%d resulting_low=%d%n",
                view.eligibleSegments(), view.eligibleBytes(), view.resultingLow());
        out.printf("kept_base=%d kept_end=%d active=%s above_frontier=%s remaining_age_ms=%d%n",
                view.firstKeptBase(), view.firstKeptEnd(), view.keepsActive(),
                view.aboveGroupFrontier(), view.remainingAgeMs());
    }

    private static Set<String> subscriptions(String values) {
        var topics = new TreeSet<String>();
        for (String name : values.split(",", -1)) {
            if (!topics.add(Settings.requireIdentifier(name, "topic"))) {
                throw new IllegalArgumentException("duplicate topic " + name);
            }
        }
        return topics;
    }

    private static void arity(String command, String[] args, int minimum, int maximum) {
        if (args.length < minimum || args.length > maximum) {
            throw new IllegalArgumentException("wrong number of arguments for " + command + "; use --help");
        }
    }

    private static long nonnegative(String value, String label) {
        long result = Long.parseLong(value);
        if (result < 0) {
            throw new IllegalArgumentException(label + " must be nonnegative");
        }
        return result;
    }

    private static int bounded(String value, int minimum, int maximum, String label) {
        int number = Integer.parseInt(value);
        if (number < minimum || number > maximum) {
            throw new IllegalArgumentException(label + " must be within " + minimum + ".." + maximum);
        }
        return number;
    }

    private static byte[] utf8(String value) {
        return value.getBytes(StandardCharsets.UTF_8);
    }
}
