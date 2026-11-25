package tidemq;

import tidemq.segment.Segment;
import tidemq.segment.PartitionInventory;
import tidemq.segment.TopicPartition;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.concurrent.atomic.AtomicLong;

public final class SegmentMain {
    private SegmentMain() {}

    public static void main(String[] args) {
        try {
            if (args.length == 0 || args[0].equals("--help")) {
                System.out.print("""
                        java -cp build/tidemq.jar tidemq.SegmentMain inspect TOPIC PARTITION SEGMENT
                        java -cp build/tidemq.jar tidemq.SegmentMain verify TOPIC PARTITION PARTITION_DIRECTORY
                        java -cp build/tidemq.jar tidemq.SegmentMain recover-copy TOPIC PARTITION SEGMENT NEW_DIRECTORY
                        """);
                return;
            }
            run(args);
        } catch (IOException problem) {
            System.err.printf("I/O error: %s%n", problem.getMessage());
            System.exit(1);
        } catch (IllegalArgumentException problem) {
            System.err.printf("Usage error: %s%n", problem.getMessage());
            System.exit(2);
        }
    }

    private static void run(String[] args) throws IOException {
        if (args.length < 4) {
            throw new IllegalArgumentException("command requires topic, partition and input path");
        }
        var partition = new TopicPartition(args[1], Integer.parseInt(args[2]));
        Path path = Path.of(args[3]);
        switch (args[0]) {
            case "inspect" -> {
                requireArguments(args, 4);
                show(Segment.open(path, partition, false), partition, path);
            }
            case "verify" -> {
                requireArguments(args, 4);
                var view = PartitionInventory.verify(path, partition);
                for (var entry : view.segments()) {
                    var segment = entry.segment();
                    System.out.printf("file=%s base=%d end=%d records=%d bytes=%d active=%s%n",
                            entry.path().getFileName(), segment.baseOffset(), segment.endOffset(),
                            segment.endOffset() - segment.baseOffset(), segment.sizeBytes(), segment.active());
                }
                System.out.printf("partition=%s low=%d end=%d records=%d bytes=%d segments=%d%n",
                        partition, view.low(), view.end(), view.records(), view.bytes(), view.segments().size());
            }
            case "recover-copy" -> {
                requireArguments(args, 5);
                var directory = Files.createDirectory(Path.of(args[4]));
                Path destination = Files.copy(path, directory.resolve(path.getFileName()));
                long before = Files.size(destination);
                Segment recovered = Segment.open(destination, partition, true);
                System.out.printf("repair_bytes=%d copy=%s%n", before - Files.size(destination), destination);
                show(recovered, partition, destination);
            }
            default -> throw new IllegalArgumentException("expected inspect, verify or recover-copy; use --help");
        }
    }

    private static void requireArguments(String[] args, int expected) {
        if (args.length != expected) {
            throw new IllegalArgumentException("wrong number of arguments for " + args[0]);
        }
    }

    private static void show(Segment segment, TopicPartition partition, Path path) throws IOException {
        var count = new AtomicLong();
        segment.forEach(message -> {
            ClientMain.printMessage(System.out, message);
            count.incrementAndGet();
        });
        System.out.printf("segment_v1=%s partition=%s records=%d bytes=%d%n",
                path.getFileName(), partition, count.get(), Files.size(path));
    }
}
