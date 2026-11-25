package tidemq;

import tidemq.segment.Segment;
import tidemq.segment.TopicPartition;
import tidemq.wire.TcpBroker;

import java.io.DataInputStream;
import java.io.IOException;
import java.net.InetAddress;
import java.net.Socket;
import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import static tidemq.TestSupport.*;

public final class Fixtures {
    private static final Path DISTRIBUTED = Path.of("samples/v1");
    private static final String NAME = Segment.fileName(0);
    private static final TopicPartition PARTITION = new TopicPartition("orders", 0);

    private Fixtures() {}

    public static void main(String[] args) throws Exception {
        if (args.length != 1) {
            throw new IllegalArgumentException("pass a fresh sample destination");
        }
        write(Path.of(args[0]));
    }

    static void write(Path output) throws Exception {
        Files.createDirectory(output);
        Files.createDirectory(output.resolve("normal"));
        Files.createDirectory(output.resolve("interrupted"));
        try (var tmp = new Scratch();
             var server = TcpBroker.start(settings(tmp.root), new Time(), Boundary.NONE);
             var producer = connect(server)) {
            BrokerTest.publish(producer, "fixture", 40, bytes("received"));
            BrokerTest.publish(producer, "fixture", 41, bytes("packed"));
            Path stored = tmp.root.resolve("segments/orders/0").resolve(NAME);
            Path normal = output.resolve("normal").resolve(NAME);
            Files.copy(stored, normal);
            byte[] full = Files.readAllBytes(normal);
            Files.write(output.resolve("interrupted").resolve(NAME), Arrays.copyOf(full, full.length - 3));
            byte[] hello = ByteBuffer.allocate(25)
                    .putInt(21).putLong(1).put((byte) 1).putInt(1).putInt(1).putInt(8192).array();
            Files.write(output.resolve("hello.request.bin"), hello);
            Files.write(output.resolve("hello.response.bin"), exchangeHello(server, hello));
        }
        System.out.printf("generated %s%n", output);
    }

    private static byte[] exchangeHello(TcpBroker server, byte[] request) throws IOException {
        try (var socket = new Socket(InetAddress.getLoopbackAddress(), server.port())) {
            socket.setSoTimeout(5000);
            socket.getOutputStream().write(request);
            socket.getOutputStream().flush();
            var response = new DataInputStream(socket.getInputStream());
            int size = response.readInt();
            eq(21, size);
            return ByteBuffer.allocate(size + 4).putInt(size).put(Binary.readExact(response, size)).array();
        }
    }

    static void check() throws Exception {
        try (var tmp = new Scratch()) {
            Path actual = tmp.root.resolve("regenerated");
            write(actual);
            for (String fixture : List.of("normal/" + NAME, "interrupted/" + NAME,
                    "hello.request.bin", "hello.response.bin")) {
                eq(-1L, Files.mismatch(DISTRIBUTED.resolve(fixture), actual.resolve(fixture)));
            }
            Path normal = DISTRIBUTED.resolve("normal").resolve(NAME);
            Path interrupted = DISTRIBUTED.resolve("interrupted").resolve(NAME);
            var offsets = new ArrayList<Long>();
            Segment.open(normal, PARTITION, false).forEach(message -> offsets.add(message.offset()));
            eq(List.of(0L, 1L), offsets);
            io(() -> Segment.open(interrupted, PARTITION, false));

            Path brokerState = tmp.root.resolve("persistent");
            Path initial = brokerState.resolve("segments/orders/0").resolve(NAME);
            Files.createDirectories(initial.getParent());
            Files.copy(interrupted, initial);
            try (var server = TcpBroker.start(settings(brokerState), new Time(), Boundary.NONE);
                 var client = connect(server)) {
                eq(1L, client.partition(PARTITION, 1).range().nextOffset());
                TestSupport.check(BrokerTest.publish(client, "fixture", 40, bytes("received")).replayed(),
                        "missing first receipt");
                eq(1L, BrokerTest.publish(client, "fixture", 41, bytes("packed")).offset());
                eq(-1L, Files.mismatch(normal, initial));
            }

            Path corruptDir = Files.createDirectory(tmp.root.resolve("corrupt"));
            Path corrupt = corruptDir.resolve(NAME);
            byte[] content = Files.readAllBytes(normal);
            content[content.length - 1] ^= 1;
            Files.write(corrupt, content);
            io(() -> Segment.open(corrupt, PARTITION, true));
            eq(content.length, (int) Files.size(corrupt));
        }
    }
}
