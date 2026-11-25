package tidemq;

import tidemq.wire.TcpBroker;

import java.io.IOException;
import java.nio.file.Path;
import java.time.Clock;

public final class ServerMain {
    private ServerMain() {}

    public static void main(String[] args) throws Exception {
        if (args.length > 1 || (args.length == 1 && args[0].equals("--help"))) {
            System.out.println("Usage: java -jar build/tidemq.jar [config/broker.properties]");
            if (args.length > 1) {
                System.exit(2);
            }
            return;
        }
        Path configuration = Path.of(args.length == 1 ? args[0] : "config/broker.properties");
        Settings settings = Settings.load(configuration);
        try (var server = TcpBroker.start(settings, Clock.systemUTC(), Boundary.NONE)) {
            Thread stop = Thread.ofPlatform().name("tidemq-shutdown").unstarted(() -> {
                try {
                    server.close();
                } catch (IOException error) {
                    System.err.println("TideMQ shutdown: " + error.getMessage());
                }
            });
            Runtime.getRuntime().addShutdownHook(stop);
            try {
                System.out.printf("TIDEMQ_READY host=%s port=%d protocol=1 directory=%s%n",
                        settings.address().getHostAddress(), server.port(), settings.dataDirectory());
                server.await();
            } finally {
                try {
                    Runtime.getRuntime().removeShutdownHook(stop);
                } catch (IllegalStateException stopping) {
                    // The process is already running shutdown hooks.
                }
            }
        }
    }
}
