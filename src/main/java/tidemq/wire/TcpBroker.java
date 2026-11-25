package tidemq.wire;

import tidemq.Boundary;
import tidemq.Broker;
import tidemq.Settings;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.time.Clock;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

public final class TcpBroker implements AutoCloseable {
    private static final System.Logger LOG = System.getLogger(TcpBroker.class.getName());

    private final Settings settings;
    private final Boundary boundary;
    private final Broker broker;
    private final ServerSocket listener;
    private final Thread acceptor;
    private final Semaphore slots;
    private final AtomicBoolean closing = new AtomicBoolean();
    private final CountDownLatch acceptorDone = new CountDownLatch(1);
    private final CountDownLatch closed = new CountDownLatch(1);
    private final Set<Socket> sockets = ConcurrentHashMap.newKeySet();
    private final ExecutorService sessions = Executors.newVirtualThreadPerTaskExecutor();
    private final ScheduledExecutorService maintenance = Executors.newSingleThreadScheduledExecutor(
            Thread.ofPlatform().daemon().name("tidemq-maintenance").factory());
    private volatile IOException acceptFailure;
    private long nextSession;

    public static TcpBroker start(Settings settings, Clock clock, Boundary boundary) throws IOException {
        Broker broker = Broker.open(settings, clock, boundary);
        ServerSocket listener = null;
        try {
            listener = new ServerSocket();
            listener.setReuseAddress(true);
            listener.bind(new InetSocketAddress(settings.address(), settings.port()), settings.maxConnections());
            return new TcpBroker(settings, boundary, broker, listener);
        } catch (IOException | RuntimeException error) {
            closeOnError(listener, error);
            closeOnError(broker, error);
            throw error;
        }
    }

    private TcpBroker(Settings settings, Boundary boundary, Broker broker, ServerSocket listener) {
        this.settings = settings;
        this.boundary = boundary;
        this.broker = broker;
        this.listener = listener;
        slots = new Semaphore(settings.maxConnections());
        long tick = Math.max(1, Math.min(settings.retentionIntervalMs(),
                Math.min(settings.leaseMs(), settings.reservationMs())));
        maintenance.scheduleWithFixedDelay(this::maintain, tick, tick, TimeUnit.MILLISECONDS);
        acceptor = Thread.ofPlatform().name("tidemq-acceptor").start(this::accept);
    }

    public int port() {
        return listener.getLocalPort();
    }

    public void await() throws IOException, InterruptedException {
        acceptorDone.await();
        if (acceptFailure != null) {
            throw acceptFailure;
        }
    }

    private void accept() {
        try {
            while (!closing.get()) {
                Socket socket = listener.accept();
                if (closing.get() || !slots.tryAcquire()) {
                    socket.close();
                    continue;
                }
                if (nextSession == Long.MAX_VALUE) {
                    socket.close();
                    slots.release();
                    throw new IOException("TCP session id space exhausted");
                }
                long session = ++nextSession;
                sockets.add(socket);
                sessions.execute(() -> {
                    try {
                        new Session(session, socket, broker, settings, boundary).run();
                    } finally {
                        sockets.remove(socket);
                        slots.release();
                    }
                });
            }
        } catch (IOException error) {
            if (!closing.get()) {
                acceptFailure = error;
            }
        } finally {
            acceptorDone.countDown();
        }
    }

    private void maintain() {
        try {
            broker.maintain();
        } catch (IOException error) {
            LOG.log(System.Logger.Level.WARNING, "maintenance halted: " + error.getMessage());
            maintenance.shutdown();
        }
    }

    @Override
    public void close() throws IOException {
        if (!closing.compareAndSet(false, true)) {
            awaitUninterruptibly(closed);
            return;
        }
        IOException first = null;
        try {
            try {
                listener.close();
            } catch (IOException error) {
                first = error;
            }
            awaitUninterruptibly(acceptorDone);
            for (Socket socket : sockets) {
                try {
                    socket.close();
                } catch (IOException error) {
                    if (first == null) {
                        first = error;
                    } else {
                        first.addSuppressed(error);
                    }
                }
            }
            maintenance.close();
            sessions.close();
            try {
                broker.close();
            } catch (IOException error) {
                if (first == null) {
                    first = error;
                } else {
                    first.addSuppressed(error);
                }
            }
        } finally {
            closed.countDown();
        }
        if (first != null) {
            throw first;
        }
    }

    private static void awaitUninterruptibly(CountDownLatch latch) {
        boolean interrupted = false;
        for (;;) {
            try {
                latch.await();
                break;
            } catch (InterruptedException ignored) {
                interrupted = true;
            }
        }
        if (interrupted) {
            Thread.currentThread().interrupt();
        }
    }

    private static void closeOnError(AutoCloseable resource, Throwable error) {
        if (resource != null) {
            try {
                resource.close();
            } catch (Exception close) {
                error.addSuppressed(close);
            }
        }
    }
}
