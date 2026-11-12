package com.minikafka.network;

import com.minikafka.broker.Broker;
import com.minikafka.broker.MiniKafkaBroker;
import com.minikafka.exception.InvalidMessageException;
import com.minikafka.exception.OffsetOutOfRangeException;
import com.minikafka.exception.PartitionNotFoundException;
import com.minikafka.exception.ProtocolException;
import com.minikafka.exception.TopicAlreadyExistsException;
import com.minikafka.exception.TopicNotFoundException;
import com.minikafka.model.Message;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.EOFException;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.SocketException;
import java.net.SocketTimeoutException;
import java.nio.file.Path;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.LongAdder;

/**
 * Robust, binary-framed TCP server for MiniKafka using Java 21 Virtual Threads.
 *
 * <p>Protections &amp; Wire Guarantees:
 * <ul>
 *   <li>Protocol versioning ({@link NetworkProtocol#PROTOCOL_VERSION}) and per-request correlation IDs.</li>
 *   <li>Bounded frame allocation enforced via {@code maxFrameSize} before reading payload bytes.</li>
 *   <li>Typed {@link ResponseStatus} error responses for invalid frames, unsupported versions, and domain errors.</li>
 *   <li>Configurable socket read timeout ({@code SO_TIMEOUT}) and write-stall watchdog timeout.</li>
 *   <li>Clean client disconnect handling without thread or socket leaks.</li>
 * </ul>
 */
public class BrokerServer implements AutoCloseable {

    public static final int DEFAULT_PORT = 9092;
    public static final int DEFAULT_READ_TIMEOUT_MS = 30_000;
    public static final int DEFAULT_WRITE_TIMEOUT_MS = 10_000;

    private final Broker broker;
    private final ServerSocket serverSocket;
    private final int maxFrameSize;
    private final int readTimeoutMs;
    private final int writeTimeoutMs;

    private final ExecutorService connectionExecutor;
    private final ScheduledExecutorService writeWatchdog;

    private final AtomicInteger activeConnections = new AtomicInteger(0);
    private final LongAdder malformedFrameCount = new LongAdder();
    private final LongAdder timedOutConnections = new LongAdder();
    private final LongAdder cleanDisconnectCount = new LongAdder();

    private volatile boolean running = false;
    private Thread acceptThread;

    public BrokerServer(Broker broker, int port) throws IOException {
        this(broker, port, NetworkProtocol.DEFAULT_MAX_FRAME_SIZE, DEFAULT_READ_TIMEOUT_MS, DEFAULT_WRITE_TIMEOUT_MS);
    }

    public BrokerServer(Broker broker, int port, int maxFrameSize, int readTimeoutMs, int writeTimeoutMs)
            throws IOException {
        this.broker = Objects.requireNonNull(broker, "Broker cannot be null");
        if (maxFrameSize < NetworkProtocol.HEADER_SIZE) {
            throw new IllegalArgumentException("maxFrameSize must be >= " + NetworkProtocol.HEADER_SIZE);
        }
        if (readTimeoutMs < 0 || writeTimeoutMs < 0) {
            throw new IllegalArgumentException("Socket timeouts must be non-negative");
        }
        this.maxFrameSize = maxFrameSize;
        this.readTimeoutMs = readTimeoutMs;
        this.writeTimeoutMs = writeTimeoutMs;

        this.serverSocket = new ServerSocket();
        this.serverSocket.setReuseAddress(true);
        this.serverSocket.bind(new InetSocketAddress(port));
        this.connectionExecutor = Executors.newVirtualThreadPerTaskExecutor();
        this.writeWatchdog = Executors.newSingleThreadScheduledExecutor(r -> {
            Thread t = new Thread(r, "broker-write-watchdog");
            t.setDaemon(true);
            return t;
        });
    }

    public synchronized void start() {
        if (running) return;
        running = true;
        acceptThread = Thread.ofVirtual().name("broker-acceptor").start(this::acceptLoop);
    }

    public int getPort() {
        return serverSocket.getLocalPort();
    }

    public int getMaxFrameSize() {
        return maxFrameSize;
    }

    public int getReadTimeoutMs() {
        return readTimeoutMs;
    }

    public int getWriteTimeoutMs() {
        return writeTimeoutMs;
    }

    public int getActiveConnections() {
        return activeConnections.get();
    }

    public long getMalformedFrameCount() {
        return malformedFrameCount.sum();
    }

    public long getTimedOutConnections() {
        return timedOutConnections.sum();
    }

    public long getCleanDisconnectCount() {
        return cleanDisconnectCount.sum();
    }

    private void acceptLoop() {
        while (running && !serverSocket.isClosed()) {
            try {
                Socket clientSocket = serverSocket.accept();
                clientSocket.setTcpNoDelay(true);
                if (readTimeoutMs > 0) {
                    clientSocket.setSoTimeout(readTimeoutMs);
                }
                connectionExecutor.submit(() -> handleClient(clientSocket));
            } catch (SocketException e) {
                if (!running || serverSocket.isClosed()) {
                    break;
                }
            } catch (IOException e) {
                if (running) {
                    System.err.println("MiniKafka server accept error: " + e.getMessage());
                }
            }
        }
    }

    private void handleClient(Socket socket) {
        activeConnections.incrementAndGet();
        try (socket;
             InputStream in = socket.getInputStream();
             OutputStream out = socket.getOutputStream()) {
            handleSession(in, out, socket);
        } catch (IOException ignored) {
            // Best-effort close
        } finally {
            activeConnections.decrementAndGet();
        }
    }

    /**
     * Processes framed protocol requests for a single client stream until disconnect,
     * timeout, or unrecoverable frame desynchronization.
     */
    void handleSession(InputStream rawIn, OutputStream rawOut, Socket socketOrNull) {
        DataInputStream in = new DataInputStream(new BufferedInputStream(rawIn));
        DataOutputStream out = new DataOutputStream(new BufferedOutputStream(rawOut));

        try {
            while (socketOrNull == null || (!socketOrNull.isClosed() && running)) {
                NetworkProtocol.Frame frame;
                try {
                    frame = NetworkProtocol.readRawFrame(in, maxFrameSize);
                } catch (EOFException e) {
                    cleanDisconnectCount.increment();
                    break;
                } catch (SocketTimeoutException e) {
                    timedOutConnections.increment();
                    break;
                } catch (ProtocolException e) {
                    malformedFrameCount.increment();
                    writeWithTimeout(socketOrNull, () ->
                            NetworkProtocol.writeErrorResponse(out, -1, e.getStatus(), e.getMessage()));
                    // Stream alignment cannot be trusted after an oversized or sub-header frame length.
                    break;
                }

                if (frame.version() != NetworkProtocol.PROTOCOL_VERSION) {
                    malformedFrameCount.increment();
                    String err = "Unsupported protocol version: " + frame.version()
                            + " (expected " + NetworkProtocol.PROTOCOL_VERSION + ")";
                    writeWithTimeout(socketOrNull, () ->
                            NetworkProtocol.writeErrorResponse(out, frame.requestId(),
                                    ResponseStatus.UNSUPPORTED_VERSION, err));
                    continue;
                }

                RequestType requestType;
                try {
                    requestType = RequestType.fromCode(frame.requestTypeCode());
                } catch (IllegalArgumentException e) {
                    malformedFrameCount.increment();
                    writeWithTimeout(socketOrNull, () ->
                            NetworkProtocol.writeErrorResponse(out, frame.requestId(),
                                    ResponseStatus.UNKNOWN_REQUEST_TYPE, e.getMessage()));
                    continue;
                }

                try {
                    dispatchFrame(frame, requestType, out, socketOrNull);
                } catch (ProtocolException e) {
                    malformedFrameCount.increment();
                    writeWithTimeout(socketOrNull, () ->
                            NetworkProtocol.writeErrorResponse(out, frame.requestId(), e.getStatus(), e.getMessage()));
                }
            }
        } catch (EOFException e) {
            cleanDisconnectCount.increment();
        } catch (SocketTimeoutException e) {
            timedOutConnections.increment();
        } catch (SocketException ignored) {
            cleanDisconnectCount.increment();
        } catch (IOException e) {
            if (running) {
                System.err.println("MiniKafka client session I/O error: " + e.getMessage());
            }
        }
    }

    private void dispatchFrame(NetworkProtocol.Frame frame, RequestType requestType,
                               DataOutputStream out, Socket socketOrNull) throws IOException {
        switch (requestType) {
            case PRODUCE -> handleProduce(frame, out, socketOrNull);
            case FETCH -> handleFetch(frame, out, socketOrNull);
            case CREATE_TOPIC -> handleCreateTopic(frame, out, socketOrNull);
            case LIST_TOPICS -> handleListTopics(frame, out, socketOrNull);
            case PING -> writeWithTimeout(socketOrNull, () -> NetworkProtocol.writePingResponse(out, frame.requestId()));
            case ERROR -> {
                malformedFrameCount.increment();
                writeWithTimeout(socketOrNull, () -> NetworkProtocol.writeErrorResponse(
                        out, frame.requestId(), ResponseStatus.BAD_REQUEST, "ERROR opcode is server-to-client only"));
            }
        }
    }

    private void handleProduce(NetworkProtocol.Frame frame, DataOutputStream out, Socket socketOrNull)
            throws IOException {
        NetworkProtocol.ProduceRequest req = NetworkProtocol.decodeProduceRequest(frame);
        try {
            Message msg = Message.create(req.topic(), req.key(), req.value());
            long offset = broker.append(req.topic(), req.partition(), msg);
            writeWithTimeout(socketOrNull, () ->
                    NetworkProtocol.writeProduceResponse(out, req.requestId(), ResponseStatus.OK, offset, null));
        } catch (Exception e) {
            ResponseStatus status = mapExceptionToStatus(e);
            writeWithTimeout(socketOrNull, () ->
                    NetworkProtocol.writeProduceResponse(out, req.requestId(), status, -1L, e.getMessage()));
        }
    }

    private void handleFetch(NetworkProtocol.Frame frame, DataOutputStream out, Socket socketOrNull)
            throws IOException {
        NetworkProtocol.FetchRequest req = NetworkProtocol.decodeFetchRequest(frame);
        try {
            List<Message> messages = broker.read(req.topic(), req.partition(), req.fromOffset(), req.maxMessages());
            writeWithTimeout(socketOrNull, () ->
                    NetworkProtocol.writeFetchResponse(
                            out, req.requestId(), ResponseStatus.OK, messages, null, maxFrameSize));
        } catch (Exception e) {
            ResponseStatus status = (e instanceof ProtocolException pe) ? pe.getStatus() : mapExceptionToStatus(e);
            writeWithTimeout(socketOrNull, () ->
                    NetworkProtocol.writeFetchResponse(
                            out, req.requestId(), status, List.of(), e.getMessage(), maxFrameSize));
        }
    }

    private void handleCreateTopic(NetworkProtocol.Frame frame, DataOutputStream out, Socket socketOrNull)
            throws IOException {
        NetworkProtocol.CreateTopicRequest req = NetworkProtocol.decodeCreateTopicRequest(frame);
        try {
            broker.createTopic(req.topic(), req.partitionCount());
            writeWithTimeout(socketOrNull, () ->
                    NetworkProtocol.writeCreateTopicResponse(out, req.requestId(), ResponseStatus.OK, null));
        } catch (Exception e) {
            ResponseStatus status = mapExceptionToStatus(e);
            writeWithTimeout(socketOrNull, () ->
                    NetworkProtocol.writeCreateTopicResponse(out, req.requestId(), status, e.getMessage()));
        }
    }

    private void handleListTopics(NetworkProtocol.Frame frame, DataOutputStream out, Socket socketOrNull)
            throws IOException {
        try {
            Set<String> topics = broker.listTopics();
            writeWithTimeout(socketOrNull, () ->
                    NetworkProtocol.writeListTopicsResponse(out, frame.requestId(), ResponseStatus.OK, topics, null));
        } catch (Exception e) {
            ResponseStatus status = mapExceptionToStatus(e);
            writeWithTimeout(socketOrNull, () ->
                    NetworkProtocol.writeListTopicsResponse(
                            out, frame.requestId(), status, Set.of(), e.getMessage()));
        }
    }

    @FunctionalInterface
    private interface IoAction {
        void run() throws IOException;
    }

    private void writeWithTimeout(Socket socketOrNull, IoAction action) throws IOException {
        if (socketOrNull == null || writeTimeoutMs <= 0) {
            action.run();
            return;
        }
        ScheduledFuture<?> watchdogTask = writeWatchdog.schedule(() -> {
            timedOutConnections.increment();
            try {
                socketOrNull.close();
            } catch (IOException ignored) {}
        }, writeTimeoutMs, TimeUnit.MILLISECONDS);
        try {
            action.run();
        } finally {
            watchdogTask.cancel(false);
        }
    }

    static ResponseStatus mapExceptionToStatus(Exception e) {
        if (e instanceof TopicNotFoundException) return ResponseStatus.TOPIC_NOT_FOUND;
        if (e instanceof TopicAlreadyExistsException) return ResponseStatus.TOPIC_ALREADY_EXISTS;
        if (e instanceof PartitionNotFoundException) return ResponseStatus.PARTITION_NOT_FOUND;
        if (e instanceof OffsetOutOfRangeException) return ResponseStatus.OFFSET_OUT_OF_RANGE;
        if (e instanceof InvalidMessageException || e instanceof IllegalArgumentException) {
            return ResponseStatus.BAD_REQUEST;
        }
        if (e instanceof ProtocolException pe) return pe.getStatus();
        return ResponseStatus.INTERNAL_ERROR;
    }

    @Override
    public synchronized void close() {
        if (!running && (serverSocket == null || serverSocket.isClosed())) {
            return;
        }
        running = false;
        try {
            if (serverSocket != null && !serverSocket.isClosed()) {
                serverSocket.close();
            }
        } catch (IOException ignored) {}

        connectionExecutor.shutdownNow();
        writeWatchdog.shutdownNow();
        try {
            connectionExecutor.awaitTermination(3, TimeUnit.SECONDS);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }

    public static void main(String[] args) throws Exception {
        int port = DEFAULT_PORT;
        Path dataDir = Path.of("data");
        if (args.length > 0) {
            port = Integer.parseInt(args[0]);
        }
        if (args.length > 1) {
            dataDir = Path.of(args[1]);
        }

        Broker broker = new MiniKafkaBroker(dataDir);
        BrokerServer server = new BrokerServer(broker, port);
        server.start();

        System.out.println("=================================================");
        System.out.println("  MiniKafka Broker Server started on port " + port);
        System.out.println("  Protocol v" + NetworkProtocol.PROTOCOL_VERSION + " | Max Frame: "
                + server.getMaxFrameSize() + " B | SO_TIMEOUT: " + server.getReadTimeoutMs() + " ms");
        System.out.println("  Storage directory: " + dataDir.toAbsolutePath());
        System.out.println("  Java 21 Virtual Threads network listener active");
        System.out.println("=================================================");

        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            System.out.println("Shutting down MiniKafka Broker Server...");
            server.close();
        }));

        Thread.currentThread().join();
    }
}
