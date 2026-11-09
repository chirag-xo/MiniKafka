package com.minikafka.network;

import com.minikafka.broker.Broker;
import com.minikafka.broker.MiniKafkaBroker;
import com.minikafka.exception.MiniKafkaException;
import com.minikafka.model.Message;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.EOFException;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.SocketException;
import java.nio.file.Path;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

/**
 * High-performance, binary-framed TCP server for MiniKafka.
 *
 * <p>Concurrency Architecture:
 * <ul>
 *   <li>Uses Java 21 Virtual Threads ({@link Executors#newVirtualThreadPerTaskExecutor()})</li>
 *   <li>One virtual thread per active client connection, scaling cheaply to thousands of concurrent clients.</li>
 * </ul>
 */
public class BrokerServer implements AutoCloseable {

    public static final int DEFAULT_PORT = 9092;

    private final Broker broker;
    private final ServerSocket serverSocket;
    private final ExecutorService connectionExecutor;
    private volatile boolean running = false;
    private Thread acceptThread;

    public BrokerServer(Broker broker, int port) throws IOException {
        this.broker = Objects.requireNonNull(broker, "Broker cannot be null");
        this.serverSocket = new ServerSocket();
        this.serverSocket.setReuseAddress(true);
        this.serverSocket.bind(new InetSocketAddress(port));
        this.connectionExecutor = Executors.newVirtualThreadPerTaskExecutor();
    }

    public synchronized void start() {
        if (running) return;
        running = true;
        acceptThread = Thread.ofVirtual().name("broker-acceptor").start(this::acceptLoop);
    }

    public int getPort() {
        return serverSocket.getLocalPort();
    }

    private void acceptLoop() {
        while (running && !serverSocket.isClosed()) {
            try {
                Socket clientSocket = serverSocket.accept();
                clientSocket.setTcpNoDelay(true);
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
        try (socket;
             DataInputStream in = new DataInputStream(new BufferedInputStream(socket.getInputStream()));
             DataOutputStream out = new DataOutputStream(new BufferedOutputStream(socket.getOutputStream()))) {

            while (running && !socket.isClosed()) {
                int frameLen;
                try {
                    frameLen = in.readInt();
                } catch (EOFException e) {
                    break; // client disconnected normally
                }

                if (frameLen < 5) {
                    throw new MiniKafkaException("Invalid frame length received: " + frameLen);
                }

                byte reqCode = in.readByte();
                int correlationId = in.readInt();
                RequestType requestType = RequestType.fromCode(reqCode);

                switch (requestType) {
                    case PRODUCE -> handleProduce(in, out, correlationId);
                    case FETCH -> handleFetch(in, out, correlationId);
                    case CREATE_TOPIC -> handleCreateTopic(in, out, correlationId);
                    case LIST_TOPICS -> handleListTopics(out, correlationId);
                    case PING -> {
                        out.writeInt(5);
                        out.writeByte(RequestType.PING.getCode());
                        out.writeInt(correlationId);
                        out.flush();
                    }
                }
            }
        } catch (EOFException | SocketException ignored) {
            // Client closed connection
        } catch (Exception e) {
            System.err.println("MiniKafka client session error: " + e.getMessage());
        }
    }

    private void handleProduce(DataInputStream in, DataOutputStream out, int correlationId) throws IOException {
        NetworkProtocol.ProduceRequest req = NetworkProtocol.readProduceRequest(in, correlationId);
        try {
            Message msg = Message.create(req.topic(), req.key(), req.value());
            long offset = broker.append(req.topic(), req.partition(), msg);
            NetworkProtocol.writeProduceResponse(out, correlationId, NetworkProtocol.STATUS_OK, offset, null);
        } catch (Exception e) {
            NetworkProtocol.writeProduceResponse(out, correlationId, NetworkProtocol.STATUS_ERROR, -1L, e.getMessage());
        }
    }

    private void handleFetch(DataInputStream in, DataOutputStream out, int correlationId) throws IOException {
        NetworkProtocol.FetchRequest req = NetworkProtocol.readFetchRequest(in, correlationId);
        try {
            List<Message> messages = broker.read(req.topic(), req.partition(), req.fromOffset(), req.maxMessages());
            NetworkProtocol.writeFetchResponse(out, correlationId, NetworkProtocol.STATUS_OK, messages, null);
        } catch (Exception e) {
            NetworkProtocol.writeFetchResponse(out, correlationId, NetworkProtocol.STATUS_ERROR, List.of(), e.getMessage());
        }
    }

    private void handleCreateTopic(DataInputStream in, DataOutputStream out, int correlationId) throws IOException {
        NetworkProtocol.CreateTopicRequest req = NetworkProtocol.readCreateTopicRequest(in, correlationId);
        try {
            broker.createTopic(req.topic(), req.partitionCount());
            NetworkProtocol.writeCreateTopicResponse(out, correlationId, NetworkProtocol.STATUS_OK, null);
        } catch (Exception e) {
            NetworkProtocol.writeCreateTopicResponse(out, correlationId, NetworkProtocol.STATUS_ERROR, e.getMessage());
        }
    }

    private void handleListTopics(DataOutputStream out, int correlationId) throws IOException {
        try {
            Set<String> topics = broker.listTopics();
            NetworkProtocol.writeListTopicsResponse(out, correlationId, NetworkProtocol.STATUS_OK, topics, null);
        } catch (Exception e) {
            NetworkProtocol.writeListTopicsResponse(out, correlationId, NetworkProtocol.STATUS_ERROR, Set.of(), e.getMessage());
        }
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

