package com.minikafka.network;

import com.minikafka.exception.MiniKafkaException;
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
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Thread-safe client communicating with a remote {@link BrokerServer} over TCP
 * using MiniKafka's versioned, bounded binary framed protocol.
 */
public class BrokerClient implements AutoCloseable {

    public static final int DEFAULT_CONNECT_TIMEOUT_MS = 5_000;
    public static final int DEFAULT_READ_TIMEOUT_MS = 10_000;

    private final Socket socket;
    private final DataInputStream in;
    private final DataOutputStream out;
    private final int maxFrameSize;
    private final AtomicInteger requestIdSeq = new AtomicInteger(1);
    private final Object ioLock = new Object();
    private volatile boolean closed = false;

    public BrokerClient(String host, int port) throws IOException {
        this(host, port, DEFAULT_CONNECT_TIMEOUT_MS, DEFAULT_READ_TIMEOUT_MS, NetworkProtocol.DEFAULT_MAX_FRAME_SIZE);
    }

    public BrokerClient(String host, int port, int connectTimeoutMs, int readTimeoutMs, int maxFrameSize)
            throws IOException {
        Objects.requireNonNull(host, "Host cannot be null");
        if (maxFrameSize < NetworkProtocol.HEADER_SIZE) {
            throw new IllegalArgumentException("maxFrameSize must be >= " + NetworkProtocol.HEADER_SIZE);
        }
        this.maxFrameSize = maxFrameSize;
        this.socket = new Socket();
        this.socket.setTcpNoDelay(true);
        if (readTimeoutMs > 0) {
            this.socket.setSoTimeout(readTimeoutMs);
        }
        this.socket.connect(new InetSocketAddress(host, port), connectTimeoutMs);
        this.in = new DataInputStream(new BufferedInputStream(socket.getInputStream()));
        this.out = new DataOutputStream(new BufferedOutputStream(socket.getOutputStream()));
    }

    /**
     * Stream-based constructor for deterministic in-process protocol testing.
     */
    BrokerClient(InputStream rawIn, OutputStream rawOut, int maxFrameSize) {
        this.socket = null;
        this.maxFrameSize = maxFrameSize;
        this.in = new DataInputStream(new BufferedInputStream(rawIn));
        this.out = new DataOutputStream(new BufferedOutputStream(rawOut));
    }

    public long produce(String topic, int partition, String key, byte[] value) {
        ensureOpen();
        int reqId = requestIdSeq.incrementAndGet();
        synchronized (ioLock) {
            try {
                NetworkProtocol.writeProduceRequest(out, reqId, topic, partition, key, value, maxFrameSize);
                NetworkProtocol.Frame frame = readAndCheckResponseFrame(reqId, RequestType.PRODUCE);
                NetworkProtocol.ProduceResponse resp = NetworkProtocol.decodeProduceResponse(frame);
                if (resp.status() != ResponseStatus.OK) {
                    throw mapStatusToException(resp.status(), resp.error(), topic, partition);
                }
                return resp.offset();
            } catch (IOException e) {
                throw new MiniKafkaException("Produce network error: " + e.getMessage(), e);
            }
        }
    }

    public long produce(String topic, int partition, String key, String value) {
        byte[] bytes = value == null ? new byte[0] : value.getBytes(StandardCharsets.UTF_8);
        return produce(topic, partition, key, bytes);
    }

    public List<Message> fetch(String topic, int partition, long fromOffset, int maxMessages) {
        ensureOpen();
        int reqId = requestIdSeq.incrementAndGet();
        synchronized (ioLock) {
            try {
                NetworkProtocol.writeFetchRequest(out, reqId, topic, partition, fromOffset, maxMessages);
                NetworkProtocol.Frame frame = readAndCheckResponseFrame(reqId, RequestType.FETCH);
                NetworkProtocol.FetchResponse resp = NetworkProtocol.decodeFetchResponse(frame, topic, partition);
                if (resp.status() != ResponseStatus.OK) {
                    throw mapStatusToException(resp.status(), resp.error(), topic, partition);
                }
                return resp.messages();
            } catch (IOException e) {
                throw new MiniKafkaException("Fetch network error: " + e.getMessage(), e);
            }
        }
    }

    public void createTopic(String topic, int partitionCount) {
        ensureOpen();
        int reqId = requestIdSeq.incrementAndGet();
        synchronized (ioLock) {
            try {
                NetworkProtocol.writeCreateTopicRequest(out, reqId, topic, partitionCount);
                NetworkProtocol.Frame frame = readAndCheckResponseFrame(reqId, RequestType.CREATE_TOPIC);
                NetworkProtocol.CreateTopicResponse resp = NetworkProtocol.decodeCreateTopicResponse(frame);
                if (resp.status() != ResponseStatus.OK) {
                    throw mapStatusToException(resp.status(), resp.error(), topic, -1);
                }
            } catch (IOException e) {
                throw new MiniKafkaException("CreateTopic network error: " + e.getMessage(), e);
            }
        }
    }

    public Set<String> listTopics() {
        ensureOpen();
        int reqId = requestIdSeq.incrementAndGet();
        synchronized (ioLock) {
            try {
                NetworkProtocol.writeListTopicsRequest(out, reqId);
                NetworkProtocol.Frame frame = readAndCheckResponseFrame(reqId, RequestType.LIST_TOPICS);
                NetworkProtocol.ListTopicsResponse resp = NetworkProtocol.decodeListTopicsResponse(frame);
                if (resp.status() != ResponseStatus.OK) {
                    throw mapStatusToException(resp.status(), resp.error(), null, -1);
                }
                return resp.topics();
            } catch (IOException e) {
                throw new MiniKafkaException("ListTopics network error: " + e.getMessage(), e);
            }
        }
    }

    public boolean ping() {
        ensureOpen();
        int reqId = requestIdSeq.incrementAndGet();
        synchronized (ioLock) {
            try {
                NetworkProtocol.writePingRequest(out, reqId);
                NetworkProtocol.Frame frame = readAndCheckResponseFrame(reqId, RequestType.PING);
                return frame.requestTypeCode() == RequestType.PING.getCode() && frame.requestId() == reqId;
            } catch (IOException e) {
                return false;
            }
        }
    }

    private NetworkProtocol.Frame readAndCheckResponseFrame(int expectedRequestId, RequestType expectedType)
            throws IOException {
        NetworkProtocol.Frame frame = NetworkProtocol.readValidatedFrame(in, maxFrameSize);
        if (frame.requestId() != expectedRequestId) {
            throw new ProtocolException(ResponseStatus.BAD_REQUEST,
                    "Request ID mismatch: expected " + expectedRequestId + ", got " + frame.requestId());
        }
        if (frame.requestTypeCode() == RequestType.ERROR.getCode()) {
            NetworkProtocol.ErrorResponse err = NetworkProtocol.decodeErrorResponse(frame);
            throw new ProtocolException(err.status(),
                    "Server protocol error [" + err.status() + "]: " + err.error());
        }
        if (frame.requestTypeCode() != expectedType.getCode()) {
            throw new ProtocolException(ResponseStatus.BAD_REQUEST,
                    "Unexpected response opcode: expected " + expectedType + ", got " + frame.requestTypeCode());
        }
        return frame;
    }

    private static RuntimeException mapStatusToException(ResponseStatus status, String error,
                                                         String topic, int partition) {
        return switch (status) {
            case TOPIC_NOT_FOUND -> new TopicNotFoundException(topic != null ? topic : error);
            case TOPIC_ALREADY_EXISTS -> new TopicAlreadyExistsException(topic != null ? topic : error);
            case PARTITION_NOT_FOUND -> new PartitionNotFoundException(
                    topic != null ? topic : "unknown", partition, -1);
            case OFFSET_OUT_OF_RANGE -> new OffsetOutOfRangeException(error, -1, 0, -1);
            case BAD_REQUEST, UNSUPPORTED_VERSION, UNKNOWN_REQUEST_TYPE, FRAME_TOO_LARGE ->
                    new ProtocolException(status, error);
            default -> new MiniKafkaException("Server error [" + status + "]: " + error);
        };
    }

    private void ensureOpen() {
        if (closed || (socket != null && socket.isClosed())) {
            throw new IllegalStateException("BrokerClient is closed");
        }
    }

    @Override
    public synchronized void close() {
        if (closed) return;
        closed = true;
        try {
            if (socket != null) {
                socket.close();
            }
        } catch (IOException ignored) {}
        try {
            in.close();
        } catch (IOException ignored) {}
        try {
            out.close();
        } catch (IOException ignored) {}
    }
}
