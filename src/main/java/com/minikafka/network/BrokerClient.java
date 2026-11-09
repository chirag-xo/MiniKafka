package com.minikafka.network;

import com.minikafka.exception.MiniKafkaException;
import com.minikafka.model.Message;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Thread-safe client communicating with a remote {@link BrokerServer} over TCP
 * using MiniKafka's binary framed protocol.
 */
public class BrokerClient implements AutoCloseable {

    private final String host;
    private final int port;
    private final Socket socket;
    private final DataInputStream in;
    private final DataOutputStream out;
    private final AtomicInteger correlationSeq = new AtomicInteger(1);
    private final Object ioLock = new Object();
    private volatile boolean closed = false;

    public BrokerClient(String host, int port) throws IOException {
        this.host = Objects.requireNonNull(host, "Host cannot be null");
        this.port = port;
        this.socket = new Socket();
        this.socket.setTcpNoDelay(true);
        this.socket.connect(new InetSocketAddress(host, port), 5000);
        this.in = new DataInputStream(new BufferedInputStream(socket.getInputStream()));
        this.out = new DataOutputStream(new BufferedOutputStream(socket.getOutputStream()));
    }

    public long produce(String topic, int partition, String key, byte[] value) {
        ensureOpen();
        int corrId = correlationSeq.incrementAndGet();
        synchronized (ioLock) {
            try {
                NetworkProtocol.writeProduceRequest(out, corrId, topic, partition, key, value);
                int frameLen = in.readInt();
                byte respType = in.readByte();
                int respCorrId = in.readInt();
                if (respCorrId != corrId) {
                    throw new MiniKafkaException("Correlation ID mismatch: expected " + corrId + ", got " + respCorrId);
                }
                NetworkProtocol.ProduceResponse resp = NetworkProtocol.readProduceResponse(in, respCorrId);
                if (resp.status() != NetworkProtocol.STATUS_OK) {
                    throw new MiniKafkaException("Produce failed on server: " + resp.error());
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
        int corrId = correlationSeq.incrementAndGet();
        synchronized (ioLock) {
            try {
                NetworkProtocol.writeFetchRequest(out, corrId, topic, partition, fromOffset, maxMessages);
                int frameLen = in.readInt();
                byte respType = in.readByte();
                int respCorrId = in.readInt();
                if (respCorrId != corrId) {
                    throw new MiniKafkaException("Correlation ID mismatch: expected " + corrId + ", got " + respCorrId);
                }
                NetworkProtocol.FetchResponse resp = NetworkProtocol.readFetchResponse(in, respCorrId, topic, partition);
                if (resp.status() != NetworkProtocol.STATUS_OK) {
                    throw new MiniKafkaException("Fetch failed on server: " + resp.error());
                }
                return resp.messages();
            } catch (IOException e) {
                throw new MiniKafkaException("Fetch network error: " + e.getMessage(), e);
            }
        }
    }

    public void createTopic(String topic, int partitionCount) {
        ensureOpen();
        int corrId = correlationSeq.incrementAndGet();
        synchronized (ioLock) {
            try {
                NetworkProtocol.writeCreateTopicRequest(out, corrId, topic, partitionCount);
                int frameLen = in.readInt();
                byte respType = in.readByte();
                int respCorrId = in.readInt();
                if (respCorrId != corrId) {
                    throw new MiniKafkaException("Correlation ID mismatch: expected " + corrId + ", got " + respCorrId);
                }
                NetworkProtocol.CreateTopicResponse resp = NetworkProtocol.readCreateTopicResponse(in, respCorrId);
                if (resp.status() != NetworkProtocol.STATUS_OK) {
                    throw new MiniKafkaException("Create topic failed: " + resp.error());
                }
            } catch (IOException e) {
                throw new MiniKafkaException("CreateTopic network error: " + e.getMessage(), e);
            }
        }
    }

    public Set<String> listTopics() {
        ensureOpen();
        int corrId = correlationSeq.incrementAndGet();
        synchronized (ioLock) {
            try {
                NetworkProtocol.writeListTopicsRequest(out, corrId);
                int frameLen = in.readInt();
                byte respType = in.readByte();
                int respCorrId = in.readInt();
                if (respCorrId != corrId) {
                    throw new MiniKafkaException("Correlation ID mismatch: expected " + corrId + ", got " + respCorrId);
                }
                NetworkProtocol.ListTopicsResponse resp = NetworkProtocol.readListTopicsResponse(in, respCorrId);
                if (resp.status() != NetworkProtocol.STATUS_OK) {
                    throw new MiniKafkaException("List topics failed: " + resp.error());
                }
                return resp.topics();
            } catch (IOException e) {
                throw new MiniKafkaException("ListTopics network error: " + e.getMessage(), e);
            }
        }
    }

    public boolean ping() {
        ensureOpen();
        int corrId = correlationSeq.incrementAndGet();
        synchronized (ioLock) {
            try {
                out.writeInt(5);
                out.writeByte(RequestType.PING.getCode());
                out.writeInt(corrId);
                out.flush();

                int frameLen = in.readInt();
                byte respType = in.readByte();
                int respCorrId = in.readInt();
                return respType == RequestType.PING.getCode() && respCorrId == corrId;
            } catch (IOException e) {
                return false;
            }
        }
    }

    private void ensureOpen() {
        if (closed || socket.isClosed()) {
            throw new IllegalStateException("BrokerClient is closed");
        }
    }

    @Override
    public synchronized void close() {
        if (closed) return;
        closed = true;
        try {
            socket.close();
        } catch (IOException ignored) {}
    }
}

