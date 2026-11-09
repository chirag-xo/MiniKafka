package com.minikafka.network;

import com.minikafka.model.Message;
import com.minikafka.storage.RecordSerializer;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

/**
 * Binary framed network protocol codecs for MiniKafka.
 *
 * <p>Frame Format:
 * <pre>
 * [total_frame_len: 4B] (excludes itself)
 * [request_type: 1B]
 * [correlation_id: 4B]
 * [payload: variable]
 * </pre>
 */
public final class NetworkProtocol {

    public static final byte STATUS_OK = 0;
    public static final byte STATUS_ERROR = 1;

    private NetworkProtocol() {}

    // -------------------------------------------------------------------------
    // PRODUCE
    // -------------------------------------------------------------------------

    public static void writeProduceRequest(DataOutputStream out, int correlationId,
                                           String topic, int partition, String key, byte[] value) throws IOException {
        byte[] topicBytes = topic.getBytes(StandardCharsets.UTF_8);
        byte[] keyBytes = key == null ? null : key.getBytes(StandardCharsets.UTF_8);
        int valLen = value == null ? 0 : value.length;

        int payloadLen = 1 + 4 // type (1B) + correlationId (4B)
                + 2 + topicBytes.length
                + 4 // partition
                + 4 + (keyBytes == null ? 0 : keyBytes.length)
                + 4 + valLen;

        out.writeInt(payloadLen);
        out.writeByte(RequestType.PRODUCE.getCode());
        out.writeInt(correlationId);

        out.writeShort(topicBytes.length);
        out.write(topicBytes);
        out.writeInt(partition);

        if (keyBytes == null) {
            out.writeInt(-1);
        } else {
            out.writeInt(keyBytes.length);
            out.write(keyBytes);
        }

        out.writeInt(valLen);
        if (value != null && valLen > 0) {
            out.write(value);
        }
        out.flush();
    }

    public record ProduceRequest(int correlationId, String topic, int partition, String key, byte[] value) {}

    public static ProduceRequest readProduceRequest(DataInputStream in, int correlationId) throws IOException {
        int topicLen = in.readShort();
        byte[] topicBytes = new byte[topicLen];
        in.readFully(topicBytes);
        String topic = new String(topicBytes, StandardCharsets.UTF_8);

        int partition = in.readInt();

        int keyLen = in.readInt();
        String key = null;
        if (keyLen >= 0) {
            byte[] keyBytes = new byte[keyLen];
            in.readFully(keyBytes);
            key = new String(keyBytes, StandardCharsets.UTF_8);
        }

        int valLen = in.readInt();
        byte[] value = new byte[valLen];
        in.readFully(value);

        return new ProduceRequest(correlationId, topic, partition, key, value);
    }

    public static void writeProduceResponse(DataOutputStream out, int correlationId,
                                            byte status, long offset, String error) throws IOException {
        byte[] errBytes = (error == null) ? new byte[0] : error.getBytes(StandardCharsets.UTF_8);
        int payloadLen = 1 + 4 + 1 + 8 + 2 + errBytes.length;

        out.writeInt(payloadLen);
        out.writeByte(RequestType.PRODUCE.getCode());
        out.writeInt(correlationId);
        out.writeByte(status);
        out.writeLong(offset);
        out.writeShort(errBytes.length);
        if (errBytes.length > 0) {
            out.write(errBytes);
        }
        out.flush();
    }

    public record ProduceResponse(int correlationId, byte status, long offset, String error) {}

    public static ProduceResponse readProduceResponse(DataInputStream in, int correlationId) throws IOException {
        byte status = in.readByte();
        long offset = in.readLong();
        int errLen = in.readShort();
        String error = null;
        if (errLen > 0) {
            byte[] errBytes = new byte[errLen];
            in.readFully(errBytes);
            error = new String(errBytes, StandardCharsets.UTF_8);
        }
        return new ProduceResponse(correlationId, status, offset, error);
    }

    // -------------------------------------------------------------------------
    // FETCH
    // -------------------------------------------------------------------------

    public static void writeFetchRequest(DataOutputStream out, int correlationId,
                                         String topic, int partition, long fromOffset, int maxMessages) throws IOException {
        byte[] topicBytes = topic.getBytes(StandardCharsets.UTF_8);
        int payloadLen = 1 + 4 + 2 + topicBytes.length + 4 + 8 + 4;

        out.writeInt(payloadLen);
        out.writeByte(RequestType.FETCH.getCode());
        out.writeInt(correlationId);
        out.writeShort(topicBytes.length);
        out.write(topicBytes);
        out.writeInt(partition);
        out.writeLong(fromOffset);
        out.writeInt(maxMessages);
        out.flush();
    }

    public record FetchRequest(int correlationId, String topic, int partition, long fromOffset, int maxMessages) {}

    public static FetchRequest readFetchRequest(DataInputStream in, int correlationId) throws IOException {
        int topicLen = in.readShort();
        byte[] topicBytes = new byte[topicLen];
        in.readFully(topicBytes);
        String topic = new String(topicBytes, StandardCharsets.UTF_8);

        int partition = in.readInt();
        long fromOffset = in.readLong();
        int maxMessages = in.readInt();
        return new FetchRequest(correlationId, topic, partition, fromOffset, maxMessages);
    }

    public static void writeFetchResponse(DataOutputStream out, int correlationId,
                                          byte status, List<Message> messages, String error) throws IOException {
        byte[] errBytes = (error == null) ? new byte[0] : error.getBytes(StandardCharsets.UTF_8);

        List<byte[]> serializedMsgs = new ArrayList<>();
        int msgsTotalBytes = 0;
        if (messages != null) {
            for (Message m : messages) {
                ByteBuffer buf = RecordSerializer.serialize(m);
                byte[] raw = new byte[buf.remaining()];
                buf.get(raw);
                serializedMsgs.add(raw);
                msgsTotalBytes += raw.length;
            }
        }

        int payloadLen = 1 + 4 + 1 + 2 + errBytes.length + 4 + msgsTotalBytes;

        out.writeInt(payloadLen);
        out.writeByte(RequestType.FETCH.getCode());
        out.writeInt(correlationId);
        out.writeByte(status);
        out.writeShort(errBytes.length);
        if (errBytes.length > 0) {
            out.write(errBytes);
        }
        out.writeInt(serializedMsgs.size());
        for (byte[] raw : serializedMsgs) {
            out.write(raw);
        }
        out.flush();
    }

    public record FetchResponse(int correlationId, byte status, List<Message> messages, String error) {}

    public static FetchResponse readFetchResponse(DataInputStream in, int correlationId, String topic, int partition) throws IOException {
        byte status = in.readByte();
        int errLen = in.readShort();
        String error = null;
        if (errLen > 0) {
            byte[] errBytes = new byte[errLen];
            in.readFully(errBytes);
            error = new String(errBytes, StandardCharsets.UTF_8);
        }

        int msgCount = in.readInt();
        List<Message> messages = new ArrayList<>(msgCount);
        for (int i = 0; i < msgCount; i++) {
            int bodyLen = in.readInt();
            ByteBuffer buf = ByteBuffer.allocate(4 + bodyLen);
            buf.putInt(bodyLen);
            byte[] body = new byte[bodyLen];
            in.readFully(body);
            buf.put(body);
            buf.flip();
            messages.add(RecordSerializer.deserialize(buf, topic, partition));
        }

        return new FetchResponse(correlationId, status, messages, error);
    }

    // -------------------------------------------------------------------------
    // CREATE_TOPIC
    // -------------------------------------------------------------------------

    public static void writeCreateTopicRequest(DataOutputStream out, int correlationId,
                                               String topic, int partitionCount) throws IOException {
        byte[] topicBytes = topic.getBytes(StandardCharsets.UTF_8);
        int payloadLen = 1 + 4 + 2 + topicBytes.length + 4;

        out.writeInt(payloadLen);
        out.writeByte(RequestType.CREATE_TOPIC.getCode());
        out.writeInt(correlationId);
        out.writeShort(topicBytes.length);
        out.write(topicBytes);
        out.writeInt(partitionCount);
        out.flush();
    }

    public record CreateTopicRequest(int correlationId, String topic, int partitionCount) {}

    public static CreateTopicRequest readCreateTopicRequest(DataInputStream in, int correlationId) throws IOException {
        int topicLen = in.readShort();
        byte[] topicBytes = new byte[topicLen];
        in.readFully(topicBytes);
        String topic = new String(topicBytes, StandardCharsets.UTF_8);
        int partitions = in.readInt();
        return new CreateTopicRequest(correlationId, topic, partitions);
    }

    public static void writeCreateTopicResponse(DataOutputStream out, int correlationId,
                                                byte status, String error) throws IOException {
        byte[] errBytes = (error == null) ? new byte[0] : error.getBytes(StandardCharsets.UTF_8);
        int payloadLen = 1 + 4 + 1 + 2 + errBytes.length;

        out.writeInt(payloadLen);
        out.writeByte(RequestType.CREATE_TOPIC.getCode());
        out.writeInt(correlationId);
        out.writeByte(status);
        out.writeShort(errBytes.length);
        if (errBytes.length > 0) {
            out.write(errBytes);
        }
        out.flush();
    }

    public record CreateTopicResponse(int correlationId, byte status, String error) {}

    public static CreateTopicResponse readCreateTopicResponse(DataInputStream in, int correlationId) throws IOException {
        byte status = in.readByte();
        int errLen = in.readShort();
        String error = null;
        if (errLen > 0) {
            byte[] errBytes = new byte[errLen];
            in.readFully(errBytes);
            error = new String(errBytes, StandardCharsets.UTF_8);
        }
        return new CreateTopicResponse(correlationId, status, error);
    }

    // -------------------------------------------------------------------------
    // LIST_TOPICS
    // -------------------------------------------------------------------------

    public static void writeListTopicsRequest(DataOutputStream out, int correlationId) throws IOException {
        int payloadLen = 1 + 4;
        out.writeInt(payloadLen);
        out.writeByte(RequestType.LIST_TOPICS.getCode());
        out.writeInt(correlationId);
        out.flush();
    }

    public static void writeListTopicsResponse(DataOutputStream out, int correlationId,
                                               byte status, Set<String> topics, String error) throws IOException {
        byte[] errBytes = (error == null) ? new byte[0] : error.getBytes(StandardCharsets.UTF_8);

        List<byte[]> topicBytesList = new ArrayList<>();
        int topicsBytesTotal = 0;
        if (topics != null) {
            for (String t : topics) {
                byte[] tb = t.getBytes(StandardCharsets.UTF_8);
                topicBytesList.add(tb);
                topicsBytesTotal += (2 + tb.length);
            }
        }

        int payloadLen = 1 + 4 + 1 + 2 + errBytes.length + 4 + topicsBytesTotal;

        out.writeInt(payloadLen);
        out.writeByte(RequestType.LIST_TOPICS.getCode());
        out.writeInt(correlationId);
        out.writeByte(status);
        out.writeShort(errBytes.length);
        if (errBytes.length > 0) {
            out.write(errBytes);
        }
        out.writeInt(topicBytesList.size());
        for (byte[] tb : topicBytesList) {
            out.writeShort(tb.length);
            out.write(tb);
        }
        out.flush();
    }

    public record ListTopicsResponse(int correlationId, byte status, Set<String> topics, String error) {}

    public static ListTopicsResponse readListTopicsResponse(DataInputStream in, int correlationId) throws IOException {
        byte status = in.readByte();
        int errLen = in.readShort();
        String error = null;
        if (errLen > 0) {
            byte[] errBytes = new byte[errLen];
            in.readFully(errBytes);
            error = new String(errBytes, StandardCharsets.UTF_8);
        }

        int count = in.readInt();
        Set<String> topics = new HashSet<>(count);
        for (int i = 0; i < count; i++) {
            int len = in.readShort();
            byte[] tb = new byte[len];
            in.readFully(tb);
            topics.add(new String(tb, StandardCharsets.UTF_8));
        }

        return new ListTopicsResponse(correlationId, status, topics, error);
    }
}

