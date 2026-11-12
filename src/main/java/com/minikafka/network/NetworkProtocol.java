package com.minikafka.network;

import com.minikafka.exception.CorruptRecordException;
import com.minikafka.exception.ProtocolException;
import com.minikafka.model.Message;
import com.minikafka.storage.RecordSerializer;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.EOFException;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

/**
 * Versioned, length-prefixed binary wire protocol codecs for MiniKafka TCP communication.
 *
 * <p>Frame Format:
 * <pre>
 * [frame_length: 4B]     Length in bytes of all fields following this integer (HEADER_SIZE + payload.length)
 * [protocol_version: 1B] Wire protocol version (currently {@link #PROTOCOL_VERSION})
 * [request_type: 1B]     {@link RequestType} operation code
 * [request_id: 4B]       Client-assigned request correlation ID echoed in the response
 * [payload: variable]    Operation-specific payload bytes
 * </pre>
 */
public final class NetworkProtocol {

    public static final byte PROTOCOL_VERSION = 1;
    public static final int HEADER_SIZE = 1 + 1 + 4; // version (1B) + type (1B) + requestId (4B)
    public static final int DEFAULT_MAX_FRAME_SIZE = 4 * 1024 * 1024; // 4 MB

    private NetworkProtocol() {}

    public record Frame(byte version, byte requestTypeCode, int requestId, byte[] payload) {}

    public record ProduceRequest(int requestId, String topic, int partition, String key, byte[] value) {}
    public record ProduceResponse(int requestId, ResponseStatus status, long offset, String error) {}

    public record FetchRequest(int requestId, String topic, int partition, long fromOffset, int maxMessages) {}
    public record FetchResponse(int requestId, ResponseStatus status, List<Message> messages, String error) {}

    public record CreateTopicRequest(int requestId, String topic, int partitionCount) {}
    public record CreateTopicResponse(int requestId, ResponseStatus status, String error) {}

    public record ListTopicsResponse(int requestId, ResponseStatus status, Set<String> topics, String error) {}
    public record ErrorResponse(int requestId, ResponseStatus status, String error) {}

    // -------------------------------------------------------------------------
    // Core Frame I/O
    // -------------------------------------------------------------------------

    public static void writeFrame(DataOutputStream out, RequestType type, int requestId,
                                  byte[] payload, int maxFrameSize) throws IOException {
        byte[] body = (payload == null) ? new byte[0] : payload;
        int frameLen = HEADER_SIZE + body.length;
        if (frameLen > maxFrameSize) {
            throw new ProtocolException(ResponseStatus.FRAME_TOO_LARGE,
                    String.format("Outgoing frame size %d exceeds max frame size %d", frameLen, maxFrameSize));
        }
        out.writeInt(frameLen);
        out.writeByte(PROTOCOL_VERSION);
        out.writeByte(type.getCode());
        out.writeInt(requestId);
        if (body.length > 0) {
            out.write(body);
        }
        out.flush();
    }

    /**
     * Reads a complete frame from the stream, validating frame bounds before allocating memory.
     *
     * @throws EOFException      if the peer closed the stream cleanly before a new frame started.
     * @throws ProtocolException if the frame is smaller than {@link #HEADER_SIZE} or exceeds {@code maxFrameSize}.
     */
    public static Frame readRawFrame(DataInputStream in, int maxFrameSize) throws IOException {
        int frameLen = in.readInt();
        if (frameLen < HEADER_SIZE) {
            throw new ProtocolException(ResponseStatus.BAD_REQUEST,
                    "Invalid frame length " + frameLen + " (minimum header size is " + HEADER_SIZE + ")");
        }
        if (frameLen > maxFrameSize) {
            throw new ProtocolException(ResponseStatus.FRAME_TOO_LARGE,
                    "Frame length " + frameLen + " exceeds maximum allowed " + maxFrameSize);
        }

        byte version = in.readByte();
        byte typeCode = in.readByte();
        int requestId = in.readInt();

        int payloadLen = frameLen - HEADER_SIZE;
        byte[] payload = new byte[payloadLen];
        if (payloadLen > 0) {
            in.readFully(payload);
        }

        return new Frame(version, typeCode, requestId, payload);
    }

    public static Frame readValidatedFrame(DataInputStream in, int maxFrameSize) throws IOException {
        Frame frame = readRawFrame(in, maxFrameSize);
        if (frame.version() != PROTOCOL_VERSION) {
            throw new ProtocolException(ResponseStatus.UNSUPPORTED_VERSION,
                    "Unsupported protocol version: " + frame.version() + " (expected " + PROTOCOL_VERSION + ")");
        }
        try {
            RequestType.fromCode(frame.requestTypeCode());
        } catch (IllegalArgumentException e) {
            throw new ProtocolException(ResponseStatus.UNKNOWN_REQUEST_TYPE,
                    "Unknown request type code: " + frame.requestTypeCode());
        }
        return frame;
    }

    // -------------------------------------------------------------------------
    // ERROR & PING
    // -------------------------------------------------------------------------

    public static void writeErrorResponse(DataOutputStream out, int requestId,
                                          ResponseStatus status, String error) throws IOException {
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        DataOutputStream p = new DataOutputStream(baos);
        writeStatusAndError(p, status, error);
        writeFrame(out, RequestType.ERROR, requestId, baos.toByteArray(), DEFAULT_MAX_FRAME_SIZE);
    }

    public static ErrorResponse decodeErrorResponse(Frame frame) throws IOException {
        DataInputStream in = payloadStream(frame);
        ResponseStatus status = ResponseStatus.fromCode(readByte(in));
        String error = readNullableShortString(in);
        ensureConsumed(in);
        return new ErrorResponse(frame.requestId(), status, error);
    }

    public static void writePingRequest(DataOutputStream out, int requestId) throws IOException {
        writeFrame(out, RequestType.PING, requestId, new byte[0], DEFAULT_MAX_FRAME_SIZE);
    }

    public static void writePingResponse(DataOutputStream out, int requestId) throws IOException {
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        DataOutputStream p = new DataOutputStream(baos);
        writeStatusAndError(p, ResponseStatus.OK, null);
        writeFrame(out, RequestType.PING, requestId, baos.toByteArray(), DEFAULT_MAX_FRAME_SIZE);
    }

    // -------------------------------------------------------------------------
    // PRODUCE
    // -------------------------------------------------------------------------

    public static void writeProduceRequest(DataOutputStream out, int requestId,
                                           String topic, int partition, String key, byte[] value,
                                           int maxFrameSize) throws IOException {
        byte[] topicBytes = topic.getBytes(StandardCharsets.UTF_8);
        byte[] keyBytes = key == null ? null : key.getBytes(StandardCharsets.UTF_8);
        byte[] valBytes = value == null ? new byte[0] : value;

        ByteArrayOutputStream baos = new ByteArrayOutputStream(
                2 + topicBytes.length + 4 + 4 + (keyBytes == null ? 0 : keyBytes.length) + 4 + valBytes.length);
        DataOutputStream p = new DataOutputStream(baos);
        p.writeShort(topicBytes.length);
        p.write(topicBytes);
        p.writeInt(partition);
        if (keyBytes == null) {
            p.writeInt(-1);
        } else {
            p.writeInt(keyBytes.length);
            p.write(keyBytes);
        }
        p.writeInt(valBytes.length);
        p.write(valBytes);
        writeFrame(out, RequestType.PRODUCE, requestId, baos.toByteArray(), maxFrameSize);
    }

    public static void writeProduceRequest(DataOutputStream out, int requestId,
                                           String topic, int partition, String key, byte[] value) throws IOException {
        writeProduceRequest(out, requestId, topic, partition, key, value, DEFAULT_MAX_FRAME_SIZE);
    }

    public static ProduceRequest decodeProduceRequest(Frame frame) throws IOException {
        DataInputStream in = payloadStream(frame);
        String topic = readRequiredShortString(in, "topic");
        int partition = readInt(in);
        byte[] keyBytes = readNullableIntBytes(in, "key");
        String key = (keyBytes == null) ? null : new String(keyBytes, StandardCharsets.UTF_8);
        byte[] value = readRequiredIntBytes(in, "value");
        ensureConsumed(in);
        return new ProduceRequest(frame.requestId(), topic, partition, key, value);
    }

    public static void writeProduceResponse(DataOutputStream out, int requestId,
                                            ResponseStatus status, long offset, String error) throws IOException {
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        DataOutputStream p = new DataOutputStream(baos);
        writeStatusAndError(p, status, error);
        p.writeLong(offset);
        writeFrame(out, RequestType.PRODUCE, requestId, baos.toByteArray(), DEFAULT_MAX_FRAME_SIZE);
    }

    public static ProduceResponse decodeProduceResponse(Frame frame) throws IOException {
        DataInputStream in = payloadStream(frame);
        ResponseStatus status = ResponseStatus.fromCode(readByte(in));
        String error = readNullableShortString(in);
        long offset = readLong(in);
        ensureConsumed(in);
        return new ProduceResponse(frame.requestId(), status, offset, error);
    }

    // -------------------------------------------------------------------------
    // FETCH
    // -------------------------------------------------------------------------

    public static void writeFetchRequest(DataOutputStream out, int requestId,
                                         String topic, int partition, long fromOffset, int maxMessages) throws IOException {
        byte[] topicBytes = topic.getBytes(StandardCharsets.UTF_8);
        ByteArrayOutputStream baos = new ByteArrayOutputStream(2 + topicBytes.length + 4 + 8 + 4);
        DataOutputStream p = new DataOutputStream(baos);
        p.writeShort(topicBytes.length);
        p.write(topicBytes);
        p.writeInt(partition);
        p.writeLong(fromOffset);
        p.writeInt(maxMessages);
        writeFrame(out, RequestType.FETCH, requestId, baos.toByteArray(), DEFAULT_MAX_FRAME_SIZE);
    }

    public static FetchRequest decodeFetchRequest(Frame frame) throws IOException {
        DataInputStream in = payloadStream(frame);
        String topic = readRequiredShortString(in, "topic");
        int partition = readInt(in);
        long fromOffset = readLong(in);
        int maxMessages = readInt(in);
        ensureConsumed(in);
        return new FetchRequest(frame.requestId(), topic, partition, fromOffset, maxMessages);
    }

    public static void writeFetchResponse(DataOutputStream out, int requestId,
                                          ResponseStatus status, List<Message> messages, String error,
                                          int maxFrameSize) throws IOException {
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        DataOutputStream p = new DataOutputStream(baos);
        writeStatusAndError(p, status, error);
        int count = (messages == null) ? 0 : messages.size();
        p.writeInt(count);
        if (messages != null) {
            for (Message m : messages) {
                ByteBuffer buf = RecordSerializer.serialize(m);
                byte[] raw = new byte[buf.remaining()];
                buf.get(raw);
                p.write(raw);
            }
        }
        writeFrame(out, RequestType.FETCH, requestId, baos.toByteArray(), maxFrameSize);
    }

    public static void writeFetchResponse(DataOutputStream out, int requestId,
                                          ResponseStatus status, List<Message> messages, String error) throws IOException {
        writeFetchResponse(out, requestId, status, messages, error, DEFAULT_MAX_FRAME_SIZE);
    }

    public static FetchResponse decodeFetchResponse(Frame frame, String topic, int partition) throws IOException {
        DataInputStream in = payloadStream(frame);
        ResponseStatus status = ResponseStatus.fromCode(readByte(in));
        String error = readNullableShortString(in);
        int msgCount = readInt(in);
        if (msgCount < 0 || msgCount > in.available()) {
            throw new ProtocolException(ResponseStatus.BAD_REQUEST, "Invalid message count in fetch response: " + msgCount);
        }

        List<Message> messages = new ArrayList<>(msgCount);
        for (int i = 0; i < msgCount; i++) {
            int bodyLen = readInt(in);
            if (bodyLen <= 0 || bodyLen > in.available()) {
                throw new ProtocolException(ResponseStatus.BAD_REQUEST, "Invalid serialized record length: " + bodyLen);
            }
            ByteBuffer buf = ByteBuffer.allocate(4 + bodyLen);
            buf.putInt(bodyLen);
            byte[] body = new byte[bodyLen];
            in.readFully(body);
            buf.put(body);
            buf.flip();
            try {
                messages.add(RecordSerializer.deserialize(buf, topic, partition));
            } catch (CorruptRecordException e) {
                throw new ProtocolException(ResponseStatus.BAD_REQUEST, "Corrupt message in fetch response", e);
            }
        }
        ensureConsumed(in);
        return new FetchResponse(frame.requestId(), status, messages, error);
    }

    // -------------------------------------------------------------------------
    // CREATE_TOPIC
    // -------------------------------------------------------------------------

    public static void writeCreateTopicRequest(DataOutputStream out, int requestId,
                                               String topic, int partitionCount) throws IOException {
        byte[] topicBytes = topic.getBytes(StandardCharsets.UTF_8);
        ByteArrayOutputStream baos = new ByteArrayOutputStream(2 + topicBytes.length + 4);
        DataOutputStream p = new DataOutputStream(baos);
        p.writeShort(topicBytes.length);
        p.write(topicBytes);
        p.writeInt(partitionCount);
        writeFrame(out, RequestType.CREATE_TOPIC, requestId, baos.toByteArray(), DEFAULT_MAX_FRAME_SIZE);
    }

    public static CreateTopicRequest decodeCreateTopicRequest(Frame frame) throws IOException {
        DataInputStream in = payloadStream(frame);
        String topic = readRequiredShortString(in, "topic");
        int partitions = readInt(in);
        ensureConsumed(in);
        return new CreateTopicRequest(frame.requestId(), topic, partitions);
    }

    public static void writeCreateTopicResponse(DataOutputStream out, int requestId,
                                                ResponseStatus status, String error) throws IOException {
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        DataOutputStream p = new DataOutputStream(baos);
        writeStatusAndError(p, status, error);
        writeFrame(out, RequestType.CREATE_TOPIC, requestId, baos.toByteArray(), DEFAULT_MAX_FRAME_SIZE);
    }

    public static CreateTopicResponse decodeCreateTopicResponse(Frame frame) throws IOException {
        DataInputStream in = payloadStream(frame);
        ResponseStatus status = ResponseStatus.fromCode(readByte(in));
        String error = readNullableShortString(in);
        ensureConsumed(in);
        return new CreateTopicResponse(frame.requestId(), status, error);
    }

    // -------------------------------------------------------------------------
    // LIST_TOPICS
    // -------------------------------------------------------------------------

    public static void writeListTopicsRequest(DataOutputStream out, int requestId) throws IOException {
        writeFrame(out, RequestType.LIST_TOPICS, requestId, new byte[0], DEFAULT_MAX_FRAME_SIZE);
    }

    public static void writeListTopicsResponse(DataOutputStream out, int requestId,
                                               ResponseStatus status, Set<String> topics, String error) throws IOException {
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        DataOutputStream p = new DataOutputStream(baos);
        writeStatusAndError(p, status, error);
        int count = (topics == null) ? 0 : topics.size();
        p.writeInt(count);
        if (topics != null) {
            for (String t : topics) {
                byte[] tb = t.getBytes(StandardCharsets.UTF_8);
                p.writeShort(tb.length);
                p.write(tb);
            }
        }
        writeFrame(out, RequestType.LIST_TOPICS, requestId, baos.toByteArray(), DEFAULT_MAX_FRAME_SIZE);
    }

    public static ListTopicsResponse decodeListTopicsResponse(Frame frame) throws IOException {
        DataInputStream in = payloadStream(frame);
        ResponseStatus status = ResponseStatus.fromCode(readByte(in));
        String error = readNullableShortString(in);
        int count = readInt(in);
        if (count < 0 || count > in.available()) {
            throw new ProtocolException(ResponseStatus.BAD_REQUEST, "Invalid topic count: " + count);
        }
        Set<String> topics = new HashSet<>(count);
        for (int i = 0; i < count; i++) {
            topics.add(readRequiredShortString(in, "topicName"));
        }
        ensureConsumed(in);
        return new ListTopicsResponse(frame.requestId(), status, topics, error);
    }

    // -------------------------------------------------------------------------
    // Bounded Payload Parsing Helpers
    // -------------------------------------------------------------------------

    private static DataInputStream payloadStream(Frame frame) {
        return new DataInputStream(new ByteArrayInputStream(frame.payload()));
    }

    private static void writeStatusAndError(DataOutputStream p, ResponseStatus status, String error) throws IOException {
        p.writeByte(status.getCode());
        byte[] errBytes = (error == null) ? new byte[0] : error.getBytes(StandardCharsets.UTF_8);
        int len = Math.min(errBytes.length, Short.MAX_VALUE);
        p.writeShort(len);
        if (len > 0) {
            p.write(errBytes, 0, len);
        }
    }

    private static byte readByte(DataInputStream in) throws IOException {
        try {
            return in.readByte();
        } catch (EOFException e) {
            throw new ProtocolException(ResponseStatus.BAD_REQUEST, "Truncated frame payload (missing byte)", e);
        }
    }

    private static int readInt(DataInputStream in) throws IOException {
        try {
            return in.readInt();
        } catch (EOFException e) {
            throw new ProtocolException(ResponseStatus.BAD_REQUEST, "Truncated frame payload (missing int)", e);
        }
    }

    private static long readLong(DataInputStream in) throws IOException {
        try {
            return in.readLong();
        } catch (EOFException e) {
            throw new ProtocolException(ResponseStatus.BAD_REQUEST, "Truncated frame payload (missing long)", e);
        }
    }

    private static String readRequiredShortString(DataInputStream in, String field) throws IOException {
        int len;
        try {
            len = in.readShort();
        } catch (EOFException e) {
            throw new ProtocolException(ResponseStatus.BAD_REQUEST, "Truncated field length for " + field, e);
        }
        if (len <= 0 || len > in.available()) {
            throw new ProtocolException(ResponseStatus.BAD_REQUEST,
                    String.format("Invalid length %d for field '%s' (remaining=%d)", len, field, in.available()));
        }
        byte[] bytes = new byte[len];
        in.readFully(bytes);
        return new String(bytes, StandardCharsets.UTF_8);
    }

    private static String readNullableShortString(DataInputStream in) throws IOException {
        int len;
        try {
            len = in.readShort();
        } catch (EOFException e) {
            throw new ProtocolException(ResponseStatus.BAD_REQUEST, "Truncated error string length", e);
        }
        if (len < 0 || len > in.available()) {
            throw new ProtocolException(ResponseStatus.BAD_REQUEST, "Invalid error string length: " + len);
        }
        if (len == 0) {
            return null;
        }
        byte[] bytes = new byte[len];
        in.readFully(bytes);
        return new String(bytes, StandardCharsets.UTF_8);
    }

    private static byte[] readNullableIntBytes(DataInputStream in, String field) throws IOException {
        int len = readInt(in);
        if (len == -1) {
            return null;
        }
        if (len < -1 || len > in.available()) {
            throw new ProtocolException(ResponseStatus.BAD_REQUEST,
                    String.format("Invalid byte length %d for field '%s' (remaining=%d)", len, field, in.available()));
        }
        byte[] bytes = new byte[len];
        in.readFully(bytes);
        return bytes;
    }

    private static byte[] readRequiredIntBytes(DataInputStream in, String field) throws IOException {
        int len = readInt(in);
        if (len < 0 || len > in.available()) {
            throw new ProtocolException(ResponseStatus.BAD_REQUEST,
                    String.format("Invalid byte length %d for field '%s' (remaining=%d)", len, field, in.available()));
        }
        byte[] bytes = new byte[len];
        in.readFully(bytes);
        return bytes;
    }

    private static void ensureConsumed(DataInputStream in) throws IOException {
        if (in.available() > 0) {
            throw new ProtocolException(ResponseStatus.BAD_REQUEST,
                    "Unexpected trailing bytes in frame payload: " + in.available());
        }
    }
}
