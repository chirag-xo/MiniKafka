package com.minikafka.network;

import com.minikafka.exception.ProtocolException;
import com.minikafka.model.Message;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Set;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

@DisplayName("NetworkProtocol Frame, Codec & Malformed Payload Tests")
class NetworkProtocolTest {

    private DataInputStream toDataIn(ByteArrayOutputStream baos) {
        return new DataInputStream(new ByteArrayInputStream(baos.toByteArray()));
    }

    @Nested
    @DisplayName("Produce Codec Tests")
    class ProduceCodecTests {

        @Test
        @DisplayName("Should encode and decode versioned PRODUCE request with key and value")
        void shouldCodecProduceRequest() throws IOException {
            ByteArrayOutputStream baos = new ByteArrayOutputStream();
            DataOutputStream out = new DataOutputStream(baos);

            NetworkProtocol.writeProduceRequest(
                    out, 101, "orders", 2, "key1", "order-payload".getBytes(StandardCharsets.UTF_8));

            NetworkProtocol.Frame frame = NetworkProtocol.readValidatedFrame(
                    toDataIn(baos), NetworkProtocol.DEFAULT_MAX_FRAME_SIZE);
            assertThat(frame.version()).isEqualTo(NetworkProtocol.PROTOCOL_VERSION);
            assertThat(frame.requestTypeCode()).isEqualTo(RequestType.PRODUCE.getCode());
            assertThat(frame.requestId()).isEqualTo(101);

            NetworkProtocol.ProduceRequest req = NetworkProtocol.decodeProduceRequest(frame);
            assertThat(req.requestId()).isEqualTo(101);
            assertThat(req.topic()).isEqualTo("orders");
            assertThat(req.partition()).isEqualTo(2);
            assertThat(req.key()).isEqualTo("key1");
            assertThat(new String(req.value(), StandardCharsets.UTF_8)).isEqualTo("order-payload");
        }

        @Test
        @DisplayName("Should encode and decode PRODUCE request with null key")
        void shouldCodecProduceRequestWithNullKey() throws IOException {
            ByteArrayOutputStream baos = new ByteArrayOutputStream();
            DataOutputStream out = new DataOutputStream(baos);

            NetworkProtocol.writeProduceRequest(
                    out, 102, "events", 0, null, "data".getBytes(StandardCharsets.UTF_8));

            NetworkProtocol.Frame frame = NetworkProtocol.readValidatedFrame(
                    toDataIn(baos), NetworkProtocol.DEFAULT_MAX_FRAME_SIZE);
            NetworkProtocol.ProduceRequest req = NetworkProtocol.decodeProduceRequest(frame);

            assertThat(req.key()).isNull();
            assertThat(req.topic()).isEqualTo("events");
        }

        @Test
        @DisplayName("Should encode and decode PRODUCE response with ResponseStatus")
        void shouldCodecProduceResponse() throws IOException {
            ByteArrayOutputStream baos = new ByteArrayOutputStream();
            DataOutputStream out = new DataOutputStream(baos);

            NetworkProtocol.writeProduceResponse(out, 103, ResponseStatus.OK, 42L, null);

            NetworkProtocol.Frame frame = NetworkProtocol.readValidatedFrame(
                    toDataIn(baos), NetworkProtocol.DEFAULT_MAX_FRAME_SIZE);
            NetworkProtocol.ProduceResponse resp = NetworkProtocol.decodeProduceResponse(frame);

            assertThat(resp.requestId()).isEqualTo(103);
            assertThat(resp.status()).isEqualTo(ResponseStatus.OK);
            assertThat(resp.offset()).isEqualTo(42L);
            assertThat(resp.error()).isNull();
        }
    }

    @Nested
    @DisplayName("Fetch Codec Tests")
    class FetchCodecTests {

        @Test
        @DisplayName("Should encode and decode FETCH request and response")
        void shouldCodecFetchRequestAndResponse() throws IOException {
            ByteArrayOutputStream baos = new ByteArrayOutputStream();
            DataOutputStream out = new DataOutputStream(baos);

            NetworkProtocol.writeFetchRequest(out, 201, "orders", 1, 10L, 5);

            NetworkProtocol.Frame reqFrame = NetworkProtocol.readValidatedFrame(
                    toDataIn(baos), NetworkProtocol.DEFAULT_MAX_FRAME_SIZE);
            NetworkProtocol.FetchRequest req = NetworkProtocol.decodeFetchRequest(reqFrame);

            assertThat(req.requestId()).isEqualTo(201);
            assertThat(req.topic()).isEqualTo("orders");
            assertThat(req.partition()).isEqualTo(1);
            assertThat(req.fromOffset()).isEqualTo(10L);
            assertThat(req.maxMessages()).isEqualTo(5);

            baos.reset();
            Message msg = Message.create("orders", "k1", "v1").withPartitionAndOffset(1, 10L);
            NetworkProtocol.writeFetchResponse(out, 201, ResponseStatus.OK, List.of(msg), null);

            NetworkProtocol.Frame respFrame = NetworkProtocol.readValidatedFrame(
                    toDataIn(baos), NetworkProtocol.DEFAULT_MAX_FRAME_SIZE);
            NetworkProtocol.FetchResponse resp = NetworkProtocol.decodeFetchResponse(respFrame, "orders", 1);

            assertThat(resp.status()).isEqualTo(ResponseStatus.OK);
            assertThat(resp.messages()).hasSize(1);
            assertThat(resp.messages().get(0).offset()).isEqualTo(10L);
            assertThat(resp.messages().get(0).valueAsString()).isEqualTo("v1");
        }
    }

    @Nested
    @DisplayName("Topic Management Codec Tests")
    class TopicCodecTests {

        @Test
        @DisplayName("Should encode and decode CREATE_TOPIC")
        void shouldCodecCreateTopic() throws IOException {
            ByteArrayOutputStream baos = new ByteArrayOutputStream();
            DataOutputStream out = new DataOutputStream(baos);

            NetworkProtocol.writeCreateTopicRequest(out, 301, "telemetry", 8);

            NetworkProtocol.Frame reqFrame = NetworkProtocol.readValidatedFrame(
                    toDataIn(baos), NetworkProtocol.DEFAULT_MAX_FRAME_SIZE);
            NetworkProtocol.CreateTopicRequest req = NetworkProtocol.decodeCreateTopicRequest(reqFrame);
            assertThat(req.topic()).isEqualTo("telemetry");
            assertThat(req.partitionCount()).isEqualTo(8);

            baos.reset();
            NetworkProtocol.writeCreateTopicResponse(out, 301, ResponseStatus.TOPIC_ALREADY_EXISTS, "Already exists");

            NetworkProtocol.Frame respFrame = NetworkProtocol.readValidatedFrame(
                    toDataIn(baos), NetworkProtocol.DEFAULT_MAX_FRAME_SIZE);
            NetworkProtocol.CreateTopicResponse resp = NetworkProtocol.decodeCreateTopicResponse(respFrame);
            assertThat(resp.status()).isEqualTo(ResponseStatus.TOPIC_ALREADY_EXISTS);
            assertThat(resp.error()).isEqualTo("Already exists");
        }

        @Test
        @DisplayName("Should encode and decode LIST_TOPICS")
        void shouldCodecListTopics() throws IOException {
            ByteArrayOutputStream baos = new ByteArrayOutputStream();
            DataOutputStream out = new DataOutputStream(baos);

            NetworkProtocol.writeListTopicsRequest(out, 401);

            NetworkProtocol.Frame reqFrame = NetworkProtocol.readValidatedFrame(
                    toDataIn(baos), NetworkProtocol.DEFAULT_MAX_FRAME_SIZE);
            assertThat(reqFrame.requestTypeCode()).isEqualTo(RequestType.LIST_TOPICS.getCode());
            assertThat(reqFrame.requestId()).isEqualTo(401);

            baos.reset();
            NetworkProtocol.writeListTopicsResponse(
                    out, 401, ResponseStatus.OK, Set.of("t1", "t2", "t3"), null);

            NetworkProtocol.Frame respFrame = NetworkProtocol.readValidatedFrame(
                    toDataIn(baos), NetworkProtocol.DEFAULT_MAX_FRAME_SIZE);
            NetworkProtocol.ListTopicsResponse resp = NetworkProtocol.decodeListTopicsResponse(respFrame);
            assertThat(resp.status()).isEqualTo(ResponseStatus.OK);
            assertThat(resp.topics()).containsExactlyInAnyOrder("t1", "t2", "t3");
        }
    }

    @Nested
    @DisplayName("Frame Bounds, Versioning & Malformed Payload Tests")
    class InvalidAndOversizedFrameTests {

        @Test
        @DisplayName("Should reject oversized incoming frame before allocating buffer")
        void shouldRejectOversizedIncomingFrame() throws IOException {
            ByteArrayOutputStream baos = new ByteArrayOutputStream();
            DataOutputStream out = new DataOutputStream(baos);
            // Claim frame size of 10 MB when max allowed is 1024 bytes
            out.writeInt(10 * 1024 * 1024);
            out.flush();

            assertThatThrownBy(() -> NetworkProtocol.readRawFrame(toDataIn(baos), 1024))
                    .isInstanceOf(ProtocolException.class)
                    .satisfies(e -> assertThat(((ProtocolException) e).getStatus())
                            .isEqualTo(ResponseStatus.FRAME_TOO_LARGE));
        }

        @Test
        @DisplayName("Should reject oversized outgoing produce payload before writing")
        void shouldRejectOversizedOutgoingPayload() {
            ByteArrayOutputStream baos = new ByteArrayOutputStream();
            DataOutputStream out = new DataOutputStream(baos);
            byte[] hugePayload = new byte[2048];

            assertThatThrownBy(() -> NetworkProtocol.writeProduceRequest(
                    out, 1, "orders", 0, "k", hugePayload, 512))
                    .isInstanceOf(ProtocolException.class)
                    .satisfies(e -> assertThat(((ProtocolException) e).getStatus())
                            .isEqualTo(ResponseStatus.FRAME_TOO_LARGE));
        }

        @Test
        @DisplayName("Should reject undersized frame smaller than header size")
        void shouldRejectUndersizedFrame() throws IOException {
            ByteArrayOutputStream baos = new ByteArrayOutputStream();
            DataOutputStream out = new DataOutputStream(baos);
            out.writeInt(3); // less than HEADER_SIZE (6)
            out.flush();

            assertThatThrownBy(() -> NetworkProtocol.readRawFrame(toDataIn(baos), 4096))
                    .isInstanceOf(ProtocolException.class)
                    .satisfies(e -> assertThat(((ProtocolException) e).getStatus())
                            .isEqualTo(ResponseStatus.BAD_REQUEST));
        }

        @Test
        @DisplayName("Should reject unsupported protocol version")
        void shouldRejectUnsupportedVersion() throws IOException {
            ByteArrayOutputStream baos = new ByteArrayOutputStream();
            DataOutputStream out = new DataOutputStream(baos);
            out.writeInt(NetworkProtocol.HEADER_SIZE);
            out.writeByte((byte) 99); // unsupported version
            out.writeByte(RequestType.PING.getCode());
            out.writeInt(55);
            out.flush();

            assertThatThrownBy(() -> NetworkProtocol.readValidatedFrame(toDataIn(baos), 4096))
                    .isInstanceOf(ProtocolException.class)
                    .satisfies(e -> assertThat(((ProtocolException) e).getStatus())
                            .isEqualTo(ResponseStatus.UNSUPPORTED_VERSION));
        }

        @Test
        @DisplayName("Should reject unknown request opcode")
        void shouldRejectUnknownOpcode() throws IOException {
            ByteArrayOutputStream baos = new ByteArrayOutputStream();
            DataOutputStream out = new DataOutputStream(baos);
            out.writeInt(NetworkProtocol.HEADER_SIZE);
            out.writeByte(NetworkProtocol.PROTOCOL_VERSION);
            out.writeByte((byte) 77); // unknown opcode
            out.writeInt(56);
            out.flush();

            assertThatThrownBy(() -> NetworkProtocol.readValidatedFrame(toDataIn(baos), 4096))
                    .isInstanceOf(ProtocolException.class)
                    .satisfies(e -> assertThat(((ProtocolException) e).getStatus())
                            .isEqualTo(ResponseStatus.UNKNOWN_REQUEST_TYPE));
        }

        @Test
        @DisplayName("Should reject malformed payload with bogus field length exceeding remaining frame bytes")
        void shouldRejectMalformedFieldLengthWithoutOom() throws IOException {
            // Build a small frame whose internal payload claims valLen = Integer.MAX_VALUE
            ByteArrayOutputStream payload = new ByteArrayOutputStream();
            DataOutputStream p = new DataOutputStream(payload);
            byte[] topic = "orders".getBytes(StandardCharsets.UTF_8);
            p.writeShort(topic.length);
            p.write(topic);
            p.writeInt(0); // partition
            p.writeInt(-1); // null key
            p.writeInt(Integer.MAX_VALUE); // malicious/malformed length!

            NetworkProtocol.Frame badFrame = new NetworkProtocol.Frame(
                    NetworkProtocol.PROTOCOL_VERSION,
                    RequestType.PRODUCE.getCode(),
                    777,
                    payload.toByteArray()
            );

            assertThatThrownBy(() -> NetworkProtocol.decodeProduceRequest(badFrame))
                    .isInstanceOf(ProtocolException.class)
                    .satisfies(e -> assertThat(((ProtocolException) e).getStatus())
                            .isEqualTo(ResponseStatus.BAD_REQUEST));
        }

        @Test
        @DisplayName("Should reject truncated payload inside frame")
        void shouldRejectTruncatedPayload() {
            NetworkProtocol.Frame truncatedFrame = new NetworkProtocol.Frame(
                    NetworkProtocol.PROTOCOL_VERSION,
                    RequestType.FETCH.getCode(),
                    888,
                    new byte[]{0x00, 0x05, 'a', 'b'} // claims 5-byte topic, only 2 bytes present
            );

            assertThatThrownBy(() -> NetworkProtocol.decodeFetchRequest(truncatedFrame))
                    .isInstanceOf(ProtocolException.class)
                    .satisfies(e -> assertThat(((ProtocolException) e).getStatus())
                            .isEqualTo(ResponseStatus.BAD_REQUEST));
        }

        @Test
        @DisplayName("Should reject frame with unexpected trailing bytes")
        void shouldRejectTrailingBytesInPayload() throws IOException {
            ByteArrayOutputStream payload = new ByteArrayOutputStream();
            DataOutputStream p = new DataOutputStream(payload);
            byte[] topic = "orders".getBytes(StandardCharsets.UTF_8);
            p.writeShort(topic.length);
            p.write(topic);
            p.writeInt(2);
            p.writeInt(999); // unexpected trailing garbage

            NetworkProtocol.Frame extraBytesFrame = new NetworkProtocol.Frame(
                    NetworkProtocol.PROTOCOL_VERSION,
                    RequestType.CREATE_TOPIC.getCode(),
                    889,
                    payload.toByteArray()
            );

            assertThatThrownBy(() -> NetworkProtocol.decodeCreateTopicRequest(extraBytesFrame))
                    .isInstanceOf(ProtocolException.class)
                    .satisfies(e -> assertThat(((ProtocolException) e).getStatus())
                            .isEqualTo(ResponseStatus.BAD_REQUEST));
        }
    }
}
