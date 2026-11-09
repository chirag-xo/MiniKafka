package com.minikafka.network;

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

@DisplayName("NetworkProtocol Frame & Codec Tests")
class NetworkProtocolTest {

    @Nested
    @DisplayName("Produce Codec Tests")
    class ProduceCodecTests {

        @Test
        @DisplayName("Should encode and decode PRODUCE request with key and value")
        void shouldCodecProduceRequest() throws IOException {
            ByteArrayOutputStream baos = new ByteArrayOutputStream();
            DataOutputStream out = new DataOutputStream(baos);

            NetworkProtocol.writeProduceRequest(out, 101, "orders", 2, "key1", "order-payload".getBytes(StandardCharsets.UTF_8));

            DataInputStream in = new DataInputStream(new ByteArrayInputStream(baos.toByteArray()));
            int frameLen = in.readInt();
            byte type = in.readByte();
            int corrId = in.readInt();

            assertThat(type).isEqualTo(RequestType.PRODUCE.getCode());
            assertThat(corrId).isEqualTo(101);

            NetworkProtocol.ProduceRequest req = NetworkProtocol.readProduceRequest(in, corrId);
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

            NetworkProtocol.writeProduceRequest(out, 102, "events", 0, null, "data".getBytes(StandardCharsets.UTF_8));

            DataInputStream in = new DataInputStream(new ByteArrayInputStream(baos.toByteArray()));
            in.readInt();
            in.readByte();
            int corrId = in.readInt();

            NetworkProtocol.ProduceRequest req = NetworkProtocol.readProduceRequest(in, corrId);
            assertThat(req.key()).isNull();
            assertThat(req.topic()).isEqualTo("events");
        }

        @Test
        @DisplayName("Should encode and decode PRODUCE response")
        void shouldCodecProduceResponse() throws IOException {
            ByteArrayOutputStream baos = new ByteArrayOutputStream();
            DataOutputStream out = new DataOutputStream(baos);

            NetworkProtocol.writeProduceResponse(out, 103, NetworkProtocol.STATUS_OK, 42L, null);

            DataInputStream in = new DataInputStream(new ByteArrayInputStream(baos.toByteArray()));
            in.readInt();
            in.readByte();
            int corrId = in.readInt();

            NetworkProtocol.ProduceResponse resp = NetworkProtocol.readProduceResponse(in, corrId);
            assertThat(resp.correlationId()).isEqualTo(103);
            assertThat(resp.status()).isEqualTo(NetworkProtocol.STATUS_OK);
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

            DataInputStream in = new DataInputStream(new ByteArrayInputStream(baos.toByteArray()));
            in.readInt();
            in.readByte();
            int corrId = in.readInt();

            NetworkProtocol.FetchRequest req = NetworkProtocol.readFetchRequest(in, corrId);
            assertThat(req.topic()).isEqualTo("orders");
            assertThat(req.partition()).isEqualTo(1);
            assertThat(req.fromOffset()).isEqualTo(10L);
            assertThat(req.maxMessages()).isEqualTo(5);

            // Response with messages
            baos.reset();
            Message msg = Message.create("orders", "k1", "v1").withPartitionAndOffset(1, 10L);
            NetworkProtocol.writeFetchResponse(out, 201, NetworkProtocol.STATUS_OK, List.of(msg), null);

            in = new DataInputStream(new ByteArrayInputStream(baos.toByteArray()));
            in.readInt();
            in.readByte();
            corrId = in.readInt();

            NetworkProtocol.FetchResponse resp = NetworkProtocol.readFetchResponse(in, corrId, "orders", 1);
            assertThat(resp.status()).isEqualTo(NetworkProtocol.STATUS_OK);
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

            DataInputStream in = new DataInputStream(new ByteArrayInputStream(baos.toByteArray()));
            in.readInt();
            in.readByte();
            int corrId = in.readInt();

            NetworkProtocol.CreateTopicRequest req = NetworkProtocol.readCreateTopicRequest(in, corrId);
            assertThat(req.topic()).isEqualTo("telemetry");
            assertThat(req.partitionCount()).isEqualTo(8);

            baos.reset();
            NetworkProtocol.writeCreateTopicResponse(out, 301, NetworkProtocol.STATUS_OK, null);

            in = new DataInputStream(new ByteArrayInputStream(baos.toByteArray()));
            in.readInt();
            in.readByte();
            corrId = in.readInt();

            NetworkProtocol.CreateTopicResponse resp = NetworkProtocol.readCreateTopicResponse(in, corrId);
            assertThat(resp.status()).isEqualTo(NetworkProtocol.STATUS_OK);
            assertThat(resp.error()).isNull();
        }

        @Test
        @DisplayName("Should encode and decode LIST_TOPICS")
        void shouldCodecListTopics() throws IOException {
            ByteArrayOutputStream baos = new ByteArrayOutputStream();
            DataOutputStream out = new DataOutputStream(baos);

            NetworkProtocol.writeListTopicsRequest(out, 401);

            DataInputStream in = new DataInputStream(new ByteArrayInputStream(baos.toByteArray()));
            in.readInt();
            byte type = in.readByte();
            int corrId = in.readInt();
            assertThat(type).isEqualTo(RequestType.LIST_TOPICS.getCode());

            baos.reset();
            NetworkProtocol.writeListTopicsResponse(out, 401, NetworkProtocol.STATUS_OK, Set.of("t1", "t2", "t3"), null);

            in = new DataInputStream(new ByteArrayInputStream(baos.toByteArray()));
            in.readInt();
            in.readByte();
            corrId = in.readInt();

            NetworkProtocol.ListTopicsResponse resp = NetworkProtocol.readListTopicsResponse(in, corrId);
            assertThat(resp.status()).isEqualTo(NetworkProtocol.STATUS_OK);
            assertThat(resp.topics()).containsExactlyInAnyOrder("t1", "t2", "t3");
        }
    }
}

