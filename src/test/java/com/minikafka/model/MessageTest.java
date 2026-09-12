package com.minikafka.model;

import com.minikafka.exception.InvalidMessageException;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

@DisplayName("Message Model Tests")
class MessageTest {

    @Nested
    @DisplayName("Creation & Immutability")
    class CreationTests {

        @Test
        @DisplayName("Should create message successfully with valid parameters")
        void shouldCreateMessageSuccessfully() {
            String topic = "orders";
            String key = "order-123";
            String payload = "{\"item\": \"laptop\"}";

            Message message = Message.create(topic, key, payload);

            assertThat(message.id()).isNotBlank();
            assertThat(message.topic()).isEqualTo(topic);
            assertThat(message.key()).isEqualTo(key);
            assertThat(message.valueAsString()).isEqualTo(payload);
            assertThat(message.partition()).isEqualTo(Message.UNASSIGNED_PARTITION);
            assertThat(message.offset()).isEqualTo(Message.UNASSIGNED_OFFSET);
            assertThat(message.timestamp()).isPositive();
        }

        @Test
        @DisplayName("Should defensively copy byte array on construction to prevent external mutation")
        void shouldDefensivelyCopyPayloadOnConstruction() {
            byte[] original = "original data".getBytes(StandardCharsets.UTF_8);
            Message message = Message.create("telemetry", "dev-1", original);

            // Mutate the original external array
            original[0] = 'X';

            assertThat(message.valueAsString())
                    .as("Mutating the external input array must not affect the internal message payload")
                    .isEqualTo("original data");
        }

        @Test
        @DisplayName("Should defensively copy byte array on value() accessor to prevent external mutation")
        void shouldDefensivelyCopyPayloadOnAccessor() {
            Message message = Message.create("telemetry", "dev-1", "secure data");

            byte[] retrieved = message.value();
            retrieved[0] = 'Z';

            assertThat(message.valueAsString())
                    .as("Mutating the array returned by value() must not affect internal state")
                    .isEqualTo("secure data");
        }
    }

    @Nested
    @DisplayName("Validation Constraints")
    class ValidationTests {

        @Test
        @DisplayName("Should throw exception when ID is null or blank")
        void shouldRejectNullOrBlankId() {
            assertThatThrownBy(() -> new Message(null, "k", new byte[0], 1000L, "topic", 0, 0))
                    .isInstanceOf(InvalidMessageException.class)
                    .hasMessageContaining("id cannot be null or blank");

            assertThatThrownBy(() -> new Message("  ", "k", new byte[0], 1000L, "topic", 0, 0))
                    .isInstanceOf(InvalidMessageException.class)
                    .hasMessageContaining("id cannot be null or blank");
        }

        @Test
        @DisplayName("Should throw exception when topic is null or blank")
        void shouldRejectNullOrBlankTopic() {
            assertThatThrownBy(() -> new Message(UUID.randomUUID().toString(), "k", new byte[0], 1000L, null, 0, 0))
                    .isInstanceOf(InvalidMessageException.class)
                    .hasMessageContaining("Topic cannot be null or blank");
        }

        @Test
        @DisplayName("Should throw exception when value is null")
        void shouldRejectNullValue() {
            assertThatThrownBy(() -> new Message(UUID.randomUUID().toString(), "k", null, 1000L, "topic", 0, 0))
                    .isInstanceOf(InvalidMessageException.class)
                    .hasMessageContaining("payload (value) cannot be null");
        }

        @Test
        @DisplayName("Should throw exception when timestamp is non-positive")
        void shouldRejectNonPositiveTimestamp() {
            assertThatThrownBy(() -> new Message(UUID.randomUUID().toString(), "k", new byte[0], 0L, "topic", 0, 0))
                    .isInstanceOf(InvalidMessageException.class)
                    .hasMessageContaining("Timestamp must be positive");
        }
    }

    @Nested
    @DisplayName("Offset & Equality Semantics")
    class EqualityAndOffsetTests {

        @Test
        @DisplayName("Should create new message instance when assigning partition and offset")
        void shouldReturnNewInstanceWithPartitionAndOffset() {
            Message initial = Message.create("orders", "key-1", "value-1");

            Message assigned = initial.withPartitionAndOffset(2, 42L);

            assertThat(assigned).isNotSameAs(initial);
            assertThat(assigned.id()).isEqualTo(initial.id());
            assertThat(assigned.partition()).isEqualTo(2);
            assertThat(assigned.offset()).isEqualTo(42L);
            assertThat(assigned.valueAsString()).isEqualTo("value-1");
        }

        @Test
        @DisplayName("Should evaluate equality by content rather than array memory address")
        void shouldEvaluateEqualityByContent() {
            String id = UUID.randomUUID().toString();
            long ts = 1700000000000L;
            byte[] bytes1 = new byte[]{1, 2, 3};
            byte[] bytes2 = new byte[]{1, 2, 3};

            Message msg1 = new Message(id, "k", bytes1, ts, "topic", 0, 10L);
            Message msg2 = new Message(id, "k", bytes2, ts, "topic", 0, 10L);

            assertThat(msg1).isEqualTo(msg2);
            assertThat(msg1.hashCode()).isEqualTo(msg2.hashCode());
        }
    }
}
