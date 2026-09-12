package com.minikafka.storage;

import com.minikafka.exception.CorruptRecordException;
import com.minikafka.model.Message;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

@DisplayName("RecordSerializer Binary Protocol Tests")
class RecordSerializerTest {

    @Test
    @DisplayName("Should successfully serialize and deserialize a message with key")
    void shouldSerializeAndDeserializeWithKey() {
        Message original = new Message(
                UUID.randomUUID().toString(),
                "customer-99",
                "payment-data".getBytes(StandardCharsets.UTF_8),
                1700000000000L,
                "payments",
                0,
                42L
        );

        ByteBuffer serialized = RecordSerializer.serialize(original);
        assertThat(serialized.remaining()).isPositive();

        Message deserialized = RecordSerializer.deserialize(serialized, "payments", 0);

        assertThat(deserialized.id()).isEqualTo(original.id());
        assertThat(deserialized.key()).isEqualTo("customer-99");
        assertThat(deserialized.valueAsString()).isEqualTo("payment-data");
        assertThat(deserialized.timestamp()).isEqualTo(original.timestamp());
        assertThat(deserialized.topic()).isEqualTo("payments");
        assertThat(deserialized.partition()).isEqualTo(0);
        assertThat(deserialized.offset()).isEqualTo(42L);
        assertThat(deserialized).isEqualTo(original);
    }

    @Test
    @DisplayName("Should successfully serialize and deserialize a keyless message")
    void shouldSerializeAndDeserializeKeylessMessage() {
        Message original = new Message(
                UUID.randomUUID().toString(),
                null,
                "raw-telemetry".getBytes(StandardCharsets.UTF_8),
                1700000000000L,
                "telemetry",
                1,
                100L
        );

        ByteBuffer serialized = RecordSerializer.serialize(original);
        Message deserialized = RecordSerializer.deserialize(serialized, "telemetry", 1);

        assertThat(deserialized.key()).isNull();
        assertThat(deserialized.valueAsString()).isEqualTo("raw-telemetry");
        assertThat(deserialized).isEqualTo(original);
    }

    @Test
    @DisplayName("Should detect bit-rot or payload corruption via CRC32 checksum mismatch")
    void shouldDetectCrc32Corruption() {
        Message original = Message.create("orders", "k", "valid-payload").withPartitionAndOffset(0, 0L);
        ByteBuffer serialized = RecordSerializer.serialize(original);

        // Corrupt a byte in the payload
        byte[] array = serialized.array();
        array[array.length - 1] ^= 0xFF; // flip bits in the last payload byte

        serialized.position(0);
        assertThatThrownBy(() -> RecordSerializer.deserialize(serialized, "orders", 0))
                .isInstanceOf(CorruptRecordException.class)
                .hasMessageContaining("CRC32 mismatch");
    }

    @Test
    @DisplayName("Should reject record with invalid magic byte")
    void shouldRejectInvalidMagicByte() {
        Message original = Message.create("orders", "k", "payload").withPartitionAndOffset(0, 0L);
        ByteBuffer serialized = RecordSerializer.serialize(original);

        // Magic byte is at offset 4 (after 4-byte record_length)
        byte[] array = serialized.array();
        array[4] = (byte) 0x00; // corrupt magic byte

        serialized.position(0);
        assertThatThrownBy(() -> RecordSerializer.deserialize(serialized, "orders", 0))
                .isInstanceOf(CorruptRecordException.class)
                .hasMessageContaining("Magic byte mismatch");
    }
}
