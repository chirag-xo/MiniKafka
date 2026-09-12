package com.minikafka.model;

import com.minikafka.exception.InvalidMessageException;

import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.Objects;
import java.util.UUID;

/**
 * Immutable data carrier representing an individual event record in MiniKafka.
 *
 * <p>Why a record?
 * In Java 21, records provide transparent, immutable data carriers.
 * To ensure absolute immutability when dealing with mutable {@code byte[]},
 * defensive copying is performed in both the constructor and the {@link #value()} accessor.
 */
public record Message(
        String id,
        String key,
        byte[] value,
        long timestamp,
        String topic,
        int partition,
        long offset
) {
    public static final long UNASSIGNED_OFFSET = -1L;
    public static final int UNASSIGNED_PARTITION = -1;

    /**
     * Compact Constructor:
     * Validates input invariants and performs defensive copying on mutable arrays.
     */
    public Message {
        if (id == null || id.isBlank()) {
            throw new InvalidMessageException("Message id cannot be null or blank");
        }
        if (topic == null || topic.isBlank()) {
            throw new InvalidMessageException("Topic cannot be null or blank");
        }
        if (value == null) {
            throw new InvalidMessageException("Message payload (value) cannot be null");
        }
        if (timestamp <= 0) {
            throw new InvalidMessageException("Timestamp must be positive epoch milliseconds");
        }

        // Defensive copy to prevent caller from mutating the array after creation
        value = value.clone();
    }

    /**
     * Factory method for creating an unassigned message prior to partition routing and offset assignment.
     */
    public static Message create(String topic, String key, byte[] value) {
        return new Message(
                UUID.randomUUID().toString(),
                key,
                value,
                System.currentTimeMillis(),
                topic,
                UNASSIGNED_PARTITION,
                UNASSIGNED_OFFSET
        );
    }

    /**
     * Factory method for creating an unassigned message with a string payload.
     */
    public static Message create(String topic, String key, String value) {
        byte[] bytes = (value == null) ? new byte[0] : value.getBytes(StandardCharsets.UTF_8);
        return create(topic, key, bytes);
    }

    /**
     * Returns a new Message instance with assigned partition and offset.
     * Preserves message immutability.
     */
    public Message withPartitionAndOffset(int assignedPartition, long assignedOffset) {
        if (assignedPartition < 0) {
            throw new InvalidMessageException("Partition must be non-negative: " + assignedPartition);
        }
        if (assignedOffset < 0) {
            throw new InvalidMessageException("Offset must be non-negative: " + assignedOffset);
        }
        return new Message(
                this.id,
                this.key,
                this.value, // will be cloned by constructor
                this.timestamp,
                this.topic,
                assignedPartition,
                assignedOffset
        );
    }

    /**
     * Defensive copy accessor for byte payload to prevent external mutation.
     */
    @Override
    public byte[] value() {
        return value.clone();
    }

    /**
     * Convenience helper to read the payload as UTF-8 string.
     */
    public String valueAsString() {
        return new String(value, StandardCharsets.UTF_8);
    }

    /**
     * Custom equals to guarantee content-based equality for the byte array.
     * Default record implementation uses reference equality (==) for arrays.
     */
    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof Message that)) return false;
        return timestamp == that.timestamp &&
                partition == that.partition &&
                offset == that.offset &&
                Objects.equals(id, that.id) &&
                Objects.equals(key, that.key) &&
                Arrays.equals(value, that.value) &&
                Objects.equals(topic, that.topic);
    }

    /**
     * Custom hashCode using Arrays.hashCode for the byte array.
     */
    @Override
    public int hashCode() {
        int result = Objects.hash(id, key, timestamp, topic, partition, offset);
        result = 31 * result + Arrays.hashCode(value);
        return result;
    }

    @Override
    public String toString() {
        return "Message{" +
                "id='" + id + '\'' +
                ", key='" + key + '\'' +
                ", topic='" + topic + '\'' +
                ", partition=" + partition +
                ", offset=" + offset +
                ", timestamp=" + timestamp +
                ", valueBytes=" + value.length +
                '}';
    }
}
