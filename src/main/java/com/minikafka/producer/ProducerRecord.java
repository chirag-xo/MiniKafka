package com.minikafka.producer;

import com.minikafka.exception.InvalidMessageException;

import java.nio.charset.StandardCharsets;

/**
 * The record published by client applications to a MiniKafka topic.
 *
 * <p>Contains:
 * <ul>
 *   <li>{@code topic}: Target topic name (required).</li>
 *   <li>{@code key}: Optional routing key for partitioning (nullable).</li>
 *   <li>{@code value}: Payload bytes (defensively copied).</li>
 *   <li>{@code timestamp}: Creation timestamp in epoch millis.</li>
 * </ul>
 */
public record ProducerRecord(
        String topic,
        String key,
        byte[] value,
        long timestamp
) {
    public ProducerRecord {
        if (topic == null || topic.isBlank()) {
            throw new InvalidMessageException("Topic cannot be null or blank");
        }
        if (value == null) {
            throw new InvalidMessageException("Payload (value) cannot be null");
        }
        if (timestamp <= 0) {
            throw new InvalidMessageException("Timestamp must be positive");
        }

        value = value.clone();
    }

    public static ProducerRecord of(String topic, String key, byte[] value) {
        return new ProducerRecord(topic, key, value, System.currentTimeMillis());
    }

    public static ProducerRecord of(String topic, String key, String value) {
        byte[] bytes = (value == null) ? new byte[0] : value.getBytes(StandardCharsets.UTF_8);
        return of(topic, key, bytes);
    }

    public static ProducerRecord of(String topic, String value) {
        return of(topic, null, value);
    }

    public static ProducerRecord of(String topic, byte[] value) {
        return of(topic, null, value);
    }

    @Override
    public byte[] value() {
        return value.clone();
    }

    public String valueAsString() {
        return new String(value, StandardCharsets.UTF_8);
    }
}
