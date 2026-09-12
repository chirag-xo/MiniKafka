package com.minikafka.consumer;

import com.minikafka.model.Message;

import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.Objects;

/**
 * A key/value pair retrieved from MiniKafka, containing the assigned partition and offset.
 */
public record ConsumerRecord(
        String topic,
        int partition,
        long offset,
        String key,
        byte[] value,
        long timestamp
) {
    public ConsumerRecord {
        Objects.requireNonNull(topic, "Topic cannot be null");
        Objects.requireNonNull(value, "Value cannot be null");
        value = value.clone();
    }

    public static ConsumerRecord from(Message message) {
        return new ConsumerRecord(
                message.topic(),
                message.partition(),
                message.offset(),
                message.key(),
                message.value(),
                message.timestamp()
        );
    }

    public TopicPartition topicPartition() {
        return new TopicPartition(topic, partition);
    }

    @Override
    public byte[] value() {
        return value.clone();
    }

    public String valueAsString() {
        return new String(value, StandardCharsets.UTF_8);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof ConsumerRecord that)) return false;
        return partition == that.partition &&
                offset == that.offset &&
                timestamp == that.timestamp &&
                Objects.equals(topic, that.topic) &&
                Objects.equals(key, that.key) &&
                Arrays.equals(value, that.value);
    }

    @Override
    public int hashCode() {
        int result = Objects.hash(topic, partition, offset, key, timestamp);
        result = 31 * result + Arrays.hashCode(value);
        return result;
    }
}
