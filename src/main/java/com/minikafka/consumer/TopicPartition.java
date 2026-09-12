package com.minikafka.consumer;

import java.util.Objects;

/**
 * Composite identifier representing a specific partition of a topic.
 *
 * @param topic The topic name.
 * @param partition The zero-based partition index.
 */
public record TopicPartition(String topic, int partition) {
    public TopicPartition {
        if (topic == null || topic.isBlank()) {
            throw new IllegalArgumentException("Topic cannot be null or blank");
        }
        if (partition < 0) {
            throw new IllegalArgumentException("Partition index must be non-negative: " + partition);
        }
    }

    @Override
    public String toString() {
        return topic + "-" + partition;
    }
}
