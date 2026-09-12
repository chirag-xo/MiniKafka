package com.minikafka.producer;

/**
 * Metadata acknowledged by the broker when a record has been successfully appended to a partition log.
 *
 * @param topic The topic to which the record was appended.
 * @param partition The specific partition index that stored the record.
 * @param offset The unique monotonic offset assigned within that partition.
 * @param timestamp The record timestamp.
 */
public record RecordMetadata(
        String topic,
        int partition,
        long offset,
        long timestamp
) {
    public RecordMetadata {
        if (topic == null || topic.isBlank()) {
            throw new IllegalArgumentException("Topic cannot be null or blank");
        }
        if (partition < 0) {
            throw new IllegalArgumentException("Partition must be non-negative: " + partition);
        }
        if (offset < 0) {
            throw new IllegalArgumentException("Offset must be non-negative: " + offset);
        }
    }
}
