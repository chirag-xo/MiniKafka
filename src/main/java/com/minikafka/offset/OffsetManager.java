package com.minikafka.offset;

import com.minikafka.consumer.TopicPartition;

import java.util.Map;
import java.util.Optional;

/**
 * Manages committed consumer offsets partitioned by consumer group and topic-partition.
 */
public interface OffsetManager {

    /**
     * Records a committed offset for a given consumer group and partition.
     *
     * @param groupId The consumer group identifier.
     * @param partition The specific topic partition.
     * @param offsetAndMetadata The offset and commit metadata.
     */
    void commitOffset(String groupId, TopicPartition partition, OffsetAndMetadata offsetAndMetadata);

    /**
     * Retrieves the last committed offset for a specific consumer group and partition.
     *
     * @param groupId The consumer group identifier.
     * @param partition The specific topic partition.
     * @return Optional containing the committed offset, or empty if never committed.
     */
    Optional<OffsetAndMetadata> getCommittedOffset(String groupId, TopicPartition partition);

    /**
     * Retrieves all committed offsets for a given consumer group.
     *
     * @param groupId The consumer group identifier.
     * @return An unmodifiable map of topic partitions to committed offsets.
     */
    Map<TopicPartition, OffsetAndMetadata> getAllCommittedOffsets(String groupId);

    /**
     * Resets/removes all committed offsets for a consumer group on a specific topic.
     *
     * @param groupId The consumer group identifier.
     * @param topic The topic name.
     */
    void resetOffsets(String groupId, String topic);
}
