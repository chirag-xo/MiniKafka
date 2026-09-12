package com.minikafka.consumer;

import java.time.Duration;
import java.util.Collection;
import java.util.Set;

/**
 * Client consumer interface for subscribing to MiniKafka topics and polling records.
 */
public interface Consumer extends AutoCloseable {

    /**
     * Manually assigns a set of partitions to this consumer.
     *
     * @param partitions The partitions to assign.
     */
    void assign(Collection<TopicPartition> partitions);

    /**
     * Subscribes to the given topics, assigning all their partitions to this consumer.
     *
     * @param topics Collection of topic names.
     */
    void subscribe(Collection<String> topics);

    /**
     * @return An unmodifiable set of currently assigned topic-partitions.
     */
    Set<TopicPartition> assignment();

    /**
     * Polls the broker for records across all assigned partitions.
     *
     * @param timeout The maximum duration to wait if no records are immediately available.
     * @return A {@link ConsumerRecords} batch containing fetched records.
     */
    ConsumerRecords poll(Duration timeout);

    /**
     * @param partition The partition to query.
     * @return The offset of the next record that will be fetched.
     */
    long position(TopicPartition partition);

    /**
     * Commits the current fetch positions for all assigned partitions to the broker.
     * Requires the consumer to be configured with a groupId.
     *
     * @throws IllegalStateException if the consumer has no groupId configured.
     */
    void commitSync();

    /**
     * Commits the specified offsets for topic partitions to the broker.
     *
     * @param offsets Map of topic-partitions to offsets to commit.
     * @throws IllegalStateException if the consumer has no groupId configured.
     */
    void commitSync(java.util.Map<TopicPartition, Long> offsets);

    /**
     * Retrieves the last committed offset for the given partition.
     *
     * @param partition The partition to query.
     * @return Optional containing the committed offset, or empty if never committed.
     */
    java.util.Optional<Long> committed(TopicPartition partition);

    /**
     * Overrides the fetch offset that the consumer will use on the next {@link #poll}.
     *
     * @param partition The target partition.
     * @param offset The new fetch offset.
     */
    void seek(TopicPartition partition, long offset);

    /**
     * Resets the fetch offsets of the given partitions to the earliest available offset.
     *
     * @param partitions Partitions to reset.
     */
    void seekToBeginning(Collection<TopicPartition> partitions);

    /**
     * Resets the fetch offsets of the given partitions to the end of the log (latest).
     *
     * @param partitions Partitions to reset.
     */
    void seekToEnd(Collection<TopicPartition> partitions);

    @Override
    void close();
}
