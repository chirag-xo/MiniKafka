package com.minikafka.broker;

import com.minikafka.model.Message;
import com.minikafka.topic.Topic;

import java.util.List;
import java.util.Set;

/**
 * Primary message broker interface defining topic lifecycle and message routing.
 */
public interface Broker {

    /**
     * Creates a new topic with the specified partition count.
     *
     * @param topicName The unique name of the topic.
     * @param partitionCount Number of partitions ($partitionCount \ge 1$).
     * @throws com.minikafka.exception.TopicAlreadyExistsException if topic already exists.
     * @throws IllegalArgumentException if arguments are invalid.
     */
    void createTopic(String topicName, int partitionCount);

    /**
     * Checks if a topic exists on the broker.
     *
     * @param topicName The topic name.
     * @return true if topic exists, false otherwise.
     */
    boolean topicExists(String topicName);

    /**
     * Retrieves the topic instance.
     *
     * @param topicName The topic name.
     * @return The {@link Topic}.
     * @throws com.minikafka.exception.TopicNotFoundException if the topic does not exist.
     */
    Topic getTopic(String topicName);

    /**
     * @return An unmodifiable set of all registered topic names.
     */
    Set<String> listTopics();

    /**
     * Appends a message to a specific topic partition.
     *
     * @param topicName The topic name.
     * @param partitionId The partition index.
     * @param message The message to append.
     * @return The assigned monotonic offset in the partition.
     * @throws com.minikafka.exception.TopicNotFoundException if topic does not exist.
     * @throws com.minikafka.exception.PartitionNotFoundException if partition does not exist.
     */
    long append(String topicName, int partitionId, Message message);

    /**
     * Reads a batch of messages from a specific topic partition.
     *
     * @param topicName The topic name.
     * @param partitionId The partition index.
     * @param fromOffset The starting offset (inclusive).
     * @param maxMessages Maximum messages to return.
     * @return Unmodifiable list of messages.
     * @throws com.minikafka.exception.TopicNotFoundException if topic does not exist.
     * @throws com.minikafka.exception.PartitionNotFoundException if partition does not exist.
     * @throws com.minikafka.exception.OffsetOutOfRangeException if offset is out of range.
     */
    List<Message> read(String topicName, int partitionId, long fromOffset, int maxMessages);

    /**
     * Deletes a topic from the broker.
     *
     * @param topicName The topic name.
     * @throws com.minikafka.exception.TopicNotFoundException if topic does not exist.
     */
    void deleteTopic(String topicName);

    /**
     * @return The offset manager coordinating committed consumer offsets.
     */
    com.minikafka.offset.OffsetManager getOffsetManager();

    /**
     * @return The coordinator managing consumer groups and partition rebalancing.
     */
    com.minikafka.group.ConsumerGroupCoordinator getGroupCoordinator();
}
