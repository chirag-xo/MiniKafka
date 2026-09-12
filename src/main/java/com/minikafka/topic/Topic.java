package com.minikafka.topic;

import com.minikafka.exception.PartitionNotFoundException;
import com.minikafka.model.Message;
import com.minikafka.partition.InMemoryPartition;
import com.minikafka.partition.Partition;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 * A Topic represents a named stream of records, horizontally sharded into one or more {@link Partition}s.
 *
 * <p>Key Architecture Principles:
 * <ul>
 *   <li>Partitions are indexed 0 to N-1.</li>
 *   <li>Each partition maintains its own independent offset space and lock.</li>
 *   <li>Writes and reads to different partitions run completely concurrently without mutual contention.</li>
 * </ul>
 */
public class Topic {

    private final String name;
    private final List<Partition> partitions;

    public Topic(String name, int partitionCount) {
        this(name, partitionCount, null);
    }

    public Topic(String name, int partitionCount, java.nio.file.Path storageDir) {
        if (name == null || name.isBlank()) {
            throw new IllegalArgumentException("Topic name cannot be null or blank");
        }
        if (partitionCount < 1) {
            throw new IllegalArgumentException("Topic must have at least 1 partition, requested: " + partitionCount);
        }

        this.name = name;
        List<Partition> partitionList = new ArrayList<>(partitionCount);
        for (int i = 0; i < partitionCount; i++) {
            if (storageDir != null) {
                partitionList.add(new com.minikafka.storage.PersistentPartition(name, i, storageDir));
            } else {
                partitionList.add(new InMemoryPartition(name, i));
            }
        }
        this.partitions = Collections.unmodifiableList(partitionList);
    }

    public String getName() {
        return name;
    }

    public int getPartitionCount() {
        return partitions.size();
    }

    /**
     * Retrieves the partition corresponding to the specified ID.
     *
     * @param partitionId 0-based partition index.
     * @return The {@link Partition} instance.
     * @throws PartitionNotFoundException if {@code partitionId} is out of range.
     */
    public Partition getPartition(int partitionId) {
        if (partitionId < 0 || partitionId >= partitions.size()) {
            throw new PartitionNotFoundException(name, partitionId, partitions.size());
        }
        return partitions.get(partitionId);
    }

    /**
     * @return An unmodifiable view of all partitions in this topic.
     */
    public List<Partition> getPartitions() {
        return partitions;
    }

    /**
     * Appends a message to a specific partition in this topic.
     *
     * @param partitionId The target partition.
     * @param message The message to append.
     * @return The assigned monotonic offset in that partition.
     */
    public long append(int partitionId, Message message) {
        return getPartition(partitionId).append(message);
    }

    /**
     * Reads messages sequentially from a specific partition.
     *
     * @param partitionId The target partition.
     * @param fromOffset The starting offset (inclusive).
     * @param maxMessages Maximum messages to return.
     * @return Unmodifiable list of messages.
     */
    public List<Message> read(int partitionId, long fromOffset, int maxMessages) {
        return getPartition(partitionId).read(fromOffset, maxMessages);
    }

    @Override
    public String toString() {
        return "Topic{" +
                "name='" + name + '\'' +
                ", partitions=" + partitions.size() +
                '}';
    }
}
