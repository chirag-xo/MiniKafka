package com.minikafka.exception;

/**
 * Thrown when an operation references a partition index that does not exist in the topic.
 */
public class PartitionNotFoundException extends MiniKafkaException {
    private final String topic;
    private final int partition;
    private final int availablePartitions;

    public PartitionNotFoundException(String topic, int partition, int availablePartitions) {
        super(String.format("Partition %d not found in topic '%s'. Available partitions: [0, %d)",
                partition, topic, availablePartitions));
        this.topic = topic;
        this.partition = partition;
        this.availablePartitions = availablePartitions;
    }

    public String getTopic() {
        return topic;
    }

    public int getPartition() {
        return partition;
    }

    public int getAvailablePartitions() {
        return availablePartitions;
    }
}
