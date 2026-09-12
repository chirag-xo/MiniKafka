package com.minikafka.producer;

/**
 * Strategy interface defining how records are mapped to topic partitions.
 */
public interface Partitioner {

    /**
     * Computes the partition index for a given record.
     *
     * @param topic Target topic name.
     * @param key Message routing key (nullable).
     * @param value Message payload.
     * @param numPartitions Total number of available partitions ($numPartitions \ge 1$).
     * @return Zero-based partition index ($0 \le return < numPartitions$).
     */
    int partition(String topic, String key, byte[] value, int numPartitions);
}
