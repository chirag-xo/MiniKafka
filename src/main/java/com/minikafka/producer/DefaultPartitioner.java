package com.minikafka.producer;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Deterministic key-affinity partitioning strategy inspired by Kafka's partition routing concept:
 * <ul>
 *   <li>If a key is present: Hash the key deterministically using Java's 32-bit hash with sign-bit masking
 *       so all messages with the same key land on the exact same partition. Note: This is an educational
 *       deterministic key-affinity policy, distinct from Apache Kafka's production MurmurHash2 algorithm.</li>
 *   <li>If no key is present: Distribute messages uniformly across all partitions in round-robin order.</li>
 * </ul>
 */
public class DefaultPartitioner implements Partitioner {

    private final ConcurrentMap<String, AtomicInteger> topicCounters = new ConcurrentHashMap<>();

    @Override
    public int partition(String topic, String key, byte[] value, int numPartitions) {
        if (numPartitions <= 0) {
            throw new IllegalArgumentException("numPartitions must be greater than zero");
        }

        if (key != null) {
            // Bitwise AND with 0x7fffffff clears the sign bit to avoid negative indices
            return (key.hashCode() & 0x7fffffff) % numPartitions;
        }

        // Key is null: round-robin selection per topic
        AtomicInteger counter = topicCounters.computeIfAbsent(topic, t -> new AtomicInteger(0));
        int nextVal = counter.getAndIncrement();
        return (nextVal & 0x7fffffff) % numPartitions;
    }
}
