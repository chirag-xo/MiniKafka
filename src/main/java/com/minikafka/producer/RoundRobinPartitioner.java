package com.minikafka.producer;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Pure round-robin partitioner distributing all records uniformly across partitions regardless of key.
 */
public class RoundRobinPartitioner implements Partitioner {

    private final ConcurrentMap<String, AtomicInteger> topicCounters = new ConcurrentHashMap<>();

    @Override
    public int partition(String topic, String key, byte[] value, int numPartitions) {
        if (numPartitions <= 0) {
            throw new IllegalArgumentException("numPartitions must be greater than zero");
        }
        AtomicInteger counter = topicCounters.computeIfAbsent(topic, t -> new AtomicInteger(0));
        return (counter.getAndIncrement() & 0x7fffffff) % numPartitions;
    }
}
