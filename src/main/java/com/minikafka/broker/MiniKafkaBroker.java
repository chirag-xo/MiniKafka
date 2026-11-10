package com.minikafka.broker;

import com.minikafka.exception.TopicAlreadyExistsException;
import com.minikafka.exception.TopicNotFoundException;
import com.minikafka.model.Message;
import com.minikafka.topic.Topic;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

/**
 * In-memory single-node implementation of the {@link Broker}.
 *
 * <p>Concurrency Architecture:
 * <ul>
 *   <li>Topic Registry: Backed by {@link ConcurrentHashMap} for lock-free reads and atomic topic registration.</li>
 *   <li>Partition Isolation: Appends and reads to distinct partitions/topics never block each other.</li>
 * </ul>
 */
public class MiniKafkaBroker implements Broker {

    private final Map<String, Topic> topics = new ConcurrentHashMap<>();
    private final com.minikafka.offset.OffsetManager offsetManager;
    private final com.minikafka.group.ConsumerGroupCoordinator groupCoordinator = new com.minikafka.group.InMemoryConsumerGroupCoordinator(this);
    private final com.minikafka.metrics.BrokerMetrics metrics = new com.minikafka.metrics.BrokerMetrics();
    private final java.nio.file.Path dataDir;

    public MiniKafkaBroker() {
        this(null);
    }

    public MiniKafkaBroker(java.nio.file.Path dataDir) {
        this.dataDir = dataDir;
        this.offsetManager = (dataDir != null)
                ? new com.minikafka.offset.PersistentOffsetManager(dataDir)
                : new com.minikafka.offset.InMemoryOffsetManager();
        if (dataDir != null && java.nio.file.Files.exists(dataDir)) {
            recoverTopicsFromDisk();
        }
    }

    private void recoverTopicsFromDisk() {
        try (var stream = java.nio.file.Files.list(dataDir)) {
            stream.filter(java.nio.file.Files::isDirectory)
                    .filter(p -> !p.getFileName().toString().equals("offsets"))
                    .forEach(topicPath -> {
                        String topicName = topicPath.getFileName().toString();
                        // Count partition directories
                        try (var pStream = java.nio.file.Files.list(topicPath)) {
                            long pCount = pStream.filter(p -> p.getFileName().toString().startsWith("partition-")).count();
                            if (pCount > 0) {
                                topics.put(topicName, new Topic(topicName, (int) pCount, dataDir));
                            }
                        } catch (java.io.IOException ignored) {}
                    });
        } catch (java.io.IOException ignored) {}
    }

    @Override
    public void createTopic(String topicName, int partitionCount) {
        if (topicName == null || topicName.isBlank()) {
            throw new IllegalArgumentException("Topic name cannot be null or blank");
        }
        if (partitionCount < 1) {
            throw new IllegalArgumentException("Partition count must be at least 1, requested: " + partitionCount);
        }

        Topic newTopic = new Topic(topicName, partitionCount, dataDir);
        Topic existing = topics.putIfAbsent(topicName, newTopic);
        if (existing != null) {
            throw new TopicAlreadyExistsException(topicName);
        }
    }

    @Override
    public boolean topicExists(String topicName) {
        if (topicName == null) return false;
        return topics.containsKey(topicName);
    }

    @Override
    public Topic getTopic(String topicName) {
        Objects.requireNonNull(topicName, "Topic name cannot be null");
        Topic topic = topics.get(topicName);
        if (topic == null) {
            throw new TopicNotFoundException(topicName);
        }
        return topic;
    }

    @Override
    public Set<String> listTopics() {
        return Collections.unmodifiableSet(topics.keySet());
    }

    @Override
    public long append(String topicName, int partitionId, Message message) {
        long start = System.nanoTime();
        try {
            Topic topic = getTopic(topicName);
            long offset = topic.append(partitionId, message);
            long latencyUs = Math.max(1, (System.nanoTime() - start) / 1000);
            metrics.recordProduce(1, message.value().length);
            metrics.recordProduceLatencyUs(latencyUs);
            return offset;
        } catch (Exception e) {
            metrics.recordProduceError();
            throw e;
        }
    }

    @Override
    public List<Message> read(String topicName, int partitionId, long fromOffset, int maxMessages) {
        Topic topic = getTopic(topicName);
        List<Message> msgs = topic.read(partitionId, fromOffset, maxMessages);
        long bytes = 0;
        for (Message m : msgs) {
            bytes += m.value().length;
        }
        metrics.recordFetch(msgs.size(), bytes);
        return msgs;
    }

    @Override
    public void deleteTopic(String topicName) {
        Objects.requireNonNull(topicName, "Topic name cannot be null");
        Topic removed = topics.remove(topicName);
        if (removed == null) {
            throw new TopicNotFoundException(topicName);
        }
    }

    @Override
    public com.minikafka.offset.OffsetManager getOffsetManager() {
        return offsetManager;
    }

    @Override
    public com.minikafka.group.ConsumerGroupCoordinator getGroupCoordinator() {
        return groupCoordinator;
    }

    @Override
    public com.minikafka.metrics.BrokerMetrics getMetrics() {
        return metrics;
    }
}
