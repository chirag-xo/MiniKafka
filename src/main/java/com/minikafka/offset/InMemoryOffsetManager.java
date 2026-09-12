package com.minikafka.offset;

import com.minikafka.consumer.TopicPartition;

import java.util.Collections;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Thread-safe in-memory implementation of {@link OffsetManager}.
 *
 * <p>Concurrency Architecture:
 * Backed by nested {@link ConcurrentHashMap} structures:
 * {@code groupId -> (TopicPartition -> OffsetAndMetadata)}
 * guaranteeing atomic updates and lock-free concurrent reads across distinct consumer groups.
 */
public class InMemoryOffsetManager implements OffsetManager {

    private final ConcurrentHashMap<String, ConcurrentHashMap<TopicPartition, OffsetAndMetadata>> groupOffsets =
            new ConcurrentHashMap<>();

    @Override
    public void commitOffset(String groupId, TopicPartition partition, OffsetAndMetadata offsetAndMetadata) {
        validateGroupId(groupId);
        Objects.requireNonNull(partition, "Partition cannot be null");
        Objects.requireNonNull(offsetAndMetadata, "OffsetAndMetadata cannot be null");

        groupOffsets.computeIfAbsent(groupId, g -> new ConcurrentHashMap<>())
                .put(partition, offsetAndMetadata);
    }

    @Override
    public Optional<OffsetAndMetadata> getCommittedOffset(String groupId, TopicPartition partition) {
        validateGroupId(groupId);
        Objects.requireNonNull(partition, "Partition cannot be null");

        ConcurrentHashMap<TopicPartition, OffsetAndMetadata> offsets = groupOffsets.get(groupId);
        if (offsets == null) {
            return Optional.empty();
        }
        return Optional.ofNullable(offsets.get(partition));
    }

    @Override
    public Map<TopicPartition, OffsetAndMetadata> getAllCommittedOffsets(String groupId) {
        validateGroupId(groupId);
        ConcurrentHashMap<TopicPartition, OffsetAndMetadata> offsets = groupOffsets.get(groupId);
        if (offsets == null) {
            return Collections.emptyMap();
        }
        return Collections.unmodifiableMap(offsets);
    }

    @Override
    public void resetOffsets(String groupId, String topic) {
        validateGroupId(groupId);
        Objects.requireNonNull(topic, "Topic cannot be null");

        ConcurrentHashMap<TopicPartition, OffsetAndMetadata> offsets = groupOffsets.get(groupId);
        if (offsets != null) {
            offsets.keySet().removeIf(tp -> tp.topic().equals(topic));
        }
    }

    private void validateGroupId(String groupId) {
        if (groupId == null || groupId.isBlank()) {
            throw new IllegalArgumentException("GroupId cannot be null or blank");
        }
    }
}
