package com.minikafka.consumer;

import com.minikafka.broker.Broker;
import com.minikafka.model.Message;
import com.minikafka.offset.OffsetAndMetadata;
import com.minikafka.partition.Partition;
import com.minikafka.topic.Topic;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Single-consumer client implementing pull-based batch message consumption,
 * cursor tracking, and crash-resilient offset management.
 */
public class MiniKafkaConsumer implements Consumer {

    private static final int DEFAULT_MAX_POLL_RECORDS = 500;

    private final Broker broker;
    private final String groupId;
    private final String memberId = java.util.UUID.randomUUID().toString();
    private final AutoOffsetReset offsetResetStrategy;
    private final int maxPollRecords;

    private final Set<TopicPartition> assignedPartitions = ConcurrentHashMap.newKeySet();
    private final Map<TopicPartition, Long> positions = new ConcurrentHashMap<>();
    private volatile boolean closed = false;

    public MiniKafkaConsumer(Broker broker) {
        this(broker, null, AutoOffsetReset.EARLIEST, DEFAULT_MAX_POLL_RECORDS);
    }

    public MiniKafkaConsumer(Broker broker, String groupId) {
        this(broker, groupId, AutoOffsetReset.EARLIEST, DEFAULT_MAX_POLL_RECORDS);
    }

    public MiniKafkaConsumer(Broker broker, AutoOffsetReset offsetResetStrategy) {
        this(broker, null, offsetResetStrategy, DEFAULT_MAX_POLL_RECORDS);
    }

    public MiniKafkaConsumer(Broker broker, String groupId, AutoOffsetReset offsetResetStrategy) {
        this(broker, groupId, offsetResetStrategy, DEFAULT_MAX_POLL_RECORDS);
    }

    public MiniKafkaConsumer(Broker broker, String groupId, AutoOffsetReset offsetResetStrategy, int maxPollRecords) {
        this.broker = Objects.requireNonNull(broker, "Broker cannot be null");
        this.groupId = groupId;
        this.offsetResetStrategy = Objects.requireNonNull(offsetResetStrategy, "Offset reset strategy cannot be null");
        if (maxPollRecords <= 0) {
            throw new IllegalArgumentException("maxPollRecords must be greater than zero");
        }
        this.maxPollRecords = maxPollRecords;
    }

    public String getGroupId() {
        return groupId;
    }

    public String getMemberId() {
        return memberId;
    }

    @Override
    public void assign(Collection<TopicPartition> partitions) {
        ensureOpen();
        Objects.requireNonNull(partitions, "Partitions cannot be null");

        assignedPartitions.clear();
        assignedPartitions.addAll(partitions);

        // Remove positions for partitions that are no longer assigned
        positions.keySet().removeIf(tp -> !assignedPartitions.contains(tp));
    }

    @Override
    public void subscribe(Collection<String> topics) {
        ensureOpen();
        Objects.requireNonNull(topics, "Topics cannot be null");

        if (groupId != null) {
            broker.getGroupCoordinator().registerMember(groupId, memberId, new HashSet<>(topics));
            Set<TopicPartition> assigned = broker.getGroupCoordinator().getAssignment(groupId, memberId);
            assign(assigned);
        } else {
            Set<TopicPartition> toAssign = new HashSet<>();
            for (String topicName : topics) {
                Topic topic = broker.getTopic(topicName);
                for (int p = 0; p < topic.getPartitionCount(); p++) {
                    toAssign.add(new TopicPartition(topicName, p));
                }
            }
            assign(toAssign);
        }
    }

    @Override
    public Set<TopicPartition> assignment() {
        ensureOpen();
        syncGroupAssignment();
        return Collections.unmodifiableSet(assignedPartitions);
    }

    private void syncGroupAssignment() {
        if (groupId != null) {
            Set<TopicPartition> latest = broker.getGroupCoordinator().getAssignment(groupId, memberId);
            if (!latest.isEmpty() && !latest.equals(assignedPartitions)) {
                assign(latest);
            }
        }
    }

    @Override
    public ConsumerRecords poll(Duration timeout) {
        ensureOpen();
        syncGroupAssignment();
        if (assignedPartitions.isEmpty()) {
            return ConsumerRecords.empty();
        }

        long timeoutMs = timeout.toMillis();
        long deadline = System.currentTimeMillis() + Math.max(0, timeoutMs);

        Map<TopicPartition, List<ConsumerRecord>> batch = new LinkedHashMap<>();

        do {
            int remainingBudget = maxPollRecords;

            for (TopicPartition tp : assignedPartitions) {
                if (remainingBudget <= 0) break;

                long fetchOffset = position(tp);
                List<Message> messages = broker.read(tp.topic(), tp.partition(), fetchOffset, remainingBudget);

                if (!messages.isEmpty()) {
                    List<ConsumerRecord> records = new ArrayList<>(messages.size());
                    for (Message msg : messages) {
                        records.add(ConsumerRecord.from(msg));
                    }
                    batch.computeIfAbsent(tp, k -> new ArrayList<>()).addAll(records);

                    // Advance consumer position to next offset: lastMessageOffset + 1
                    long nextFetchOffset = messages.get(messages.size() - 1).offset() + 1;
                    positions.put(tp, nextFetchOffset);

                    remainingBudget -= messages.size();
                }
            }

            // If we found any messages, return them immediately
            if (!batch.isEmpty()) {
                return new ConsumerRecords(batch);
            }

            // If timeout has not expired, wait briefly before retrying
            if (System.currentTimeMillis() < deadline) {
                try {
                    Thread.sleep(Math.min(20, deadline - System.currentTimeMillis()));
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    break;
                }
            }
        } while (System.currentTimeMillis() < deadline);

        return ConsumerRecords.empty();
    }

    @Override
    public long position(TopicPartition partition) {
        ensureOpen();
        if (!assignedPartitions.contains(partition)) {
            throw new IllegalArgumentException("Partition is not assigned to this consumer: " + partition);
        }

        return positions.computeIfAbsent(partition, tp -> {
            // Check if a committed offset exists for this consumer group
            if (groupId != null) {
                Optional<OffsetAndMetadata> committed = broker.getOffsetManager().getCommittedOffset(groupId, tp);
                if (committed.isPresent()) {
                    return committed.get().offset();
                }
            }

            // Otherwise fall back to configured offset reset policy
            Partition p = broker.getTopic(tp.topic()).getPartition(tp.partition());
            return switch (offsetResetStrategy) {
                case EARLIEST -> p.getEarliestOffset();
                case LATEST -> p.getNextOffset();
            };
        });
    }

    @Override
    public void commitSync() {
        ensureOpen();
        ensureGroupId();

        for (TopicPartition tp : assignedPartitions) {
            Long currentPos = positions.get(tp);
            if (currentPos != null) {
                broker.getOffsetManager().commitOffset(groupId, tp, OffsetAndMetadata.of(currentPos));
            }
        }
    }

    @Override
    public void commitSync(Map<TopicPartition, Long> offsets) {
        ensureOpen();
        ensureGroupId();
        Objects.requireNonNull(offsets, "Offsets cannot be null");

        for (Map.Entry<TopicPartition, Long> entry : offsets.entrySet()) {
            broker.getOffsetManager().commitOffset(groupId, entry.getKey(), OffsetAndMetadata.of(entry.getValue()));
        }
    }

    @Override
    public Optional<Long> committed(TopicPartition partition) {
        ensureOpen();
        if (groupId == null) {
            return Optional.empty();
        }
        return broker.getOffsetManager().getCommittedOffset(groupId, partition).map(OffsetAndMetadata::offset);
    }

    @Override
    public void seek(TopicPartition partition, long offset) {
        ensureOpen();
        if (!assignedPartitions.contains(partition)) {
            throw new IllegalArgumentException("Partition is not assigned to this consumer: " + partition);
        }
        if (offset < 0) {
            throw new IllegalArgumentException("Offset must be non-negative: " + offset);
        }
        positions.put(partition, offset);
    }

    @Override
    public void seekToBeginning(Collection<TopicPartition> partitions) {
        ensureOpen();
        for (TopicPartition tp : partitions) {
            if (!assignedPartitions.contains(tp)) {
                throw new IllegalArgumentException("Partition is not assigned to this consumer: " + tp);
            }
            Partition p = broker.getTopic(tp.topic()).getPartition(tp.partition());
            positions.put(tp, p.getEarliestOffset());
        }
    }

    @Override
    public void seekToEnd(Collection<TopicPartition> partitions) {
        ensureOpen();
        for (TopicPartition tp : partitions) {
            if (!assignedPartitions.contains(tp)) {
                throw new IllegalArgumentException("Partition is not assigned to this consumer: " + tp);
            }
            Partition p = broker.getTopic(tp.topic()).getPartition(tp.partition());
            positions.put(tp, p.getNextOffset());
        }
    }

    private void ensureOpen() {
        if (closed) {
            throw new IllegalStateException("Consumer is closed");
        }
    }

    private void ensureGroupId() {
        if (groupId == null || groupId.isBlank()) {
            throw new IllegalStateException("Cannot commit offsets: consumer has no groupId configured");
        }
    }

    @Override
    public void close() {
        if (closed) return;
        closed = true;
        if (groupId != null) {
            broker.getGroupCoordinator().unregisterMember(groupId, memberId);
        }
        assignedPartitions.clear();
        positions.clear();
    }
}
