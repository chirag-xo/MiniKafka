package com.minikafka.partition;

import com.minikafka.exception.InvalidMessageException;
import com.minikafka.exception.OffsetOutOfRangeException;
import com.minikafka.model.Message;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

/**
 * In-memory implementation of {@link Partition} backed by an {@link ArrayList} and
 * optimized with a {@link ReentrantReadWriteLock} (Single-Writer / Multiple-Readers).
 */
public class InMemoryPartition implements Partition {

    private final int id;
    private final String topic;
    private final List<Message> log = new ArrayList<>();

    private final ReentrantReadWriteLock rwLock = new ReentrantReadWriteLock();
    private final Lock readLock = rwLock.readLock();
    private final Lock writeLock = rwLock.writeLock();

    public InMemoryPartition(String topic, int id) {
        if (topic == null || topic.isBlank()) {
            throw new IllegalArgumentException("Topic name cannot be null or blank");
        }
        if (id < 0) {
            throw new IllegalArgumentException("Partition ID must be non-negative: " + id);
        }
        this.topic = topic;
        this.id = id;
    }

    @Override
    public int getId() {
        return id;
    }

    @Override
    public String getTopic() {
        return topic;
    }

    @Override
    public long append(Message message) {
        Objects.requireNonNull(message, "Message cannot be null");
        if (!topic.equals(message.topic())) {
            throw new InvalidMessageException(String.format(
                    "Message topic '%s' does not match partition topic '%s'", message.topic(), topic));
        }

        writeLock.lock();
        try {
            long assignedOffset = log.size();
            Message persisted = message.withPartitionAndOffset(id, assignedOffset);
            log.add(persisted);
            return assignedOffset;
        } finally {
            writeLock.unlock();
        }
    }

    @Override
    public List<Message> read(long fromOffset, int maxMessages) {
        if (maxMessages <= 0) {
            throw new IllegalArgumentException("maxMessages must be greater than zero, got: " + maxMessages);
        }

        readLock.lock();
        try {
            long currentEndOffset = log.size();

            if (fromOffset < 0 || fromOffset > currentEndOffset) {
                throw new OffsetOutOfRangeException(
                        String.format("Requested offset %d is out of bounds. Valid range: [0, %d]",
                                fromOffset, currentEndOffset),
                        fromOffset,
                        0L,
                        currentEndOffset
                );
            }

            if (fromOffset == currentEndOffset) {
                return List.of();
            }

            int startIndex = (int) fromOffset;
            int endIndex = (int) Math.min(currentEndOffset, fromOffset + maxMessages);

            List<Message> slice = log.subList(startIndex, endIndex);
            return Collections.unmodifiableList(new ArrayList<>(slice));
        } finally {
            readLock.unlock();
        }
    }

    @Override
    public long getEarliestOffset() {
        return 0L;
    }

    @Override
    public long getNextOffset() {
        readLock.lock();
        try {
            return log.size();
        } finally {
            readLock.unlock();
        }
    }

    @Override
    public long size() {
        readLock.lock();
        try {
            return log.size();
        } finally {
            readLock.unlock();
        }
    }

    @Override
    public boolean isEmpty() {
        readLock.lock();
        try {
            return log.isEmpty();
        } finally {
            readLock.unlock();
        }
    }
}
