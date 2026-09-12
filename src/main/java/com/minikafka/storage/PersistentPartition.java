package com.minikafka.storage;

import com.minikafka.exception.InvalidMessageException;
import com.minikafka.exception.MiniKafkaException;
import com.minikafka.exception.OffsetOutOfRangeException;
import com.minikafka.model.Message;
import com.minikafka.partition.Partition;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

/**
 * Durable, disk-persisted implementation of {@link Partition} backed by an append-only log file
 * and synchronized with a {@link ReentrantReadWriteLock}.
 */
public class PersistentPartition implements Partition, AutoCloseable {

    private final String topic;
    private final int id;
    private final Path partitionDir;
    private final Path logFile;
    private final FileChannel fileChannel;

    private final ReentrantReadWriteLock rwLock = new ReentrantReadWriteLock();
    private final Lock readLock = rwLock.readLock();
    private final Lock writeLock = rwLock.writeLock();

    // In-memory index: offset -> physical byte position in fileChannel
    private final List<Long> offsetToPositionIndex = new ArrayList<>();
    private long nextOffset = 0L;
    private volatile boolean closed = false;

    public PersistentPartition(String topic, int id, Path baseDir) {
        if (topic == null || topic.isBlank()) {
            throw new IllegalArgumentException("Topic name cannot be null or blank");
        }
        if (id < 0) {
            throw new IllegalArgumentException("Partition ID must be non-negative: " + id);
        }
        Objects.requireNonNull(baseDir, "Base storage directory cannot be null");

        this.topic = topic;
        this.id = id;
        this.partitionDir = baseDir.resolve(topic).resolve("partition-" + id);
        this.logFile = partitionDir.resolve("partition.log");

        try {
            Files.createDirectories(partitionDir);
            this.fileChannel = FileChannel.open(
                    logFile,
                    StandardOpenOption.CREATE,
                    StandardOpenOption.READ,
                    StandardOpenOption.WRITE
            );
            recoverState();
        } catch (IOException e) {
            throw new MiniKafkaException("Failed to initialize persistent partition: " + logFile, e);
        }
    }

    private void recoverState() throws IOException {
        long fileSize = fileChannel.size();
        if (fileSize == 0) {
            return;
        }

        long currentPosition = 0L;
        ByteBuffer lengthBuffer = ByteBuffer.allocate(4);

        while (currentPosition < fileSize) {
            fileChannel.position(currentPosition);
            lengthBuffer.clear();

            int bytesRead = fileChannel.read(lengthBuffer);
            if (bytesRead < 4) {
                break;
            }
            lengthBuffer.flip();
            int totalBodyLength = lengthBuffer.getInt();

            ByteBuffer fullRecordBuffer = ByteBuffer.allocate(4 + totalBodyLength);
            fullRecordBuffer.putInt(totalBodyLength);

            bytesRead = fileChannel.read(fullRecordBuffer);
            if (bytesRead < totalBodyLength) {
                break;
            }

            fullRecordBuffer.flip();
            Message message = RecordSerializer.deserialize(fullRecordBuffer, topic, id);

            offsetToPositionIndex.add(currentPosition);
            nextOffset = message.offset() + 1;

            currentPosition += (4 + totalBodyLength);
        }

        fileChannel.position(currentPosition);
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
        ensureOpen();
        Objects.requireNonNull(message, "Message cannot be null");
        if (!topic.equals(message.topic())) {
            throw new InvalidMessageException(String.format(
                    "Message topic '%s' does not match partition topic '%s'", message.topic(), topic));
        }

        writeLock.lock();
        try {
            long assignedOffset = nextOffset;
            Message persisted = message.withPartitionAndOffset(id, assignedOffset);
            ByteBuffer buffer = RecordSerializer.serialize(persisted);

            long writePosition = fileChannel.size();
            fileChannel.position(writePosition);

            while (buffer.hasRemaining()) {
                fileChannel.write(buffer);
            }

            fileChannel.force(false);

            offsetToPositionIndex.add(writePosition);
            nextOffset++;

            return assignedOffset;
        } catch (IOException e) {
            throw new MiniKafkaException("Failed to append message to disk log: " + logFile, e);
        } finally {
            writeLock.unlock();
        }
    }

    @Override
    public List<Message> read(long fromOffset, int maxMessages) {
        ensureOpen();
        if (maxMessages <= 0) {
            throw new IllegalArgumentException("maxMessages must be greater than zero: " + maxMessages);
        }

        readLock.lock();
        try {
            if (fromOffset < 0 || fromOffset > nextOffset) {
                throw new OffsetOutOfRangeException(
                        String.format("Requested offset %d is out of bounds. Valid range: [0, %d]",
                                fromOffset, nextOffset),
                        fromOffset,
                        0L,
                        nextOffset
                );
            }

            if (fromOffset == nextOffset) {
                return List.of();
            }

            List<Message> result = new ArrayList<>();
            int startIndex = (int) fromOffset;
            int limit = (int) Math.min(nextOffset, fromOffset + maxMessages);

            ByteBuffer lengthBuffer = ByteBuffer.allocate(4);

            for (int i = startIndex; i < limit; i++) {
                long recordPos = offsetToPositionIndex.get(i);
                fileChannel.position(recordPos);

                lengthBuffer.clear();
                fileChannel.read(lengthBuffer);
                lengthBuffer.flip();
                int totalBodyLength = lengthBuffer.getInt();

                ByteBuffer fullRecord = ByteBuffer.allocate(4 + totalBodyLength);
                fullRecord.putInt(totalBodyLength);
                fileChannel.read(fullRecord);
                fullRecord.flip();

                result.add(RecordSerializer.deserialize(fullRecord, topic, id));
            }

            return Collections.unmodifiableList(result);
        } catch (IOException e) {
            throw new MiniKafkaException("Failed reading records from disk log: " + logFile, e);
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
            return nextOffset;
        } finally {
            readLock.unlock();
        }
    }

    @Override
    public long size() {
        readLock.lock();
        try {
            return nextOffset;
        } finally {
            readLock.unlock();
        }
    }

    @Override
    public boolean isEmpty() {
        readLock.lock();
        try {
            return nextOffset == 0L;
        } finally {
            readLock.unlock();
        }
    }

    public Path getLogFile() {
        return logFile;
    }

    private void ensureOpen() {
        if (closed) {
            throw new IllegalStateException("PersistentPartition is closed: " + logFile);
        }
    }

    @Override
    public void close() {
        writeLock.lock();
        try {
            if (closed) return;
            closed = true;
            fileChannel.close();
        } catch (IOException e) {
            throw new MiniKafkaException("Error closing file channel: " + logFile, e);
        } finally {
            writeLock.unlock();
        }
    }
}
