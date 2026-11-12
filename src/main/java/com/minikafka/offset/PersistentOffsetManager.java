package com.minikafka.offset;

import com.minikafka.consumer.TopicPartition;
import com.minikafka.exception.MiniKafkaException;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.AtomicMoveNotSupportedException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.nio.file.StandardOpenOption;
import java.util.Collections;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Properties;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Stream;

/**
 * Durable, disk-backed implementation of {@link OffsetManager}.
 *
 * <p>Storage Layout:
 * <pre>
 * {baseDir}/
 *   offsets/
 *     {groupId}/
 *       {topic}-{partition}.offset
 * </pre>
 *
 * <p>Durability &amp; Atomicity Guarantees:
 * <ul>
 *   <li>Updates are written to a temporary {@code .offset.tmp} file, flushed via {@code FileChannel.force(true)},
 *       and atomically swapped into place with {@link StandardCopyOption#ATOMIC_MOVE}.</li>
 *   <li>A crash mid-write never leaves a partially written or corrupted {@code .offset} file.</li>
 *   <li>Reads are served directly from an in-memory {@link ConcurrentHashMap} write-through cache.</li>
 * </ul>
 */
public class PersistentOffsetManager implements OffsetManager {

    private static final String OFFSET_EXT = ".offset";
    private static final String TMP_EXT = ".offset.tmp";

    private final Path offsetsDir;
    private final ConcurrentHashMap<String, ConcurrentHashMap<TopicPartition, OffsetAndMetadata>> groupOffsets =
            new ConcurrentHashMap<>();
    private final ConcurrentHashMap<String, Object> writeLocks = new ConcurrentHashMap<>();

    public PersistentOffsetManager(Path baseDir) {
        Objects.requireNonNull(baseDir, "Base storage directory cannot be null");
        this.offsetsDir = baseDir.resolve("offsets");
        try {
            Files.createDirectories(offsetsDir);
            recoverFromDisk();
        } catch (IOException e) {
            throw new MiniKafkaException("Failed to initialize PersistentOffsetManager at: " + offsetsDir, e);
        }
    }

    private void recoverFromDisk() throws IOException {
        if (!Files.exists(offsetsDir)) {
            return;
        }

        try (Stream<Path> groupStreams = Files.list(offsetsDir)) {
            for (Path groupDir : groupStreams.filter(Files::isDirectory).toList()) {
                String groupId = groupDir.getFileName().toString();
                ConcurrentHashMap<TopicPartition, OffsetAndMetadata> partitionMap = new ConcurrentHashMap<>();

                try (Stream<Path> fileStream = Files.list(groupDir)) {
                    for (Path file : fileStream.filter(Files::isRegularFile).toList()) {
                        String fileName = file.getFileName().toString();
                        if (fileName.endsWith(TMP_EXT)) {
                            // Clean up leftover temp file from an interrupted write
                            Files.deleteIfExists(file);
                            continue;
                        }
                        if (!fileName.endsWith(OFFSET_EXT)) {
                            continue;
                        }

                        byte[] raw = Files.readAllBytes(file);
                        Properties props = new Properties();
                        props.load(new ByteArrayInputStream(raw));

                        String topic = props.getProperty("topic");
                        int partition = Integer.parseInt(props.getProperty("partition"));
                        long offset = Long.parseLong(props.getProperty("offset"));
                        long timestamp = Long.parseLong(props.getProperty("timestamp"));
                        String metadata = props.getProperty("metadata", "");

                        TopicPartition tp = new TopicPartition(topic, partition);
                        partitionMap.put(tp, new OffsetAndMetadata(offset, metadata, timestamp));
                    }
                }

                if (!partitionMap.isEmpty()) {
                    groupOffsets.put(groupId, partitionMap);
                }
            }
        }
    }

    @Override
    public void commitOffset(String groupId, TopicPartition partition, OffsetAndMetadata offsetAndMetadata) {
        validateGroupId(groupId);
        Objects.requireNonNull(partition, "Partition cannot be null");
        Objects.requireNonNull(offsetAndMetadata, "OffsetAndMetadata cannot be null");
        validateTopicName(partition.topic());

        String lockKey = groupId + ":" + partition.topic() + ":" + partition.partition();
        Object lock = writeLocks.computeIfAbsent(lockKey, k -> new Object());

        synchronized (lock) {
            writeOffsetAtomically(groupId, partition, offsetAndMetadata);
            groupOffsets.computeIfAbsent(groupId, g -> new ConcurrentHashMap<>())
                    .put(partition, offsetAndMetadata);
        }
    }

    private void writeOffsetAtomically(String groupId, TopicPartition tp, OffsetAndMetadata om) {
        Path groupDir = offsetsDir.resolve(groupId);
        String baseName = tp.topic() + "-" + tp.partition();
        Path targetFile = groupDir.resolve(baseName + OFFSET_EXT);
        Path tmpFile = groupDir.resolve(baseName + TMP_EXT);

        try {
            Files.createDirectories(groupDir);

            Properties props = new Properties();
            props.setProperty("topic", tp.topic());
            props.setProperty("partition", Integer.toString(tp.partition()));
            props.setProperty("offset", Long.toString(om.offset()));
            props.setProperty("timestamp", Long.toString(om.commitTimestamp()));
            props.setProperty("metadata", om.metadata() == null ? "" : om.metadata());

            ByteArrayOutputStream baos = new ByteArrayOutputStream();
            props.store(baos, "MiniKafka Committed Offset");
            ByteBuffer buffer = ByteBuffer.wrap(baos.toByteArray());

            try (FileChannel channel = FileChannel.open(
                    tmpFile,
                    StandardOpenOption.CREATE,
                    StandardOpenOption.WRITE,
                    StandardOpenOption.TRUNCATE_EXISTING
            )) {
                while (buffer.hasRemaining()) {
                    channel.write(buffer);
                }
                channel.force(true);
            }

            try {
                Files.move(tmpFile, targetFile, StandardCopyOption.ATOMIC_MOVE, StandardCopyOption.REPLACE_EXISTING);
            } catch (AtomicMoveNotSupportedException e) {
                Files.move(tmpFile, targetFile, StandardCopyOption.REPLACE_EXISTING);
            }
        } catch (IOException e) {
            throw new MiniKafkaException(
                    String.format("Failed to persist offset %d for group '%s' partition %s",
                            om.offset(), groupId, tp), e);
        }
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
        validateTopicName(topic);

        ConcurrentHashMap<TopicPartition, OffsetAndMetadata> offsets = groupOffsets.get(groupId);
        if (offsets == null) {
            return;
        }

        for (TopicPartition tp : offsets.keySet().stream().filter(p -> p.topic().equals(topic)).toList()) {
            String lockKey = groupId + ":" + tp.topic() + ":" + tp.partition();
            Object lock = writeLocks.computeIfAbsent(lockKey, k -> new Object());
            synchronized (lock) {
                offsets.remove(tp);
                Path targetFile = offsetsDir.resolve(groupId).resolve(tp.topic() + "-" + tp.partition() + OFFSET_EXT);
                try {
                    Files.deleteIfExists(targetFile);
                } catch (IOException e) {
                    throw new MiniKafkaException("Failed to delete offset file: " + targetFile, e);
                }
            }
        }
    }

    public Path getOffsetsDir() {
        return offsetsDir;
    }

    private void validateGroupId(String groupId) {
        if (groupId == null || groupId.isBlank()) {
            throw new IllegalArgumentException("GroupId cannot be null or blank");
        }
        if (groupId.contains("/") || groupId.contains("\\") || groupId.contains("..")) {
            throw new IllegalArgumentException("Invalid characters in groupId: " + groupId);
        }
    }

    private void validateTopicName(String topic) {
        if (topic.contains("/") || topic.contains("\\") || topic.contains("..")) {
            throw new IllegalArgumentException("Invalid characters in topic name: " + topic);
        }
    }
}

