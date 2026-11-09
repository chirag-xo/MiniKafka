package com.minikafka.storage;

import com.minikafka.exception.InvalidMessageException;
import com.minikafka.exception.MiniKafkaException;
import com.minikafka.exception.OffsetOutOfRangeException;
import com.minikafka.model.Message;
import com.minikafka.partition.Partition;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.stream.Stream;

/**
 * Durable, disk-persisted implementation of {@link Partition} backed by a <em>segmented</em>
 * append-only log — the same design used by Apache Kafka.
 *
 * <p>Each partition directory contains one or more {@link LogSegment} pairs:
 * <ul>
 *   <li>{@code {baseOffset:020d}.log}   — binary message records</li>
 *   <li>{@code {baseOffset:020d}.index} — sparse offset index</li>
 * </ul>
 *
 * <p>When the active segment reaches {@link #MAX_SEGMENT_BYTES}, it is sealed and a fresh
 * segment is created with the current {@code nextOffset} as its base offset.
 *
 * <p>All public methods are thread-safe via a {@link ReentrantReadWriteLock}.
 */
public class PersistentPartition implements Partition, AutoCloseable {

    /** Maximum size of one log segment before the partition rolls to a new one. */
    public static final long MAX_SEGMENT_BYTES = LogSegment.MAX_SEGMENT_BYTES;

    private final String topic;
    private final int id;
    private final Path partitionDir;

    /** Ordered list of segments by ascending baseOffset. */
    private final List<LogSegment> segments = new ArrayList<>();

    private final ReentrantReadWriteLock rwLock = new ReentrantReadWriteLock();
    private final Lock readLock = rwLock.readLock();
    private final Lock writeLock = rwLock.writeLock();

    private volatile boolean closed = false;

    /** Monotonically increasing next offset (log-end offset). */
    private long nextOffset = 0L;

    // -------------------------------------------------------------------------
    // Construction & Recovery
    // -------------------------------------------------------------------------

    /**
     * Creates or recovers a partition at {@code baseDir/topic/partition-{id}}.
     *
     * @param topic   topic name; must not be null or blank.
     * @param id      zero-based partition identifier; must be non-negative.
     * @param baseDir root storage directory; must not be null.
     * @throws MiniKafkaException if the directory cannot be created or recovery fails.
     */
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

        try {
            Files.createDirectories(partitionDir);
        } catch (IOException e) {
            throw new MiniKafkaException("Failed to create partition directory: " + partitionDir, e);
        }

        recoverOrInit();
    }

    /**
     * Scans the partition directory for {@code *.log} files, sorts them by their
     * base-offset filename prefix, and recovers each {@link LogSegment} from disk.
     * If no segments exist yet, creates the first empty segment at offset 0.
     */
    private void recoverOrInit() {
        try (Stream<Path> paths = Files.list(partitionDir)) {
            List<Path> logFiles = paths
                    .filter(p -> p.getFileName().toString().endsWith(".log"))
                    .sorted()
                    .toList();

            if (logFiles.isEmpty()) {
                // Brand-new partition: create the first segment starting at offset 0.
                segments.add(new LogSegment(partitionDir, 0L));
                nextOffset = 0L;
                return;
            }

            for (Path logFile : logFiles) {
                String name = logFile.getFileName().toString();
                // Strip ".log" suffix to get the 20-digit base-offset string.
                long baseOffset = Long.parseLong(name.replace(".log", ""));
                LogSegment seg = LogSegment.recover(partitionDir, baseOffset, topic, id);
                segments.add(seg);
            }

            // The log-end offset is determined by the last (active) segment.
            nextOffset = activeSegment().getNextOffset();

        } catch (IOException e) {
            throw new MiniKafkaException("Failed to recover partition from " + partitionDir, e);
        }
    }

    // -------------------------------------------------------------------------
    // Internal helpers
    // -------------------------------------------------------------------------

    /**
     * @return the last (currently-active) segment.
     */
    private LogSegment activeSegment() {
        return segments.get(segments.size() - 1);
    }

    /**
     * If the active segment has reached {@link #MAX_SEGMENT_BYTES}, closes it and creates
     * a new segment whose base offset equals the current {@code nextOffset}.
     */
    private void maybeRollSegment() {
        if (activeSegment().isFull()) {
            activeSegment().close();
            LogSegment newSeg = new LogSegment(partitionDir, nextOffset);
            segments.add(newSeg);
        }
    }

    // -------------------------------------------------------------------------
    // Partition interface
    // -------------------------------------------------------------------------

    /**
     * {@inheritDoc}
     *
     * <p>Acquires the write lock, rolls the segment if full, then delegates to the active segment.
     */
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
            maybeRollSegment();
            long assignedOffset = nextOffset;
            // Re-stamp the message with the correct partition id and assigned offset.
            Message stamped = message.withPartitionAndOffset(id, assignedOffset);
            activeSegment().append(stamped, assignedOffset);
            nextOffset++;
            return assignedOffset;
        } finally {
            writeLock.unlock();
        }
    }

    /**
     * {@inheritDoc}
     *
     * <p>Acquires the read lock, binary-searches the segment list for the starting segment,
     * then collects messages across adjacent segments until {@code maxMessages} is satisfied.
     */
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
                        getEarliestOffset(),
                        nextOffset
                );
            }

            if (fromOffset == nextOffset) {
                return List.of();
            }

            // Binary search: find last segment whose baseOffset <= fromOffset.
            int startSegIdx = findSegmentIndex(fromOffset);
            if (startSegIdx < 0) {
                return List.of();
            }

            List<Message> result = new ArrayList<>();
            int remaining = maxMessages;

            for (int segIdx = startSegIdx; segIdx < segments.size() && remaining > 0; segIdx++) {
                LogSegment seg = segments.get(segIdx);
                // For the first (starting) segment read from fromOffset; subsequent segments
                // start at their own baseOffset.
                long readFrom = (segIdx == startSegIdx) ? fromOffset : seg.getBaseOffset();
                List<Message> batch = seg.read(topic, id, readFrom, remaining);
                result.addAll(batch);
                remaining -= batch.size();
            }

            return Collections.unmodifiableList(result);
        } finally {
            readLock.unlock();
        }
    }

    /**
     * Binary-searches the {@code segments} list for the index of the last segment
     * whose {@code baseOffset <= targetOffset}.
     *
     * @return index into {@code segments}, or {@code -1} if none found.
     */
    private int findSegmentIndex(long targetOffset) {
        int lo = 0, hi = segments.size() - 1, best = -1;
        while (lo <= hi) {
            int mid = (lo + hi) >>> 1;
            if (segments.get(mid).getBaseOffset() <= targetOffset) {
                best = mid;
                lo = mid + 1;
            } else {
                hi = mid - 1;
            }
        }
        return best;
    }

    @Override
    public long getEarliestOffset() {
        readLock.lock();
        try {
            return segments.isEmpty() ? 0L : segments.get(0).getBaseOffset();
        } finally {
            readLock.unlock();
        }
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

    @Override
    public int getId() {
        return id;
    }

    @Override
    public String getTopic() {
        return topic;
    }

    // -------------------------------------------------------------------------
    // Package-private helpers for LogRetentionManager
    // -------------------------------------------------------------------------

    /**
     * @return the directory that holds this partition's segment files.
     */
    Path getPartitionDir() {
        return partitionDir;
    }


    /**
     * Deletes the oldest (non-active) segment's {@code .log} and {@code .index} files and removes
     * it from the in-memory segment list.
     *
     * @return {@code true} if a segment was deleted; {@code false} if only the active segment remains.
     */
    boolean deleteOldestSegment() {
        writeLock.lock();
        try {
            if (segments.size() <= 1) {
                return false; // Always keep at least the active segment.
            }
            LogSegment oldest = segments.remove(0);
            oldest.close();
            silentDelete(oldest.getLogFile());
            silentDelete(oldest.getIndexFile());
            return true;
        } finally {
            writeLock.unlock();
        }
    }

    /**
     * @return an unmodifiable snapshot of each segment's {@code .log} file size in bytes,
     *         ordered from oldest to newest.
     */
    List<Long> getSegmentSizes() {
        readLock.lock();
        try {
            List<Long> sizes = new ArrayList<>(segments.size());
            for (LogSegment seg : segments) {
                sizes.add(seg.getSizeBytes());
            }
            return Collections.unmodifiableList(sizes);
        } finally {
            readLock.unlock();
        }
    }

    /**
     * @return the total number of segments (including the active one).
     */
    int getSegmentCount() {
        readLock.lock();
        try {
            return segments.size();
        } finally {
            readLock.unlock();
        }
    }

    // -------------------------------------------------------------------------
    // AutoCloseable
    // -------------------------------------------------------------------------

    /**
     * Closes all open segments. Idempotent — safe to call multiple times.
     *
     * @throws MiniKafkaException if any segment fails to close.
     */
    @Override
    public void close() {
        writeLock.lock();
        try {
            if (closed) return;
            closed = true;
            MiniKafkaException firstError = null;
            for (LogSegment seg : segments) {
                try {
                    seg.close();
                } catch (MiniKafkaException e) {
                    if (firstError == null) firstError = e;
                }
            }
            if (firstError != null) {
                throw firstError;
            }
        } finally {
            writeLock.unlock();
        }
    }

    // -------------------------------------------------------------------------
    // Internal utilities
    // -------------------------------------------------------------------------

    private void ensureOpen() {
        if (closed) {
            throw new IllegalStateException("PersistentPartition is closed for topic=" + topic + " id=" + id);
        }
    }

    private static void silentDelete(Path path) {
        try {
            Files.deleteIfExists(path);
        } catch (IOException ignored) {
            // Best-effort deletion; retention cycle will retry on next run.
        }
    }
}
