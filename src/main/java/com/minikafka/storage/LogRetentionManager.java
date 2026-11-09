package com.minikafka.storage;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;

/**
 * Manages log retention by deleting segments that exceed configured thresholds.
 *
 * <p>Two independent retention policies are applied on each cycle:
 * <ol>
 *   <li><strong>Time-based</strong> — segments whose {@code .log} file was last modified more than
 *       {@code retentionMs} milliseconds ago are deleted (the active segment is always preserved).</li>
 *   <li><strong>Size-based</strong> — when the total bytes across all segments of a partition
 *       exceed {@code maxBytes}, the oldest segments are deleted until the partition is within
 *       the limit (the active segment is always preserved).</li>
 * </ol>
 *
 * <p>The retention cycle runs in a background virtual-thread daemon so it does not block
 * application threads and is automatically reclaimed when the JVM exits.
 *
 * <p>Thread-safety: deletion is delegated to {@link PersistentPartition#deleteOldestSegment()},
 * which holds the partition's write lock.
 */
public class LogRetentionManager implements AutoCloseable {

    /** Default retention period: 7 days. */
    public static final long DEFAULT_RETENTION_MS = 7L * 24 * 60 * 60 * 1000;

    /** Default maximum total bytes per partition before size-based deletion triggers. */
    public static final long DEFAULT_MAX_BYTES = 1024L * 1024 * 1024; // 1 GB

    private final List<PersistentPartition> partitions;
    private final long retentionMs;
    private final long maxBytes;
    private final long checkIntervalMs;

    private volatile boolean running = false;
    private Thread daemonThread;

    // -------------------------------------------------------------------------
    // Constructors
    // -------------------------------------------------------------------------

    /**
     * Creates a retention manager with default policies (7 days / 1 GB) checked every 60 seconds.
     *
     * @param partitions the partitions to manage; must not be null.
     */
    public LogRetentionManager(List<PersistentPartition> partitions) {
        this(partitions, DEFAULT_RETENTION_MS, DEFAULT_MAX_BYTES, 60_000L);
    }

    /**
     * Creates a retention manager with custom policies.
     *
     * @param partitions      the partitions to manage; must not be null.
     * @param retentionMs     maximum age of a segment in milliseconds before it is eligible for deletion.
     * @param maxBytes        maximum total bytes per partition before size-based cleanup triggers.
     * @param checkIntervalMs how frequently the retention cycle runs, in milliseconds.
     */
    public LogRetentionManager(List<PersistentPartition> partitions,
                               long retentionMs,
                               long maxBytes,
                               long checkIntervalMs) {
        if (partitions == null) {
            throw new IllegalArgumentException("Partition list cannot be null");
        }
        this.partitions = partitions;
        this.retentionMs = retentionMs;
        this.maxBytes = maxBytes;
        this.checkIntervalMs = checkIntervalMs;
    }

    // -------------------------------------------------------------------------
    // Lifecycle
    // -------------------------------------------------------------------------

    /**
     * Starts the background daemon virtual thread that periodically calls {@link #runRetentionCycle()}.
     * Calling {@code start()} when already running has no effect.
     */
    public void start() {
        if (running) return;
        running = true;
        daemonThread = Thread.ofVirtual()
                .name("minikafka-retention")
                .start(() -> {
                    while (running) {
                        try {
                            runRetentionCycle();
                            Thread.sleep(checkIntervalMs);
                        } catch (InterruptedException e) {
                            Thread.currentThread().interrupt();
                            break;
                        }
                    }
                });
    }

    /**
     * Stops the daemon thread. This method is idempotent.
     */
    @Override
    public void close() {
        running = false;
        if (daemonThread != null) {
            daemonThread.interrupt();
        }
    }

    // -------------------------------------------------------------------------
    // Retention logic
    // -------------------------------------------------------------------------

    /**
     * Executes one full retention cycle over all managed partitions, applying
     * time-based and size-based policies in sequence.
     */
    void runRetentionCycle() {
        for (PersistentPartition partition : partitions) {
            deleteExpiredSegments(partition);
            deleteBeyondSizeLimit(partition);
        }
    }

    /**
     * Deletes all non-active segments whose {@code .log} file was last modified
     * more than {@link #retentionMs} milliseconds ago.
     *
     * @param partition the partition to inspect and clean.
     */
    void deleteExpiredSegments(PersistentPartition partition) {
        long now = System.currentTimeMillis();
        // Keep deleting until there is only one segment left (the active one) or none qualify.
        while (partition.getSegmentCount() > 1) {
            // Peek at the oldest segment's log file last-modified time.
            // We access it through the partition dir rather than a direct handle to avoid holding
            // any internal references to closed segments.
            List<Long> sizes = partition.getSegmentSizes();
            if (sizes.size() <= 1) break;

            // We cannot read the file's last-modified time without a path reference.
            // PersistentPartition exposes deleteOldestSegment() which does the heavy lifting;
            // to check expiry we inspect the first segment's log-file modification time via the
            // partition directory.
            Path oldestLog = resolveOldestLogFile(partition);
            if (oldestLog == null) break;

            long lastModified = oldestLog.toFile().lastModified();
            if (now - lastModified > retentionMs) {
                boolean deleted = partition.deleteOldestSegment();
                if (!deleted) break;
            } else {
                // Segments are ordered by time — if the oldest isn't expired, none are.
                break;
            }
        }
    }

    /**
     * Deletes the oldest segments until total partition bytes fall within {@link #maxBytes},
     * always keeping at least the active (newest) segment.
     *
     * @param partition the partition to inspect and clean.
     */
    void deleteBeyondSizeLimit(PersistentPartition partition) {
        while (partition.getSegmentCount() > 1) {
            List<Long> sizes = partition.getSegmentSizes();
            long totalBytes = sizes.stream().mapToLong(Long::longValue).sum();
            if (totalBytes <= maxBytes) break;

            boolean deleted = partition.deleteOldestSegment();
            if (!deleted) break;
        }
    }

    // -------------------------------------------------------------------------
    // Internal helpers
    // -------------------------------------------------------------------------

    /**
     * Resolves the {@code .log} file path for the oldest (first) segment of the partition.
     * Returns {@code null} if the partition directory cannot be listed.
     */
    private static Path resolveOldestLogFile(PersistentPartition partition) {
        try (var stream = Files.list(partition.getPartitionDir())) {
            return stream
                    .filter(p -> p.getFileName().toString().endsWith(".log"))
                    .sorted()
                    .findFirst()
                    .orElse(null);
        } catch (IOException e) {
            return null;
        }
    }
}
