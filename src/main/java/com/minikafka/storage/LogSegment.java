package com.minikafka.storage;

import com.minikafka.exception.MiniKafkaException;
import com.minikafka.model.Message;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 * A single rolling log segment, backed by two files in the partition directory:
 * <ul>
 *   <li>{@code {baseOffset:020d}.log}   — binary records serialized via {@link RecordSerializer}</li>
 *   <li>{@code {baseOffset:020d}.index} — sparse offset index with {@code [(relativeOffset: 4B)(bytePosition: 4B)]}</li>
 * </ul>
 *
 * <p>An index entry is written every {@link #INDEX_STRIDE} records, allowing fast
 * binary-search-then-linear-scan reads without loading the entire log into memory.
 *
 * <p>Thread-safety: this class is <em>not</em> thread-safe on its own; callers (e.g.,
 * {@link PersistentPartition}) are responsible for external synchronization.
 */
public class LogSegment implements AutoCloseable {

    /** Maximum size of a single segment file before the partition rolls to a new segment. */
    public static final long MAX_SEGMENT_BYTES = 64L * 1024 * 1024; // 64 MB

    /** Write an index entry every N records (sparse index). */
    private static final int INDEX_STRIDE = 8;

    /** Each index entry is 8 bytes: 4B relative offset + 4B byte position. */
    private static final int INDEX_ENTRY_SIZE = 8;

    private final long baseOffset;
    private final Path logFile;
    private final Path indexFile;
    private final FileChannel logChannel;
    private final FileChannel indexChannel;

    /** Current size of the .log file in bytes. */
    private long sizeBytes;

    /** Number of records written to this segment. */
    private long recordCount;

    /** Next offset to assign (= baseOffset + recordCount). */
    private long nextOffset;

    // -------------------------------------------------------------------------
    // Construction
    // -------------------------------------------------------------------------

    /**
     * Opens (or creates) a log segment rooted at {@code segmentDir} with the given {@code baseOffset}.
     * File names are zero-padded to 20 digits so directory listings sort chronologically.
     *
     * @param segmentDir directory in which the .log and .index files will be created.
     * @param baseOffset the first logical offset stored in this segment.
     * @throws MiniKafkaException if file I/O fails.
     */
    public LogSegment(Path segmentDir, long baseOffset) {
        this.baseOffset = baseOffset;
        this.nextOffset = baseOffset;
        this.sizeBytes = 0L;
        this.recordCount = 0L;

        String prefix = String.format("%020d", baseOffset);
        this.logFile = segmentDir.resolve(prefix + ".log");
        this.indexFile = segmentDir.resolve(prefix + ".index");

        try {
            this.logChannel = FileChannel.open(
                    logFile,
                    StandardOpenOption.CREATE,
                    StandardOpenOption.READ,
                    StandardOpenOption.WRITE
            );
            this.indexChannel = FileChannel.open(
                    indexFile,
                    StandardOpenOption.CREATE,
                    StandardOpenOption.READ,
                    StandardOpenOption.WRITE
            );
            // If opening an existing (non-empty) segment, sync sizeBytes from actual file size.
            this.sizeBytes = logChannel.size();
        } catch (IOException e) {
            throw new MiniKafkaException(
                    "Failed to open log segment at " + segmentDir + " baseOffset=" + baseOffset, e);
        }
    }

    // -------------------------------------------------------------------------
    // Append
    // -------------------------------------------------------------------------

    /**
     * Appends a message to this segment, writing a sparse index entry every {@link #INDEX_STRIDE} records.
     *
     * @param message        the message to persist (topic/partition should already match).
     * @param assignedOffset the monotonic offset assigned by the partition.
     * @return the assigned offset (same value as {@code assignedOffset}).
     * @throws MiniKafkaException on I/O failure.
     */
    public long append(Message message, long assignedOffset) {
        // Stamp the message with its final partition and offset before serialization.
        Message persisted = message.withPartitionAndOffset(message.partition(), assignedOffset);
        ByteBuffer buffer = RecordSerializer.serialize(persisted);
        int recordSize = buffer.remaining();

        try {
            // Write sparse index entry BEFORE writing the record so the byte position
            // in the index points to the start of this record.
            if (recordCount % INDEX_STRIDE == 0) {
                writeIndexEntry(assignedOffset, sizeBytes);
            }

            // Write the serialized record at the current end of the log file.
            logChannel.position(sizeBytes);
            while (buffer.hasRemaining()) {
                logChannel.write(buffer);
            }
            logChannel.force(false);

            sizeBytes += recordSize;
            recordCount++;
            nextOffset = assignedOffset + 1;

            return assignedOffset;
        } catch (IOException e) {
            throw new MiniKafkaException("Failed to append record to segment " + logFile, e);
        }
    }

    /**
     * Writes a single 8-byte sparse index entry: {@code (relativeOffset: 4B)(bytePosition: 4B)}.
     */
    private void writeIndexEntry(long absoluteOffset, long bytePosition) throws IOException {
        int relativeOffset = (int) (absoluteOffset - baseOffset);
        int bytePos = (int) bytePosition; // segment max is 64 MB so int is safe
        ByteBuffer entry = ByteBuffer.allocate(INDEX_ENTRY_SIZE);
        entry.putInt(relativeOffset);
        entry.putInt(bytePos);
        entry.flip();
        indexChannel.position(indexChannel.size());
        while (entry.hasRemaining()) {
            indexChannel.write(entry);
        }
    }

    // -------------------------------------------------------------------------
    // Read
    // -------------------------------------------------------------------------

    /**
     * Reads up to {@code maxMessages} messages starting at {@code fromOffset}.
     *
     * <p>Implementation:
     * <ol>
     *   <li>Load all sparse index entries into memory.</li>
     *   <li>Binary-search for the largest relative offset &le; {@code (fromOffset - baseOffset)}.</li>
     *   <li>Seek to the corresponding byte position and linearly scan records.</li>
     *   <li>Skip records before {@code fromOffset}, then collect up to {@code maxMessages}.</li>
     * </ol>
     *
     * @param topic       topic name (used for deserialization).
     * @param partitionId partition id (used for deserialization).
     * @param fromOffset  first offset to include (inclusive).
     * @param maxMessages maximum number of messages to return.
     * @return unmodifiable list of messages, may be empty.
     * @throws MiniKafkaException on I/O failure.
     */
    public List<Message> read(String topic, int partitionId, long fromOffset, int maxMessages) {
        if (fromOffset >= nextOffset) {
            return List.of();
        }

        try {
            // Load all index entries: each encoded as (relativeOffset << 32 | bytePosition).
            long indexSize = indexChannel.size();
            int entryCount = (int) (indexSize / INDEX_ENTRY_SIZE);
            long[] indexEntries = new long[entryCount];

            if (entryCount > 0) {
                ByteBuffer indexBuf = ByteBuffer.allocate((int) indexSize);
                indexChannel.position(0);
                while (indexBuf.hasRemaining()) {
                    int read = indexChannel.read(indexBuf);
                    if (read < 0) break;
                }
                indexBuf.flip();
                for (int i = 0; i < entryCount; i++) {
                    int relOff = indexBuf.getInt();
                    int bytePos = indexBuf.getInt();
                    indexEntries[i] = ((long) relOff << 32) | (bytePos & 0xFFFFFFFFL);
                }
            }

            // Binary search: find largest relativeOffset <= target.
            long targetRelative = fromOffset - baseOffset;
            long startBytePosition = 0L;

            if (entryCount > 0) {
                int lo = 0, hi = entryCount - 1, best = 0;
                while (lo <= hi) {
                    int mid = (lo + hi) >>> 1;
                    long relOff = indexEntries[mid] >> 32;
                    if (relOff <= targetRelative) {
                        best = mid;
                        lo = mid + 1;
                    } else {
                        hi = mid - 1;
                    }
                }
                startBytePosition = indexEntries[best] & 0xFFFFFFFFL;
            }

            // Linear scan from startBytePosition, skip until fromOffset.
            List<Message> result = new ArrayList<>();
            long pos = startBytePosition;
            ByteBuffer lenBuf = ByteBuffer.allocate(4);

            while (pos < sizeBytes && result.size() < maxMessages) {
                logChannel.position(pos);
                lenBuf.clear();
                int read = logChannel.read(lenBuf);
                if (read < 4) break;
                lenBuf.flip();
                int bodyLen = lenBuf.getInt();

                ByteBuffer fullRecord = ByteBuffer.allocate(4 + bodyLen);
                fullRecord.putInt(bodyLen);
                int remaining = bodyLen;
                while (remaining > 0) {
                    int r = logChannel.read(fullRecord);
                    if (r < 0) break;
                    remaining -= r;
                }
                fullRecord.flip();

                Message msg = RecordSerializer.deserialize(fullRecord, topic, partitionId);

                if (msg.offset() >= fromOffset) {
                    result.add(msg);
                }

                pos += (4 + bodyLen);
            }

            return Collections.unmodifiableList(result);
        } catch (IOException e) {
            throw new MiniKafkaException("Failed to read from segment " + logFile, e);
        }
    }

    // -------------------------------------------------------------------------
    // Segment fullness
    // -------------------------------------------------------------------------

    /**
     * @return {@code true} if this segment has reached or exceeded {@link #MAX_SEGMENT_BYTES}.
     */
    public boolean isFull() {
        return sizeBytes >= MAX_SEGMENT_BYTES;
    }

    // -------------------------------------------------------------------------
    // Recovery
    // -------------------------------------------------------------------------

    /**
     * Recovers a segment that already exists on disk by scanning its {@code .log} file from scratch,
     * rebuilding {@code recordCount}, {@code sizeBytes}, and {@code nextOffset}, then rewriting
     * the sparse {@code .index} file.
     *
     * @param segmentDir  directory containing the segment files.
     * @param baseOffset  the base offset encoded in the filename.
     * @param topic       topic name used for deserialization.
     * @param partitionId partition id used for deserialization.
     * @return a fully-initialized {@link LogSegment} ready for appending or reading.
     * @throws MiniKafkaException on I/O failure or corrupt data.
     */
    public static LogSegment recover(Path segmentDir, long baseOffset, String topic, int partitionId) {
        LogSegment segment = new LogSegment(segmentDir, baseOffset);

        try {
            long fileSize = segment.logChannel.size();
            if (fileSize == 0) {
                return segment;
            }

            // Reset to start-of-file and rebuild index from scratch.
            segment.sizeBytes = 0L;
            segment.recordCount = 0L;
            segment.nextOffset = baseOffset;

            // Truncate any stale index file.
            segment.indexChannel.truncate(0);

            long pos = 0L;
            ByteBuffer lenBuf = ByteBuffer.allocate(4);

            while (pos < fileSize) {
                segment.logChannel.position(pos);
                lenBuf.clear();
                int r = segment.logChannel.read(lenBuf);
                if (r < 4) break;
                lenBuf.flip();
                int bodyLen = lenBuf.getInt();

                if (bodyLen <= 0 || pos + 4 + bodyLen > fileSize) {
                    // Truncate corrupt tail.
                    break;
                }

                ByteBuffer fullRecord = ByteBuffer.allocate(4 + bodyLen);
                fullRecord.putInt(bodyLen);
                int remaining = bodyLen;
                while (remaining > 0) {
                    int read = segment.logChannel.read(fullRecord);
                    if (read < 0) break;
                    remaining -= read;
                }
                fullRecord.flip();

                Message msg = RecordSerializer.deserialize(fullRecord, topic, partitionId);

                // Write sparse index entry if appropriate.
                if (segment.recordCount % INDEX_STRIDE == 0) {
                    segment.writeIndexEntry(msg.offset(), pos);
                }

                segment.recordCount++;
                segment.sizeBytes = pos + 4 + bodyLen;
                segment.nextOffset = msg.offset() + 1;
                pos = segment.sizeBytes;
            }

            // Truncate the log file to the last good record boundary.
            segment.logChannel.truncate(segment.sizeBytes);

            return segment;
        } catch (IOException e) {
            throw new MiniKafkaException(
                    "Failed to recover segment at " + segmentDir + " baseOffset=" + baseOffset, e);
        }
    }

    // -------------------------------------------------------------------------
    // Getters
    // -------------------------------------------------------------------------

    /** @return the base (first) offset of this segment. */
    public long getBaseOffset() {
        return baseOffset;
    }

    /** @return the next offset that will be assigned in this segment (= baseOffset + recordCount). */
    public long getNextOffset() {
        return nextOffset;
    }

    /** @return the current size of the {@code .log} file in bytes. */
    public long getSizeBytes() {
        return sizeBytes;
    }

    /** @return the absolute path to the {@code .log} file. */
    public Path getLogFile() {
        return logFile;
    }

    /** @return the absolute path to the {@code .index} file. */
    public Path getIndexFile() {
        return indexFile;
    }

    // -------------------------------------------------------------------------
    // AutoCloseable
    // -------------------------------------------------------------------------

    /**
     * Closes both the {@code .log} and {@code .index} file channels.
     *
     * @throws MiniKafkaException if closing either channel fails.
     */
    @Override
    public void close() {
        IOException first = null;
        try {
            logChannel.close();
        } catch (IOException e) {
            first = e;
        }
        try {
            indexChannel.close();
        } catch (IOException e) {
            if (first == null) first = e;
        }
        if (first != null) {
            throw new MiniKafkaException("Failed to close log segment " + logFile, first);
        }
    }
}
