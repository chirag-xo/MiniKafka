package com.minikafka.storage;

import com.minikafka.model.Message;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Comparator;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Unit tests for {@link LogSegment}: append, read, full-detection, and crash recovery.
 */
@DisplayName("LogSegment Tests")
class LogSegmentTest {

    private static final String TOPIC = "test-topic";
    private static final int PARTITION_ID = 0;

    private Path tempDir;

    @BeforeEach
    void setUp() throws IOException {
        tempDir = Files.createTempDirectory("log-segment-test");
    }

    @AfterEach
    void tearDown() throws IOException {
        // Recursively delete the temp directory.
        try (var walk = Files.walk(tempDir)) {
            walk.sorted(Comparator.reverseOrder())
                    .forEach(p -> p.toFile().delete());
        }
    }

    // -------------------------------------------------------------------------
    // Helper
    // -------------------------------------------------------------------------

    private Message makeMessage(String key, String value) {
        return Message.create(TOPIC, key, value);
    }

    // -------------------------------------------------------------------------
    // Append Tests
    // -------------------------------------------------------------------------

    @Nested
    @DisplayName("Append Tests")
    class AppendTests {

        @Test
        @DisplayName("Should append and read back a single message")
        void shouldAppendAndReadSingleMessage() {
            try (LogSegment seg = new LogSegment(tempDir, 0L)) {
                Message msg = makeMessage("k1", "hello");
                long offset = seg.append(msg.withPartitionAndOffset(PARTITION_ID, 0L), 0L);

                assertThat(offset).isEqualTo(0L);

                List<Message> read = seg.read(TOPIC, PARTITION_ID, 0L, 10);
                assertThat(read).hasSize(1);
                assertThat(read.get(0).offset()).isEqualTo(0L);
                assertThat(read.get(0).valueAsString()).isEqualTo("hello");
            }
        }

        @Test
        @DisplayName("Should assign correct sequential offsets")
        void shouldAssignCorrectOffsets() {
            try (LogSegment seg = new LogSegment(tempDir, 5L)) {
                for (int i = 0; i < 3; i++) {
                    Message msg = makeMessage("k" + i, "v" + i);
                    long assignedOffset = 5L + i;
                    long returned = seg.append(msg.withPartitionAndOffset(PARTITION_ID, assignedOffset), assignedOffset);
                    assertThat(returned).isEqualTo(assignedOffset);
                }

                assertThat(seg.getBaseOffset()).isEqualTo(5L);
                assertThat(seg.getNextOffset()).isEqualTo(8L);
            }
        }

        @Test
        @DisplayName("Should report correct sizeBytes after appends")
        void shouldReportCorrectSizeBytes() {
            try (LogSegment seg = new LogSegment(tempDir, 0L)) {
                assertThat(seg.getSizeBytes()).isEqualTo(0L);

                seg.append(makeMessage("k", "value").withPartitionAndOffset(PARTITION_ID, 0L), 0L);
                assertThat(seg.getSizeBytes()).isGreaterThan(0L);

                long afterFirst = seg.getSizeBytes();
                seg.append(makeMessage("k2", "value2").withPartitionAndOffset(PARTITION_ID, 1L), 1L);
                assertThat(seg.getSizeBytes()).isGreaterThan(afterFirst);
            }
        }
    }

    // -------------------------------------------------------------------------
    // Read Tests
    // -------------------------------------------------------------------------

    @Nested
    @DisplayName("Read Tests")
    class ReadTests {

        @Test
        @DisplayName("Should read multiple messages starting from a given offset")
        void shouldReadMultipleMessagesFromOffset() {
            try (LogSegment seg = new LogSegment(tempDir, 0L)) {
                for (int i = 0; i < 10; i++) {
                    Message m = makeMessage("k" + i, "val" + i).withPartitionAndOffset(PARTITION_ID, i);
                    seg.append(m, i);
                }

                List<Message> result = seg.read(TOPIC, PARTITION_ID, 3L, 4);
                assertThat(result).hasSize(4);
                assertThat(result.get(0).offset()).isEqualTo(3L);
                assertThat(result.get(3).offset()).isEqualTo(6L);
            }
        }

        @Test
        @DisplayName("Should respect maxMessages limit")
        void shouldReadPartialBatch() {
            try (LogSegment seg = new LogSegment(tempDir, 0L)) {
                for (int i = 0; i < 20; i++) {
                    Message m = makeMessage("k" + i, "v" + i).withPartitionAndOffset(PARTITION_ID, i);
                    seg.append(m, i);
                }

                List<Message> result = seg.read(TOPIC, PARTITION_ID, 0L, 5);
                assertThat(result).hasSize(5);
            }
        }

        @Test
        @DisplayName("Should return empty list when fromOffset equals or exceeds nextOffset")
        void shouldReturnEmptyListWhenOffsetBeyondSegment() {
            try (LogSegment seg = new LogSegment(tempDir, 10L)) {
                Message m = makeMessage("k", "v").withPartitionAndOffset(PARTITION_ID, 10L);
                seg.append(m, 10L);

                // nextOffset is now 11; reading at 11 returns empty.
                List<Message> result = seg.read(TOPIC, PARTITION_ID, 11L, 10);
                assertThat(result).isEmpty();
            }
        }
    }

    // -------------------------------------------------------------------------
    // Segmentation Tests
    // -------------------------------------------------------------------------

    @Nested
    @DisplayName("Segmentation Tests")
    class SegmentationTests {

        @Test
        @DisplayName("Should not be full when empty")
        void shouldNotBeFullWhenEmpty() {
            try (LogSegment seg = new LogSegment(tempDir, 0L)) {
                assertThat(seg.isFull()).isFalse();
            }
        }

        @Test
        @DisplayName("isFull should be false after a small number of appends")
        void shouldNotBeFullAfterFewAppends() {
            try (LogSegment seg = new LogSegment(tempDir, 0L)) {
                for (int i = 0; i < 10; i++) {
                    Message m = makeMessage("k" + i, "v" + i).withPartitionAndOffset(PARTITION_ID, i);
                    seg.append(m, i);
                }
                // 10 small records are nowhere near 64 MB.
                assertThat(seg.isFull()).isFalse();
            }
        }
    }

    // -------------------------------------------------------------------------
    // Recovery Tests
    // -------------------------------------------------------------------------

    @Nested
    @DisplayName("Recovery Tests")
    class RecoveryTests {

        @Test
        @DisplayName("Should recover nextOffset and recordCount after reopening")
        void shouldRecoverStateAfterReopen() {
            // Write 3 messages, then recover and verify state.
            try (LogSegment seg = new LogSegment(tempDir, 0L)) {
                for (int i = 0; i < 3; i++) {
                    Message m = makeMessage("k" + i, "val" + i).withPartitionAndOffset(PARTITION_ID, i);
                    seg.append(m, i);
                }
                assertThat(seg.getNextOffset()).isEqualTo(3L);
            }

            // Recover without touching the files.
            LogSegment recovered = LogSegment.recover(tempDir, 0L, TOPIC, PARTITION_ID);
            try (recovered) {
                assertThat(recovered.getNextOffset()).isEqualTo(3L);
                assertThat(recovered.getSizeBytes()).isGreaterThan(0L);

                List<Message> msgs = recovered.read(TOPIC, PARTITION_ID, 0L, 10);
                assertThat(msgs).hasSize(3);
                assertThat(msgs.get(0).offset()).isEqualTo(0L);
                assertThat(msgs.get(2).offset()).isEqualTo(2L);
            }
        }

        @Test
        @DisplayName("Should recover all records across multiple INDEX_STRIDE boundaries")
        void shouldRecoverMultipleRecords() {
            int count = 25; // exceeds INDEX_STRIDE (8) multiple times

            try (LogSegment seg = new LogSegment(tempDir, 100L)) {
                for (int i = 0; i < count; i++) {
                    long off = 100L + i;
                    Message m = makeMessage("k" + i, "value-" + i).withPartitionAndOffset(PARTITION_ID, off);
                    seg.append(m, off);
                }
            }

            LogSegment recovered = LogSegment.recover(tempDir, 100L, TOPIC, PARTITION_ID);
            try (recovered) {
                assertThat(recovered.getNextOffset()).isEqualTo(100L + count);

                List<Message> msgs = recovered.read(TOPIC, PARTITION_ID, 100L, count);
                assertThat(msgs).hasSize(count);
                for (int i = 0; i < count; i++) {
                    assertThat(msgs.get(i).offset()).isEqualTo(100L + i);
                    assertThat(msgs.get(i).valueAsString()).isEqualTo("value-" + i);
                }
            }
        }
    }
}
