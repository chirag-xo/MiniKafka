package com.minikafka.partition;

import com.minikafka.exception.InvalidMessageException;
import com.minikafka.exception.OffsetOutOfRangeException;
import com.minikafka.model.Message;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

@DisplayName("InMemoryPartition Tests")
class InMemoryPartitionTest {

    private static final String TOPIC = "orders";
    private static final int PARTITION_ID = 0;
    private InMemoryPartition partition;

    @BeforeEach
    void setUp() {
        partition = new InMemoryPartition(TOPIC, PARTITION_ID);
    }

    @Nested
    @DisplayName("Initialization State")
    class InitializationTests {

        @Test
        @DisplayName("Should initialize as empty partition with zero offsets")
        void shouldInitializeCorrectly() {
            assertThat(partition.getId()).isEqualTo(PARTITION_ID);
            assertThat(partition.getTopic()).isEqualTo(TOPIC);
            assertThat(partition.isEmpty()).isTrue();
            assertThat(partition.size()).isZero();
            assertThat(partition.getEarliestOffset()).isZero();
            assertThat(partition.getNextOffset()).isZero();
        }

        @Test
        @DisplayName("Should reject invalid partition arguments on construction")
        void shouldRejectInvalidConstructorArguments() {
            assertThatThrownBy(() -> new InMemoryPartition(null, 0))
                    .isInstanceOf(IllegalArgumentException.class);
            assertThatThrownBy(() -> new InMemoryPartition("   ", 0))
                    .isInstanceOf(IllegalArgumentException.class);
            assertThatThrownBy(() -> new InMemoryPartition(TOPIC, -1))
                    .isInstanceOf(IllegalArgumentException.class);
        }
    }

    @Nested
    @DisplayName("Append Operations & Monotonic Offsets")
    class AppendTests {

        @Test
        @DisplayName("Should assign strictly monotonic sequential offsets starting from 0")
        void shouldAssignMonotonicOffsets() {
            long offset0 = partition.append(Message.create(TOPIC, "k1", "val0"));
            long offset1 = partition.append(Message.create(TOPIC, "k2", "val1"));
            long offset2 = partition.append(Message.create(TOPIC, "k3", "val2"));

            assertThat(offset0).isEqualTo(0L);
            assertThat(offset1).isEqualTo(1L);
            assertThat(offset2).isEqualTo(2L);
            assertThat(partition.size()).isEqualTo(3L);
            assertThat(partition.getNextOffset()).isEqualTo(3L);
            assertThat(partition.isEmpty()).isFalse();
        }

        @Test
        @DisplayName("Should reject appending message with mismatching topic")
        void shouldRejectMismatchedTopic() {
            Message mismatched = Message.create("payments", "k1", "payload");

            assertThatThrownBy(() -> partition.append(mismatched))
                    .isInstanceOf(InvalidMessageException.class)
                    .hasMessageContaining("does not match partition topic");
        }
    }

    @Nested
    @DisplayName("Read Operations & Slice Semantics")
    class ReadTests {

        @BeforeEach
        void populateMessages() {
            for (int i = 0; i < 5; i++) {
                partition.append(Message.create(TOPIC, "key-" + i, "val-" + i));
            }
        }

        @Test
        @DisplayName("Should read batch of messages up to requested maxMessages")
        void shouldReadBatchCorrectly() {
            List<Message> batch = partition.read(1L, 3);

            assertThat(batch).hasSize(3);
            assertThat(batch.get(0).offset()).isEqualTo(1L);
            assertThat(batch.get(0).valueAsString()).isEqualTo("val-1");
            assertThat(batch.get(1).offset()).isEqualTo(2L);
            assertThat(batch.get(1).valueAsString()).isEqualTo("val-2");
            assertThat(batch.get(2).offset()).isEqualTo(3L);
            assertThat(batch.get(2).valueAsString()).isEqualTo("val-3");
        }

        @Test
        @DisplayName("Should return remaining messages if maxMessages exceeds available records")
        void shouldReturnRemainingWhenMaxExceeds() {
            List<Message> batch = partition.read(3L, 10);

            assertThat(batch).hasSize(2);
            assertThat(batch.get(0).offset()).isEqualTo(3L);
            assertThat(batch.get(1).offset()).isEqualTo(4L);
        }

        @Test
        @DisplayName("Should return empty list when reading exactly at the end of the log")
        void shouldReturnEmptyListAtEndOfLog() {
            List<Message> batch = partition.read(5L, 10);
            assertThat(batch).isEmpty();
        }

        @Test
        @DisplayName("Should throw OffsetOutOfRangeException for negative offset")
        void shouldThrowOnNegativeOffset() {
            assertThatThrownBy(() -> partition.read(-1L, 5))
                    .isInstanceOf(OffsetOutOfRangeException.class);
        }

        @Test
        @DisplayName("Should throw OffsetOutOfRangeException when offset is strictly greater than nextOffset")
        void shouldThrowWhenOffsetExceedsEnd() {
            assertThatThrownBy(() -> partition.read(6L, 5))
                    .isInstanceOf(OffsetOutOfRangeException.class)
                    .hasMessageContaining("Requested offset 6 is out of bounds");
        }

        @Test
        @DisplayName("Should return an unmodifiable list to protect internal log")
        void shouldReturnUnmodifiableList() {
            List<Message> batch = partition.read(0L, 2);
            Message extra = Message.create(TOPIC, "k", "v");

            assertThatThrownBy(() -> batch.add(extra))
                    .isInstanceOf(UnsupportedOperationException.class);
        }
    }

    @Nested
    @DisplayName("Concurrent Appends & Thread Safety")
    class ConcurrencyTests {

        @Test
        @DisplayName("Concurrent appends from multiple threads must yield strictly unique, gapless offsets")
        void shouldHandleConcurrentAppendsAtomically() throws InterruptedException {
            int threadCount = 10;
            int messagesPerThread = 100;
            int totalExpected = threadCount * messagesPerThread;

            ExecutorService executor = Executors.newFixedThreadPool(threadCount);
            CountDownLatch readyLatch = new CountDownLatch(threadCount);
            CountDownLatch startLatch = new CountDownLatch(1);
            CountDownLatch doneLatch = new CountDownLatch(threadCount);

            Set<Long> recordedOffsets = ConcurrentHashMap.newKeySet();

            for (int t = 0; t < threadCount; t++) {
                final int threadId = t;
                executor.submit(() -> {
                    readyLatch.countDown();
                    try {
                        startLatch.await(); // ensure all threads fire simultaneously
                        for (int i = 0; i < messagesPerThread; i++) {
                            Message msg = Message.create(TOPIC, "key-" + threadId, "msg-" + i);
                            long offset = partition.append(msg);
                            recordedOffsets.add(offset);
                        }
                    } catch (Exception e) {
                        e.printStackTrace();
                    } finally {
                        doneLatch.countDown();
                    }
                });
            }

            readyLatch.await(5, TimeUnit.SECONDS);
            startLatch.countDown(); // trigger race
            boolean finished = doneLatch.await(10, TimeUnit.SECONDS);

            executor.shutdown();

            assertThat(finished).isTrue();
            assertThat(partition.size()).isEqualTo(totalExpected);
            assertThat(partition.getNextOffset()).isEqualTo(totalExpected);
            assertThat(recordedOffsets).hasSize(totalExpected); // no duplicate offsets assigned!
        }
    }
}
