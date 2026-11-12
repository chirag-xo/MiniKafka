package com.minikafka.offset;

import com.minikafka.consumer.TopicPartition;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

@DisplayName("PersistentOffsetManager Tests")
class PersistentOffsetManagerTest {

    @TempDir
    Path tempDir;

    private PersistentOffsetManager offsetManager;

    @BeforeEach
    void setUp() {
        offsetManager = new PersistentOffsetManager(tempDir);
    }

    @Nested
    @DisplayName("Storage Layout & Commit Tests")
    class StorageTests {

        @Test
        @DisplayName("Should persist offset to expected group/topic-partition file layout")
        void shouldPersistToExpectedLayout() {
            TopicPartition tp0 = new TopicPartition("orders", 0);
            TopicPartition tp1 = new TopicPartition("orders", 1);

            offsetManager.commitOffset("billing-service", tp0, OffsetAndMetadata.of(15L, "node-a"));
            offsetManager.commitOffset("billing-service", tp1, OffsetAndMetadata.of(32L, "node-b"));

            Path expectedFile0 = tempDir.resolve("offsets/billing-service/orders-0.offset");
            Path expectedFile1 = tempDir.resolve("offsets/billing-service/orders-1.offset");

            assertThat(expectedFile0).exists().isRegularFile();
            assertThat(expectedFile1).exists().isRegularFile();
            assertThat(tempDir.resolve("offsets/billing-service/orders-0.offset.tmp")).doesNotExist();

            Optional<OffsetAndMetadata> loaded = offsetManager.getCommittedOffset("billing-service", tp0);
            assertThat(loaded).isPresent();
            assertThat(loaded.get().offset()).isEqualTo(15L);
            assertThat(loaded.get().metadata()).isEqualTo("node-a");
        }

        @Test
        @DisplayName("Should atomically overwrite previously committed offset")
        void shouldOverwriteCommittedOffset() {
            TopicPartition tp = new TopicPartition("orders", 0);

            offsetManager.commitOffset("billing-service", tp, OffsetAndMetadata.of(10L));
            offsetManager.commitOffset("billing-service", tp, OffsetAndMetadata.of(25L, "updated"));

            PersistentOffsetManager reloaded = new PersistentOffsetManager(tempDir);
            Optional<OffsetAndMetadata> loaded = reloaded.getCommittedOffset("billing-service", tp);
            assertThat(loaded).isPresent();
            assertThat(loaded.get().offset()).isEqualTo(25L);
            assertThat(loaded.get().metadata()).isEqualTo("updated");
        }

        @Test
        @DisplayName("Should isolate committed offsets between groups")
        void shouldIsolateGroups() {
            TopicPartition tp = new TopicPartition("orders", 0);

            offsetManager.commitOffset("billing-service", tp, OffsetAndMetadata.of(10L));
            offsetManager.commitOffset("analytics-service", tp, OffsetAndMetadata.of(99L));

            PersistentOffsetManager reloaded = new PersistentOffsetManager(tempDir);
            assertThat(reloaded.getCommittedOffset("billing-service", tp).get().offset()).isEqualTo(10L);
            assertThat(reloaded.getCommittedOffset("analytics-service", tp).get().offset()).isEqualTo(99L);
        }
    }

    @Nested
    @DisplayName("Recovery & Reset Tests")
    class RecoveryTests {

        @Test
        @DisplayName("Should recover all committed offsets across manager restart")
        void shouldRecoverOffsetsOnRestart() {
            offsetManager.commitOffset("group-1", new TopicPartition("orders", 0), OffsetAndMetadata.of(100L));
            offsetManager.commitOffset("group-1", new TopicPartition("orders", 1), OffsetAndMetadata.of(200L));
            offsetManager.commitOffset("group-2", new TopicPartition("payments-eu", 3), OffsetAndMetadata.of(50L));

            // Simulate restart with fresh PersistentOffsetManager pointing to same tempDir
            PersistentOffsetManager recovered = new PersistentOffsetManager(tempDir);

            Map<TopicPartition, OffsetAndMetadata> group1Offsets = recovered.getAllCommittedOffsets("group-1");
            assertThat(group1Offsets).hasSize(2);
            assertThat(group1Offsets.get(new TopicPartition("orders", 0)).offset()).isEqualTo(100L);
            assertThat(group1Offsets.get(new TopicPartition("orders", 1)).offset()).isEqualTo(200L);

            assertThat(recovered.getCommittedOffset("group-2", new TopicPartition("payments-eu", 3)))
                    .get()
                    .extracting(OffsetAndMetadata::offset)
                    .isEqualTo(50L);
        }

        @Test
        @DisplayName("Should clean up orphaned .tmp files during recovery")
        void shouldCleanUpOrphanedTempFilesOnRecovery() throws IOException {
            Path groupDir = tempDir.resolve("offsets/billing-service");
            Files.createDirectories(groupDir);
            Path orphanedTmp = groupDir.resolve("orders-0.offset.tmp");
            Files.writeString(orphanedTmp, "corrupted partial write");

            offsetManager.commitOffset("billing-service", new TopicPartition("orders", 1), OffsetAndMetadata.of(5L));

            PersistentOffsetManager recovered = new PersistentOffsetManager(tempDir);

            assertThat(orphanedTmp).doesNotExist();
            assertThat(recovered.getCommittedOffset("billing-service", new TopicPartition("orders", 0))).isEmpty();
            assertThat(recovered.getCommittedOffset("billing-service", new TopicPartition("orders", 1)))
                    .get()
                    .extracting(OffsetAndMetadata::offset)
                    .isEqualTo(5L);
        }

        @Test
        @DisplayName("Should delete offset files on disk when resetting topic offsets")
        void shouldDeleteDiskFilesOnReset() {
            TopicPartition tpOrders0 = new TopicPartition("orders", 0);
            TopicPartition tpOrders1 = new TopicPartition("orders", 1);
            TopicPartition tpPayments0 = new TopicPartition("payments", 0);

            offsetManager.commitOffset("svc", tpOrders0, OffsetAndMetadata.of(10L));
            offsetManager.commitOffset("svc", tpOrders1, OffsetAndMetadata.of(20L));
            offsetManager.commitOffset("svc", tpPayments0, OffsetAndMetadata.of(30L));

            offsetManager.resetOffsets("svc", "orders");

            assertThat(tempDir.resolve("offsets/svc/orders-0.offset")).doesNotExist();
            assertThat(tempDir.resolve("offsets/svc/orders-1.offset")).doesNotExist();
            assertThat(tempDir.resolve("offsets/svc/payments-0.offset")).exists();

            PersistentOffsetManager reloaded = new PersistentOffsetManager(tempDir);
            assertThat(reloaded.getCommittedOffset("svc", tpOrders0)).isEmpty();
            assertThat(reloaded.getCommittedOffset("svc", tpOrders1)).isEmpty();
            assertThat(reloaded.getCommittedOffset("svc", tpPayments0)).isPresent();
        }
    }

    @Nested
    @DisplayName("Concurrency & Validation Tests")
    class ConcurrencyAndValidationTests {

        @Test
        @DisplayName("Should handle concurrent commits to same and different partitions safely")
        void shouldHandleConcurrentCommitsSafely() throws Exception {
            int threads = 8;
            int commitsPerThread = 25;
            TopicPartition tp = new TopicPartition("concurrent-orders", 0);

            ExecutorService pool = Executors.newFixedThreadPool(threads);
            CountDownLatch startLatch = new CountDownLatch(1);
            List<Future<?>> futures = new ArrayList<>();

            for (int t = 0; t < threads; t++) {
                final int threadIdx = t;
                futures.add(pool.submit(() -> {
                    startLatch.await();
                    for (int i = 1; i <= commitsPerThread; i++) {
                        long off = (long) threadIdx * 100 + i;
                        offsetManager.commitOffset("concurrent-group", tp, OffsetAndMetadata.of(off));
                    }
                    return null;
                }));
            }

            startLatch.countDown();
            for (Future<?> f : futures) {
                f.get();
            }
            pool.shutdown();

            // Reload from disk and verify file is intact and valid
            PersistentOffsetManager reloaded = new PersistentOffsetManager(tempDir);
            Optional<OffsetAndMetadata> recovered = reloaded.getCommittedOffset("concurrent-group", tp);
            assertThat(recovered).isPresent();
            assertThat(recovered.get().offset()).isGreaterThan(0L);
            assertThat(tempDir.resolve("offsets/concurrent-group/concurrent-orders-0.offset.tmp")).doesNotExist();
        }

        @Test
        @DisplayName("Should reject invalid groupId or path traversal characters")
        void shouldRejectInvalidInputs() {
            TopicPartition tp = new TopicPartition("orders", 0);
            assertThatThrownBy(() -> offsetManager.commitOffset(null, tp, OffsetAndMetadata.of(1L)))
                    .isInstanceOf(IllegalArgumentException.class);
            assertThatThrownBy(() -> offsetManager.commitOffset("   ", tp, OffsetAndMetadata.of(1L)))
                    .isInstanceOf(IllegalArgumentException.class);
            assertThatThrownBy(() -> offsetManager.commitOffset("../escape", tp, OffsetAndMetadata.of(1L)))
                    .isInstanceOf(IllegalArgumentException.class);
        }
    }
}

