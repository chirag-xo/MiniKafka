package com.minikafka.producer;

import com.minikafka.broker.Broker;
import com.minikafka.broker.MiniKafkaBroker;
import com.minikafka.exception.ProducerBufferFullException;
import com.minikafka.group.ConsumerGroupCoordinator;
import com.minikafka.metrics.BrokerMetrics;
import com.minikafka.model.Message;
import com.minikafka.offset.OffsetManager;
import com.minikafka.topic.Topic;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.assertj.core.api.Assertions.within;

@DisplayName("Producer Backpressure & Bounded Queue Tests")
class ProducerBackpressureTest {

    private MiniKafkaBroker realBroker;

    @BeforeEach
    void setUp() {
        realBroker = new MiniKafkaBroker();
        realBroker.createTopic("orders", 2);
    }

    /**
     * Test broker wrapper that lets tests deterministically pause or throttle appends
     * to simulate a slow broker under producer overload.
     */
    private static class ControllableBroker implements Broker {
        private final Broker delegate;
        private volatile CountDownLatch blockLatch;
        private volatile CountDownLatch firstAppendEntered;
        private volatile long delayMs = 0L;
        private volatile Thread lastAppendThread;

        ControllableBroker(Broker delegate) {
            this.delegate = delegate;
        }

        void blockAppendsUntilReleased() {
            this.blockLatch = new CountDownLatch(1);
            this.firstAppendEntered = new CountDownLatch(1);
        }

        void releaseBlockedAppends() {
            if (blockLatch != null) {
                blockLatch.countDown();
            }
        }

        void setDelayMs(long delayMs) {
            this.delayMs = delayMs;
        }

        boolean awaitFirstAppendEntered(long timeoutMs) throws InterruptedException {
            return firstAppendEntered != null && firstAppendEntered.await(timeoutMs, TimeUnit.MILLISECONDS);
        }

        @Override
        public long append(String topicName, int partitionId, Message message) {
            lastAppendThread = Thread.currentThread();
            if (firstAppendEntered != null) {
                firstAppendEntered.countDown();
            }
            if (blockLatch != null) {
                try {
                    blockLatch.await(5, TimeUnit.SECONDS);
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                }
            }
            if (delayMs > 0) {
                try {
                    Thread.sleep(delayMs);
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                }
            }
            return delegate.append(topicName, partitionId, message);
        }

        @Override public void createTopic(String topicName, int partitionCount) { delegate.createTopic(topicName, partitionCount); }
        @Override public boolean topicExists(String topicName) { return delegate.topicExists(topicName); }
        @Override public Topic getTopic(String topicName) { return delegate.getTopic(topicName); }
        @Override public Set<String> listTopics() { return delegate.listTopics(); }
        @Override public List<Message> read(String topicName, int partitionId, long fromOffset, int maxMessages) {
            return delegate.read(topicName, partitionId, fromOffset, maxMessages);
        }
        @Override public void deleteTopic(String topicName) { delegate.deleteTopic(topicName); }
        @Override public OffsetManager getOffsetManager() { return delegate.getOffsetManager(); }
        @Override public ConsumerGroupCoordinator getGroupCoordinator() { return delegate.getGroupCoordinator(); }
        @Override public BrokerMetrics getMetrics() { return delegate.getMetrics(); }
    }

    @Nested
    @DisplayName("Configuration Validation Tests")
    class ConfigTests {

        @Test
        @DisplayName("Should reject invalid queue capacities, timeouts, or worker counts")
        void shouldValidateConfig() {
            assertThatThrownBy(() -> new ProducerConfig(0, BackpressurePolicy.REJECT_IMMEDIATELY, Duration.ofSeconds(1), 1))
                    .isInstanceOf(IllegalArgumentException.class);
            assertThatThrownBy(() -> new ProducerConfig(-5, BackpressurePolicy.REJECT_IMMEDIATELY, Duration.ofSeconds(1), 1))
                    .isInstanceOf(IllegalArgumentException.class);
            assertThatThrownBy(() -> new ProducerConfig(10, null, Duration.ofSeconds(1), 1))
                    .isInstanceOf(NullPointerException.class);
            assertThatThrownBy(() -> new ProducerConfig(10, BackpressurePolicy.BLOCK_WITH_TIMEOUT, Duration.ofMillis(-1), 1))
                    .isInstanceOf(IllegalArgumentException.class);
            assertThatThrownBy(() -> new ProducerConfig(10, BackpressurePolicy.BLOCK_WITH_TIMEOUT, Duration.ofSeconds(1), 0))
                    .isInstanceOf(IllegalArgumentException.class);
        }
    }

    @Nested
    @DisplayName("REJECT_IMMEDIATELY Policy Tests")
    class RejectImmediatelyTests {

        @Test
        @DisplayName("Should reject immediately and record metrics when bounded queue reaches capacity")
        void shouldRejectWhenQueueFull() throws Exception {
            ControllableBroker slowBroker = new ControllableBroker(realBroker);
            slowBroker.blockAppendsUntilReleased();

            ProducerConfig config = ProducerConfig.of(3, BackpressurePolicy.REJECT_IMMEDIATELY, Duration.ofMillis(500));
            try (MiniKafkaProducer producer = new MiniKafkaProducer(slowBroker, config)) {
                // Fill all 3 slots
                CompletableFuture<RecordMetadata> f1 = producer.sendAsync(ProducerRecord.of("orders", "k1", "v1"));
                assertThat(slowBroker.awaitFirstAppendEntered(2000)).isTrue();
                CompletableFuture<RecordMetadata> f2 = producer.sendAsync(ProducerRecord.of("orders", "k2", "v2"));
                CompletableFuture<RecordMetadata> f3 = producer.sendAsync(ProducerRecord.of("orders", "k3", "v3"));

                // Queue must now be at 100% utilization (3/3)
                assertThat(producer.getMetrics().getCurrentQueueSize()).isEqualTo(3);
                assertThat(producer.getMetrics().getQueueUtilization()).isCloseTo(1.0, within(0.001));
                assertThat(realBroker.getMetrics().getProducerQueueUtilization()).isCloseTo(1.0, within(0.001));

                // 4th synchronous send must reject immediately
                assertThatThrownBy(() -> producer.send(ProducerRecord.of("orders", "k4", "v4")))
                        .isInstanceOf(ProducerBufferFullException.class)
                        .hasMessageContaining("rejected immediately");

                // 5th async send must also fail immediately with ProducerBufferFullException
                CompletableFuture<RecordMetadata> f5 = producer.sendAsync(ProducerRecord.of("orders", "k5", "v5"));
                assertThat(f5).isCompletedExceptionally();
                assertThatThrownBy(f5::get)
                        .isInstanceOf(ExecutionException.class)
                        .hasCauseInstanceOf(ProducerBufferFullException.class);

                assertThat(producer.getMetrics().getRejectedCount()).isEqualTo(2);
                assertThat(producer.getMetrics().getTimedOutCount()).isZero();
                assertThat(realBroker.getMetrics().getProducerRejectedTotal()).isEqualTo(2);

                // Unblock broker and verify accepted 3 messages drain cleanly
                slowBroker.releaseBlockedAppends();
                CompletableFuture.allOf(f1, f2, f3).get(3, TimeUnit.SECONDS);
                producer.flush();

                assertThat(producer.getMetrics().getCurrentQueueSize()).isZero();
                assertThat(producer.getMetrics().getQueueUtilization()).isZero();
                assertThat(producer.getMetrics().getCompletedCount()).isEqualTo(3);
                assertThat(producer.getMetrics().getPeakQueueSize()).isEqualTo(3);
            }

            // Capacity unregistered on close
            assertThat(realBroker.getMetrics().getProducerQueueCapacity()).isZero();
        }
    }

    @Nested
    @DisplayName("BLOCK_WITH_TIMEOUT Policy Tests")
    class BlockWithTimeoutTests {

        @Test
        @DisplayName("Should block up to sendTimeout and throw ProducerBufferFullException when expired")
        void shouldTimeoutWhenQueueStaysFull() throws Exception {
            ControllableBroker slowBroker = new ControllableBroker(realBroker);
            slowBroker.blockAppendsUntilReleased();

            ProducerConfig config = ProducerConfig.of(2, BackpressurePolicy.BLOCK_WITH_TIMEOUT, Duration.ofMillis(80));
            try (MiniKafkaProducer producer = new MiniKafkaProducer(slowBroker, config)) {
                CompletableFuture<RecordMetadata> f1 = producer.sendAsync(ProducerRecord.of("orders", "k1", "v1"));
                assertThat(slowBroker.awaitFirstAppendEntered(2000)).isTrue();
                CompletableFuture<RecordMetadata> f2 = producer.sendAsync(ProducerRecord.of("orders", "k2", "v2"));

                long startMs = System.currentTimeMillis();
                assertThatThrownBy(() -> producer.send(ProducerRecord.of("orders", "k3", "v3")))
                        .isInstanceOf(ProducerBufferFullException.class)
                        .hasMessageContaining("timed out");
                long elapsedMs = System.currentTimeMillis() - startMs;

                assertThat(elapsedMs).isGreaterThanOrEqualTo(65L);
                assertThat(producer.getMetrics().getRejectedCount()).isEqualTo(1);
                assertThat(producer.getMetrics().getTimedOutCount()).isEqualTo(1);
                assertThat(realBroker.getMetrics().getProducerRejectedTotal()).isEqualTo(1);
                assertThat(realBroker.getMetrics().getProducerTimedOutTotal()).isEqualTo(1);

                slowBroker.releaseBlockedAppends();
                CompletableFuture.allOf(f1, f2).get(3, TimeUnit.SECONDS);
            }
        }

        @Test
        @DisplayName("Should succeed after blocking when queue space frees before timeout")
        void shouldUnblockAndSucceedWhenSpaceFrees() throws Exception {
            ControllableBroker slowBroker = new ControllableBroker(realBroker);
            slowBroker.setDelayMs(35);

            ProducerConfig config = ProducerConfig.of(2, BackpressurePolicy.BLOCK_WITH_TIMEOUT, Duration.ofSeconds(2));
            try (MiniKafkaProducer producer = new MiniKafkaProducer(slowBroker, config)) {
                List<CompletableFuture<RecordMetadata>> futures = new ArrayList<>();
                for (int i = 0; i < 6; i++) {
                    futures.add(producer.sendAsync(ProducerRecord.of("orders", "k" + i, "v" + i)));
                }

                CompletableFuture.allOf(futures.toArray(new CompletableFuture[0])).get(5, TimeUnit.SECONDS);
                producer.flush();

                assertThat(producer.getMetrics().getCompletedCount()).isEqualTo(6);
                assertThat(producer.getMetrics().getRejectedCount()).isZero();
                assertThat(producer.getMetrics().getPeakQueueSize()).isLessThanOrEqualTo(2);
            }
        }
    }

    @Nested
    @DisplayName("CALLER_RUNS Policy Tests")
    class CallerRunsTests {

        @Test
        @DisplayName("Should execute synchronously on caller thread when queue is full without dropping records")
        void shouldRunOnCallerThreadWhenQueueFull() throws Exception {
            ControllableBroker slowBroker = new ControllableBroker(realBroker);
            slowBroker.blockAppendsUntilReleased();

            // Capacity 1, worker 1 -> worker occupies the single slot on slowBroker
            ProducerConfig config = ProducerConfig.of(1, BackpressurePolicy.CALLER_RUNS, Duration.ofMillis(10));
            try (MiniKafkaProducer producer = new MiniKafkaProducer(slowBroker, config)) {
                CompletableFuture<RecordMetadata> f1 = producer.sendAsync(ProducerRecord.of("orders", "k1", "v1"));
                assertThat(slowBroker.awaitFirstAppendEntered(2000)).isTrue();

                // Queue is full (1/1). Release block in a background thread after 40ms, then call sendAsync from a named caller thread
                AtomicReference<Thread> callerThread = new AtomicReference<>();
                CompletableFuture<RecordMetadata> f2 = CompletableFuture.supplyAsync(() -> {
                    callerThread.set(Thread.currentThread());
                    // Wait until f1 is released? Actually f2 should execute directly on callerThread via CALLER_RUNS!
                    // Unblock slowBroker right as f2 enters append so f2 finishes without waiting 5s
                    Thread.ofVirtual().start(() -> {
                        try { Thread.sleep(30); } catch (InterruptedException ignored) {}
                        slowBroker.releaseBlockedAppends();
                    });
                    return producer.sendAsync(ProducerRecord.of("orders", "k2", "v2")).join();
                });

                RecordMetadata md2 = f2.get(5, TimeUnit.SECONDS);
                RecordMetadata md1 = f1.get(5, TimeUnit.SECONDS);

                assertThat(md1).isNotNull();
                assertThat(md2).isNotNull();
                assertThat(producer.getMetrics().getCallerRunsCount()).isEqualTo(1);
                assertThat(realBroker.getMetrics().getProducerCallerRunsTotal()).isEqualTo(1);
                assertThat(producer.getMetrics().getRejectedCount()).isZero();
                assertThat(producer.getMetrics().getCompletedCount()).isEqualTo(2);
            }
        }
    }

    @Nested
    @DisplayName("Overload Management & Metrics Exposure Tests")
    class OverloadAndMetricsTests {

        @Test
        @DisplayName("Should bound memory and expose queue utilization & rejections under high concurrency overload")
        void shouldBoundQueueUnderOverloadAndExposeMetrics() throws Exception {
            ControllableBroker throttledBroker = new ControllableBroker(realBroker);
            throttledBroker.setDelayMs(15);

            int capacity = 6;
            ProducerConfig config = ProducerConfig.of(capacity, BackpressurePolicy.BLOCK_WITH_TIMEOUT, Duration.ofMillis(20));

            int threads = 8;
            int perThread = 10;
            int totalAttempted = threads * perThread;

            try (MiniKafkaProducer producer = new MiniKafkaProducer(throttledBroker, config)) {
                ExecutorService pool = Executors.newFixedThreadPool(threads);
                List<CompletableFuture<RecordMetadata>> allFutures = new ArrayList<>();
                CountDownLatch start = new CountDownLatch(1);

                List<CompletableFuture<Void>> callers = new ArrayList<>();
                for (int t = 0; t < threads; t++) {
                    final int tid = t;
                    callers.add(CompletableFuture.runAsync(() -> {
                        try { start.await(); } catch (InterruptedException ignored) {}
                        for (int i = 0; i < perThread; i++) {
                            CompletableFuture<RecordMetadata> f = producer.sendAsync(
                                    ProducerRecord.of("orders", "t" + tid + "-" + i, "payload"));
                            synchronized (allFutures) {
                                allFutures.add(f);
                            }
                        }
                    }, pool));
                }

                start.countDown();
                CompletableFuture.allOf(callers.toArray(new CompletableFuture[0])).get(10, TimeUnit.SECONDS);
                for (CompletableFuture<RecordMetadata> f : allFutures) {
                    try {
                        f.get(5, TimeUnit.SECONDS);
                    } catch (ExecutionException ignored) {
                        // Expected for timed-out / rejected tasks under overload
                    }
                }
                producer.flush();
                pool.shutdown();

                ProducerMetrics pm = producer.getMetrics();
                // Queue size MUST never exceed configured capacity
                assertThat(pm.getPeakQueueSize()).isLessThanOrEqualTo(capacity);
                assertThat(pm.getCompletedCount() + pm.getRejectedCount()).isEqualTo(totalAttempted);
                assertThat(pm.getRejectedCount()).isGreaterThan(0);
                assertThat(pm.getCurrentQueueSize()).isZero();

                String metricsText = realBroker.getMetrics().formatAsText();
                assertThat(metricsText).contains("producer_queue_size 0");
                assertThat(metricsText).contains("producer_queue_capacity " + capacity);
                assertThat(metricsText).contains("producer_queue_utilization ");
                assertThat(metricsText).contains("producer_rejected_total " + pm.getRejectedCount());
            }
        }
    }
}

