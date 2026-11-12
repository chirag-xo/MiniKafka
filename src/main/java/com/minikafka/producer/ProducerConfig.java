package com.minikafka.producer;

import java.time.Duration;
import java.util.Objects;

/**
 * Immutable configuration for bounded producer buffering, backpressure policies, and timeouts.
 *
 * @param maxQueueCapacity   maximum number of buffered/in-flight produce requests before backpressure engages.
 * @param backpressurePolicy policy applied when {@code maxQueueCapacity} is reached.
 * @param sendTimeout        maximum duration to wait for queue capacity under {@link BackpressurePolicy#BLOCK_WITH_TIMEOUT}.
 * @param workerThreads      number of background virtual-thread workers draining the async queue (default 1 for FIFO).
 */
public record ProducerConfig(
        int maxQueueCapacity,
        BackpressurePolicy backpressurePolicy,
        Duration sendTimeout,
        int workerThreads
) {
    public static final int DEFAULT_QUEUE_CAPACITY = 1024;
    public static final Duration DEFAULT_SEND_TIMEOUT = Duration.ofSeconds(5);
    public static final int DEFAULT_WORKER_THREADS = 1;

    public ProducerConfig {
        if (maxQueueCapacity <= 0) {
            throw new IllegalArgumentException("maxQueueCapacity must be positive: " + maxQueueCapacity);
        }
        Objects.requireNonNull(backpressurePolicy, "backpressurePolicy cannot be null");
        Objects.requireNonNull(sendTimeout, "sendTimeout cannot be null");
        if (sendTimeout.isNegative()) {
            throw new IllegalArgumentException("sendTimeout cannot be negative: " + sendTimeout);
        }
        if (workerThreads <= 0) {
            throw new IllegalArgumentException("workerThreads must be positive: " + workerThreads);
        }
    }

    public static ProducerConfig defaults() {
        return new ProducerConfig(
                DEFAULT_QUEUE_CAPACITY,
                BackpressurePolicy.BLOCK_WITH_TIMEOUT,
                DEFAULT_SEND_TIMEOUT,
                DEFAULT_WORKER_THREADS
        );
    }

    public static ProducerConfig of(int maxQueueCapacity, BackpressurePolicy policy, Duration sendTimeout) {
        return new ProducerConfig(maxQueueCapacity, policy, sendTimeout, DEFAULT_WORKER_THREADS);
    }
}

