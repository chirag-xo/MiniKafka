package com.minikafka.producer;

import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.LongAdder;

/**
 * Thread-safe observability metrics for a {@link MiniKafkaProducer}'s bounded queue and backpressure behavior.
 */
public class ProducerMetrics {

    private final int queueCapacity;
    private final AtomicInteger currentQueueSize = new AtomicInteger(0);
    private final AtomicInteger peakQueueSize = new AtomicInteger(0);
    private final LongAdder enqueuedCount = new LongAdder();
    private final LongAdder completedCount = new LongAdder();
    private final LongAdder rejectedCount = new LongAdder();
    private final LongAdder timedOutCount = new LongAdder();
    private final LongAdder callerRunsCount = new LongAdder();

    public ProducerMetrics(int queueCapacity) {
        this.queueCapacity = queueCapacity;
    }

    void recordEnqueued() {
        enqueuedCount.increment();
        int size = currentQueueSize.incrementAndGet();
        peakQueueSize.accumulateAndGet(size, Math::max);
    }

    void recordDequeued() {
        currentQueueSize.updateAndGet(v -> Math.max(0, v - 1));
    }

    void recordCompleted() {
        completedCount.increment();
    }

    void recordRejected(boolean timedOut) {
        rejectedCount.increment();
        if (timedOut) {
            timedOutCount.increment();
        }
    }

    void recordCallerRuns() {
        callerRunsCount.increment();
    }

    public int getQueueCapacity() {
        return queueCapacity;
    }

    public int getCurrentQueueSize() {
        return currentQueueSize.get();
    }

    public int getPeakQueueSize() {
        return peakQueueSize.get();
    }

    /**
     * @return current queue utilization in the range {@code [0.0, 1.0]}.
     */
    public double getQueueUtilization() {
        return queueCapacity == 0 ? 0.0 : (double) currentQueueSize.get() / queueCapacity;
    }

    public long getEnqueuedCount() {
        return enqueuedCount.sum();
    }

    public long getCompletedCount() {
        return completedCount.sum();
    }

    public long getRejectedCount() {
        return rejectedCount.sum();
    }

    public long getTimedOutCount() {
        return timedOutCount.sum();
    }

    public long getCallerRunsCount() {
        return callerRunsCount.sum();
    }
}

