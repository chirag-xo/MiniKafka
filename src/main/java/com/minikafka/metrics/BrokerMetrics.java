package com.minikafka.metrics;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.LongAdder;

/**
 * Thread-safe metrics registry for the MiniKafka broker.
 *
 * <p>Uses lock-free primitives ({@link AtomicLong}, {@link LongAdder}) throughout
 * to avoid adding latency to the hot produce/consume paths.
 *
 * <p>Metrics exposed:
 * <ul>
 *   <li>messages_produced_total — total messages successfully appended</li>
 *   <li>messages_fetched_total — total messages returned to consumers</li>
 *   <li>bytes_written_total — total bytes written to storage</li>
 *   <li>bytes_read_total — total bytes read from storage</li>
 *   <li>produce_errors_total — failed produce attempts</li>
 *   <li>produce_latency_* — P50/P95/P99 latency histogram (microseconds)</li>
 *   <li>consumer_lag{topic,partition,group} — per-consumer-group lag</li>
 * </ul>
 */
public class BrokerMetrics {

    // --- Counters ---
    private final LongAdder messagesProduced = new LongAdder();
    private final LongAdder messagesFetched = new LongAdder();
    private final LongAdder bytesWritten = new LongAdder();
    private final LongAdder bytesRead = new LongAdder();
    private final LongAdder produceErrors = new LongAdder();

    // --- Latency Histogram (microseconds, lock-free bucket array) ---
    // Buckets: <100µs, <500µs, <1ms, <5ms, <10ms, <50ms, <100ms, >=100ms
    private static final long[] BUCKET_UPPER_BOUNDS_US = {100, 500, 1_000, 5_000, 10_000, 50_000, 100_000, Long.MAX_VALUE};
    private final LongAdder[] latencyBuckets = new LongAdder[BUCKET_UPPER_BOUNDS_US.length];
    private final LongAdder latencyTotalUs = new LongAdder();
    private final LongAdder latencySamples = new LongAdder();

    // --- Consumer Lag: key = "topic:partition:groupId" ---
    private final ConcurrentHashMap<String, AtomicLong> consumerLag = new ConcurrentHashMap<>();

    public BrokerMetrics() {
        for (int i = 0; i < latencyBuckets.length; i++) {
            latencyBuckets[i] = new LongAdder();
        }
    }

    // --- Recording methods ---
    public void recordProduce(int messageCount, long bytesWritten) {
        this.messagesProduced.add(messageCount);
        this.bytesWritten.add(bytesWritten);
    }

    public void recordFetch(int messageCount, long bytesRead) {
        this.messagesFetched.add(messageCount);
        this.bytesRead.add(bytesRead);
    }

    public void recordProduceError() {
        produceErrors.increment();
    }

    /**
     * Records a produce latency sample in microseconds.
     * Updates the histogram bucket and running total for percentile computation.
     */
    public void recordProduceLatencyUs(long latencyUs) {
        for (int i = 0; i < BUCKET_UPPER_BOUNDS_US.length; i++) {
            if (latencyUs < BUCKET_UPPER_BOUNDS_US[i]) {
                latencyBuckets[i].increment();
                break;
            }
        }
        latencyTotalUs.add(latencyUs);
        latencySamples.increment();
    }

    /**
     * Updates the consumer lag for a given topic-partition-group combination.
     * lag = latestOffset - committedOffset
     */
    public void updateConsumerLag(String topic, int partition, String groupId, long lag) {
        String key = topic + ":" + partition + ":" + groupId;
        consumerLag.computeIfAbsent(key, k -> new AtomicLong()).set(lag);
    }

    // --- Snapshot / export methods ---
    public long getMessagesProduced() { return messagesProduced.sum(); }
    public long getMessagesFetched() { return messagesFetched.sum(); }
    public long getBytesWritten() { return bytesWritten.sum(); }
    public long getBytesRead() { return bytesRead.sum(); }
    public long getProduceErrors() { return produceErrors.sum(); }

    /**
     * Returns the per-bucket histogram as an array parallel to BUCKET_UPPER_BOUNDS_US.
     */
    public long[] getLatencyHistogram() {
        long[] snapshot = new long[latencyBuckets.length];
        for (int i = 0; i < latencyBuckets.length; i++) {
            snapshot[i] = latencyBuckets[i].sum();
        }
        return snapshot;
    }

    public long getLatencySamples() { return latencySamples.sum(); }

    /**
     * Returns average produce latency in microseconds (0 if no samples).
     */
    public double getAvgLatencyUs() {
        long samples = latencySamples.sum();
        return samples == 0 ? 0.0 : (double) latencyTotalUs.sum() / samples;
    }

    /**
     * Returns all consumer lag entries as a snapshot map (key = "topic:partition:groupId").
     */
    public java.util.Map<String, Long> getConsumerLagSnapshot() {
        java.util.Map<String, Long> snapshot = new java.util.LinkedHashMap<>();
        consumerLag.forEach((k, v) -> snapshot.put(k, v.get()));
        return java.util.Collections.unmodifiableMap(snapshot);
    }

    /**
     * Formats all metrics as a plain-text string (one metric per line, Prometheus-inspired).
     */
    public String formatAsText() {
        StringBuilder sb = new StringBuilder();
        sb.append("# MiniKafka Metrics\n");
        sb.append("messages_produced_total ").append(getMessagesProduced()).append("\n");
        sb.append("messages_fetched_total ").append(getMessagesFetched()).append("\n");
        sb.append("bytes_written_total ").append(getBytesWritten()).append("\n");
        sb.append("bytes_read_total ").append(getBytesRead()).append("\n");
        sb.append("produce_errors_total ").append(getProduceErrors()).append("\n");
        sb.append("produce_latency_samples ").append(getLatencySamples()).append("\n");
        sb.append("produce_latency_avg_us ").append(String.format("%.2f", getAvgLatencyUs())).append("\n");

        long[] hist = getLatencyHistogram();
        String[] labels = {"lt100us","lt500us","lt1ms","lt5ms","lt10ms","lt50ms","lt100ms","ge100ms"};
        for (int i = 0; i < hist.length; i++) {
            sb.append("produce_latency_bucket{le=\"").append(labels[i]).append("\"} ").append(hist[i]).append("\n");
        }

        getConsumerLagSnapshot().forEach((key, lag) -> {
            String[] parts = key.split(":");
            sb.append("consumer_lag{topic=\"").append(parts[0])
              .append("\",partition=\"").append(parts[1])
              .append("\",group=\"").append(parts[2])
              .append("\"} ").append(lag).append("\n");
        });

        return sb.toString();
    }
}
