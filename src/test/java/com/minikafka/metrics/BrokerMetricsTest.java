package com.minikafka.metrics;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.within;

@DisplayName("BrokerMetrics Tests")
class BrokerMetricsTest {

    private BrokerMetrics metrics;

    @BeforeEach
    void setUp() {
        metrics = new BrokerMetrics();
    }

    @Nested
    @DisplayName("Counter Tests")
    class CounterTests {

        @Test
        @DisplayName("Should start with all counters at zero")
        void shouldStartAtZero() {
            assertThat(metrics.getMessagesProduced()).isZero();
            assertThat(metrics.getMessagesFetched()).isZero();
            assertThat(metrics.getBytesWritten()).isZero();
            assertThat(metrics.getBytesRead()).isZero();
            assertThat(metrics.getProduceErrors()).isZero();
        }

        @Test
        @DisplayName("Should record produce correctly")
        void shouldRecordProduce() {
            metrics.recordProduce(10, 1024L);
            assertThat(metrics.getMessagesProduced()).isEqualTo(10);
            assertThat(metrics.getBytesWritten()).isEqualTo(1024L);
        }

        @Test
        @DisplayName("Should record fetch correctly")
        void shouldRecordFetch() {
            metrics.recordFetch(5, 512L);
            assertThat(metrics.getMessagesFetched()).isEqualTo(5);
            assertThat(metrics.getBytesRead()).isEqualTo(512L);
        }

        @Test
        @DisplayName("Should accumulate multiple records")
        void shouldAccumulateMultipleRecords() {
            metrics.recordProduce(5, 100L);
            metrics.recordProduce(3, 50L);
            assertThat(metrics.getMessagesProduced()).isEqualTo(8);
            assertThat(metrics.getBytesWritten()).isEqualTo(150L);
        }

        @Test
        @DisplayName("Should record produce errors")
        void shouldRecordProduceErrors() {
            metrics.recordProduceError();
            metrics.recordProduceError();
            assertThat(metrics.getProduceErrors()).isEqualTo(2);
        }
    }

    @Nested
    @DisplayName("Latency Histogram Tests")
    class LatencyHistogramTests {

        @Test
        @DisplayName("Should record latency into correct bucket")
        void shouldBucketLatencyCorrectly() {
            metrics.recordProduceLatencyUs(50);    // < 100µs bucket
            metrics.recordProduceLatencyUs(300);   // < 500µs bucket
            metrics.recordProduceLatencyUs(800);   // < 1ms bucket
            long[] hist = metrics.getLatencyHistogram();
            assertThat(hist[0]).isEqualTo(1); // lt100us
            assertThat(hist[1]).isEqualTo(1); // lt500us
            assertThat(hist[2]).isEqualTo(1); // lt1ms
        }

        @Test
        @DisplayName("Should compute average latency correctly")
        void shouldComputeAverageLatency() {
            metrics.recordProduceLatencyUs(100L);
            metrics.recordProduceLatencyUs(300L);
            assertThat(metrics.getAvgLatencyUs()).isCloseTo(200.0, within(0.01));
        }

        @Test
        @DisplayName("Should return zero average with no samples")
        void shouldReturnZeroAverageWithNoSamples() {
            assertThat(metrics.getAvgLatencyUs()).isZero();
        }
    }

    @Nested
    @DisplayName("Consumer Lag Tests")
    class ConsumerLagTests {

        @Test
        @DisplayName("Should track consumer lag per topic-partition-group")
        void shouldTrackConsumerLag() {
            metrics.updateConsumerLag("orders", 0, "billing", 15L);
            metrics.updateConsumerLag("orders", 1, "billing", 3L);
            var snapshot = metrics.getConsumerLagSnapshot();
            assertThat(snapshot).containsEntry("orders:0:billing", 15L);
            assertThat(snapshot).containsEntry("orders:1:billing", 3L);
        }

        @Test
        @DisplayName("Should update lag for existing key")
        void shouldUpdateExistingLag() {
            metrics.updateConsumerLag("events", 0, "group1", 100L);
            metrics.updateConsumerLag("events", 0, "group1", 50L);
            assertThat(metrics.getConsumerLagSnapshot()).containsEntry("events:0:group1", 50L);
        }
    }

    @Nested
    @DisplayName("Text Format Tests")
    class TextFormatTests {

        @Test
        @DisplayName("Should include all metrics in text format")
        void shouldFormatAllMetrics() {
            metrics.recordProduce(10, 2048L);
            metrics.recordFetch(5, 1024L);
            metrics.updateConsumerLag("orders", 0, "billing", 7L);

            String text = metrics.formatAsText();
            assertThat(text).contains("messages_produced_total 10");
            assertThat(text).contains("messages_fetched_total 5");
            assertThat(text).contains("bytes_written_total 2048");
            assertThat(text).contains("consumer_lag{topic=\"orders\"");
        }
    }
}
