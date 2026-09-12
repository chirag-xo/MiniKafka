package com.minikafka.producer;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

import java.util.HashSet;
import java.util.Set;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

@DisplayName("Partitioner Strategy Tests")
class PartitionerTest {

    @Nested
    @DisplayName("DefaultPartitioner Tests")
    class DefaultPartitionerTests {

        private final DefaultPartitioner partitioner = new DefaultPartitioner();

        @Test
        @DisplayName("Should deterministically route identical keys to the same partition")
        void shouldRouteIdenticalKeysToSamePartition() {
            int partitions = 4;
            String key = "user-account-8829";

            int p1 = partitioner.partition("orders", key, new byte[0], partitions);
            int p2 = partitioner.partition("orders", key, new byte[0], partitions);
            int p3 = partitioner.partition("orders", key, new byte[0], partitions);

            assertThat(p1).isEqualTo(p2);
            assertThat(p2).isEqualTo(p3);
            assertThat(p1).isBetween(0, partitions - 1);
        }

        @Test
        @DisplayName("Should correctly handle negative hashCodes without throwing index error")
        void shouldHandleNegativeHashCodeKeysSafely() {
            int partitions = 5;

            // Test keys known to generate negative Java hashCodes
            String[] testKeys = {"polygenelubricants", "grok", "java_hash_negative_key_123", "test_42"};

            for (String key : testKeys) {
                int p = partitioner.partition("events", key, new byte[0], partitions);
                assertThat(p)
                        .as("Partition for key '%s' must be in range [0, %d)", key, partitions)
                        .isBetween(0, partitions - 1);
            }
        }

        @Test
        @DisplayName("Should distribute keyless records across partitions in round-robin order")
        void shouldRoundRobinKeylessRecords() {
            int partitions = 3;
            String topic = "metrics";

            int p0 = partitioner.partition(topic, null, new byte[0], partitions);
            int p1 = partitioner.partition(topic, null, new byte[0], partitions);
            int p2 = partitioner.partition(topic, null, new byte[0], partitions);
            int p3 = partitioner.partition(topic, null, new byte[0], partitions);

            assertThat(p0).isEqualTo(0);
            assertThat(p1).isEqualTo(1);
            assertThat(p2).isEqualTo(2);
            assertThat(p3).isEqualTo(0); // cycles back to 0
        }

        @Test
        @DisplayName("Should reject non-positive partition count")
        void shouldRejectInvalidPartitionCount() {
            assertThatThrownBy(() -> partitioner.partition("t", "k", new byte[0], 0))
                    .isInstanceOf(IllegalArgumentException.class);
            assertThatThrownBy(() -> partitioner.partition("t", "k", new byte[0], -1))
                    .isInstanceOf(IllegalArgumentException.class);
        }
    }

    @Nested
    @DisplayName("RoundRobinPartitioner Tests")
    class RoundRobinPartitionerTests {

        private final RoundRobinPartitioner partitioner = new RoundRobinPartitioner();

        @Test
        @DisplayName("Should uniformly cycle across partitions even if keys are provided")
        void shouldCycleAcrossPartitionsRegardlessOfKey() {
            int partitions = 3;
            String topic = "logs";

            int p0 = partitioner.partition(topic, "same-key", new byte[0], partitions);
            int p1 = partitioner.partition(topic, "same-key", new byte[0], partitions);
            int p2 = partitioner.partition(topic, "same-key", new byte[0], partitions);
            int p3 = partitioner.partition(topic, "same-key", new byte[0], partitions);

            assertThat(p0).isEqualTo(0);
            assertThat(p1).isEqualTo(1);
            assertThat(p2).isEqualTo(2);
            assertThat(p3).isEqualTo(0);
        }
    }
}
