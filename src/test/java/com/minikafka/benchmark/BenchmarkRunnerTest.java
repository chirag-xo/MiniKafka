package com.minikafka.benchmark;

import com.minikafka.broker.Broker;
import com.minikafka.broker.MiniKafkaBroker;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

@DisplayName("BenchmarkRunner Tests")
class BenchmarkRunnerTest {

    @Test
    @DisplayName("Should execute produce and consume benchmarks and compute metrics")
    void shouldExecuteBenchmarkSuite() {
        Broker broker = new MiniKafkaBroker();

        BenchmarkRunner.BenchmarkResult produceResult = BenchmarkRunner.runProduceBenchmark(
                broker, "bench-test-topic", 2, 1000, 128, 2
        );

        assertThat(produceResult.totalMessages()).isEqualTo(1000);
        assertThat(produceResult.throughputMsgSec()).isGreaterThan(0);
        assertThat(produceResult.durationMs()).isGreaterThanOrEqualTo(1);
        assertThat(produceResult.p50LatencyUs()).isGreaterThan(0);

        BenchmarkRunner.BenchmarkResult consumeResult = BenchmarkRunner.runConsumeBenchmark(
                broker, "bench-test-topic", 1000
        );

        assertThat(consumeResult.totalMessages()).isEqualTo(1000);
        assertThat(consumeResult.throughputMsgSec()).isGreaterThan(0);

        // Verify summary printing executes cleanly
        BenchmarkRunner.printSummary(List.of(produceResult, consumeResult));
    }
}

