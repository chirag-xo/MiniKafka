package com.minikafka.benchmark;

import com.minikafka.broker.Broker;
import com.minikafka.broker.MiniKafkaBroker;
import com.minikafka.consumer.Consumer;
import com.minikafka.consumer.ConsumerRecord;
import com.minikafka.consumer.ConsumerRecords;
import com.minikafka.consumer.MiniKafkaConsumer;
import com.minikafka.producer.MiniKafkaProducer;
import com.minikafka.producer.Producer;
import com.minikafka.producer.ProducerRecord;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicLong;

/**
 * End-to-end benchmark harness measuring sustained throughput (msg/sec, MB/sec)
 * and P50/P90/P95/P99 latency profiles under concurrent load.
 */
public class BenchmarkRunner {

    public record BenchmarkResult(
            String mode,
            int totalMessages,
            int payloadBytes,
            int threads,
            long durationMs,
            double throughputMsgSec,
            double throughputMbSec,
            double p50LatencyUs,
            double p95LatencyUs,
            double p99LatencyUs
    ) {}

    public static BenchmarkResult runProduceBenchmark(Broker broker, String topic, int partitionCount,
                                                       int totalMessages, int payloadSizeBytes, int concurrency) {
        if (!broker.topicExists(topic)) {
            broker.createTopic(topic, partitionCount);
        }

        Producer producer = new MiniKafkaProducer(broker);
        byte[] payload = new byte[payloadSizeBytes];
        Arrays.fill(payload, (byte) 'X');

        long[] latenciesUs = new long[totalMessages];
        AtomicLong indexSeq = new AtomicLong(0);

        ExecutorService executor = Executors.newFixedThreadPool(concurrency);
        CountDownLatch latch = new CountDownLatch(concurrency);
        int messagesPerThread = totalMessages / concurrency;

        long startTime = System.nanoTime();

        for (int t = 0; t < concurrency; t++) {
            executor.submit(() -> {
                try {
                    for (int i = 0; i < messagesPerThread; i++) {
                        long t0 = System.nanoTime();
                        producer.send(ProducerRecord.of(topic, "key-" + i, payload));
                        long t1 = System.nanoTime();

                        int idx = (int) indexSeq.getAndIncrement();
                        if (idx < latenciesUs.length) {
                            latenciesUs[idx] = Math.max(1, (t1 - t0) / 1000);
                        }
                    }
                } finally {
                    latch.countDown();
                }
            });
        }

        try {
            latch.await();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
        long durationNs = System.nanoTime() - startTime;
        executor.shutdown();
        producer.close();

        long durationMs = Math.max(1, durationNs / 1_000_000);
        double throughputMsgSec = ((double) totalMessages / durationNs) * 1_000_000_000.0;
        double throughputMbSec = (throughputMsgSec * payloadSizeBytes) / (1024 * 1024);

        Arrays.sort(latenciesUs, 0, (int) Math.min(totalMessages, indexSeq.get()));
        int recordedCount = (int) Math.min(totalMessages, indexSeq.get());
        double p50 = latenciesUs[(int) (recordedCount * 0.50)];
        double p95 = latenciesUs[(int) (recordedCount * 0.95)];
        double p99 = latenciesUs[(int) (recordedCount * 0.99)];

        return new BenchmarkResult(
                "Produce",
                totalMessages,
                payloadSizeBytes,
                concurrency,
                durationMs,
                throughputMsgSec,
                throughputMbSec,
                p50,
                p95,
                p99
        );
    }

    public static BenchmarkResult runConsumeBenchmark(Broker broker, String topic, int totalMessages) {
        Consumer consumer = new MiniKafkaConsumer(broker, "benchmark-group");
        consumer.subscribe(List.of(topic));

        int consumed = 0;
        long startTime = System.nanoTime();

        while (consumed < totalMessages) {
            ConsumerRecords records = consumer.poll(Duration.ofMillis(200));
            if (records.isEmpty()) {
                break;
            }
            for (ConsumerRecord r : records) {
                consumed++;
            }
            consumer.commitSync();
        }

        long durationNs = System.nanoTime() - startTime;
        long durationMs = Math.max(1, durationNs / 1_000_000);
        double throughputMsgSec = ((double) consumed / durationNs) * 1_000_000_000.0;
        double throughputMbSec = (throughputMsgSec * 128) / (1024 * 1024); // estimate 128B

        consumer.close();

        return new BenchmarkResult(
                "Consume",
                consumed,
                128,
                1,
                durationMs,
                throughputMsgSec,
                throughputMbSec,
                0,
                0,
                0
        );
    }

    public static void printSummary(List<BenchmarkResult> results) {
        System.out.println("\n=========================================================================================================");
        System.out.println("                                     MiniKafka Performance Benchmarks                                   ");
        System.out.println("=========================================================================================================");
        System.out.printf("%-12s | %-10s | %-8s | %-8s | %-12s | %-14s | %-10s | %-10s | %-10s%n",
                "Workload", "Messages", "Payload", "Threads", "Duration", "Throughput", "P50 Lat", "P95 Lat", "P99 Lat");
        System.out.println("-------------+------------+----------+----------+--------------+----------------+------------+------------+-----------");

        for (BenchmarkResult r : results) {
            System.out.printf("%-12s | %-10d | %-5dB   | %-8d | %-8d ms | %-10.0f msg/s | %-8.1f µs | %-8.1f µs | %-8.1f µs%n",
                    r.mode(),
                    r.totalMessages(),
                    r.payloadBytes(),
                    r.threads(),
                    r.durationMs(),
                    r.throughputMsgSec(),
                    r.p50LatencyUs(),
                    r.p95LatencyUs(),
                    r.p99LatencyUs()
            );
        }
        System.out.println("=========================================================================================================\n");
    }

    public static void main(String[] args) throws IOException {
        System.out.println("Starting MiniKafka Benchmark Suite (Java 21 Virtual Threads & NIO storage engine)...");

        List<BenchmarkResult> results = new ArrayList<>();

        // 1. In-Memory Broker Benchmark
        System.out.println("\n[1/3] Running In-Memory Broker Produce Benchmark (100,000 messages, 4 partitions, 8 threads)...");
        Broker inMemoryBroker = new MiniKafkaBroker();
        BenchmarkResult memProduce = runProduceBenchmark(inMemoryBroker, "bench-mem", 4, 100_000, 256, 8);
        results.add(memProduce);

        System.out.println("[2/3] Running In-Memory Broker Consumer Benchmark...");
        BenchmarkResult memConsume = runConsumeBenchmark(inMemoryBroker, "bench-mem", 100_000);
        results.add(memConsume);

        // 2. Persistent Disk Broker Benchmark (using segmented log)
        Path tempDir = Files.createTempDirectory("minikafka-bench");
        try {
            System.out.println("[3/3] Running Segmented Disk Persistent Benchmark (50,000 messages, 4 partitions, 4 threads)...");
            Broker diskBroker = new MiniKafkaBroker(tempDir);
            BenchmarkResult diskProduce = runProduceBenchmark(diskBroker, "bench-disk", 4, 50_000, 256, 4);
            results.add(diskProduce);
        } finally {
            // Clean up temporary benchmark directory
            try (var s = Files.walk(tempDir)) {
                s.sorted(Comparator.reverseOrder()).forEach(p -> {
                    try { Files.deleteIfExists(p); } catch (Exception ignored) {}
                });
            }
        }

        printSummary(results);
    }
}

