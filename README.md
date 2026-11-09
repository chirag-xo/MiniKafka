# MiniKafka 🗂️

A **Kafka-inspired, append-only event-streaming broker** built from scratch in Java 21 — zero external runtime dependencies.

Built to demonstrate deep mastery of **concurrent systems programming**, **NIO storage engines**, **distributed system primitives**, and **clean OOP architecture** in Java.

---

## Architecture

```
┌───────────────────────────────────────────────────────────────────────┐
│                         MiniKafka Broker                              │
│                                                                       │
│  ┌──────────┐   ┌──────────┐   ┌──────────────────────────────────┐  │
│  │ Producer │   │ Consumer │   │       ConsumerGroupCoordinator   │  │
│  │   SDK    │   │   SDK    │   │  (RoundRobin partition assignor) │  │
│  └────┬─────┘   └────▲─────┘   └──────────────────────────────────┘  │
│       │   route      │  poll                                          │
│       ▼              │                                                │
│  ┌────────────────────────────────────────────────────────────────┐   │
│  │                         Topic                                  │   │
│  │   ┌──────────┐   ┌──────────┐   ┌──────────┐   ┌──────────┐  │   │
│  │   │Partition │   │Partition │   │Partition │   │Partition │  │   │
│  │   │    0     │   │    1     │   │    2     │   │    3     │  │   │
│  │   │ (RWLock) │   │ (RWLock) │   │ (RWLock) │   │ (RWLock) │  │   │
│  │   └─────┬────┘   └─────┬────┘   └─────┬────┘   └─────┬────┘  │   │
│  └─────────┼──────────────┼──────────────┼──────────────┼────────┘   │
│            │              │              │              │             │
│            ▼ FileChannel NIO (append-only binary log)                 │
│   data/topic/partition-N/{baseOffset}.log + {baseOffset}.index        │
└───────────────────────────────────────────────────────────────────────┘
```

**Component breakdown:**

| Package | Responsibility |
|---------|---------------|
| `model` | Immutable `Message` record — UUID, key, value, timestamp, topic, partition, offset |
| `partition` | `Partition` interface — append-only ordered log abstraction |
| `topic` | `Topic` — logical namespace coordinating N partitions |
| `broker` | `MiniKafkaBroker` — topic registry, routing, crash recovery |
| `producer` | `MiniKafkaProducer` — key-hash + round-robin partitioning, async dispatch |
| `consumer` | `MiniKafkaConsumer` — pull-based poll, seek, offset commit/replay |
| `storage` | `PersistentPartition`, `LogSegment`, `RecordSerializer` — NIO disk persistence |
| `offset` | `OffsetManager` — per-consumer-group committed offset tracking |
| `group` | `ConsumerGroupCoordinator`, `RoundRobinAssignor` — dynamic partition rebalancing |
| `metrics` | `BrokerMetrics` — atomic counters, P99 latency histogram, consumer lag |
| `network` | `BrokerServer`, `BrokerClient` — binary-framed TCP protocol layer |

---

## Features

- [x] **Append-only partition log** — strict per-partition FIFO ordering, monotonic offsets
- [x] **Producer SDK** — deterministic key-affinity hashing (`(key.hashCode() & 0x7fffffff) % partitions`) and round-robin fallback
- [x] **Async produce** — virtual-thread dispatch (`Thread.ofVirtual().start(...)`) for concurrent publishing
- [x] **Consumer SDK** — pull-based `poll(Duration)`, configurable `EARLIEST`/`LATEST` auto-offset-reset, `seek(offset)`
- [x] **Offset management** — per-consumer-group committed offset tracking; replay from any historical offset
- [x] **Persistent storage** — `FileChannel` NIO append-only binary log with CRC32 integrity verification
- [x] **Log segmentation** — rolling log segments with sparse offset indexes; size-based and time-based retention cleanup
- [x] **Crash recovery** — log replay on broker restart; at-least-once delivery semantics
- [x] **Consumer groups** — `RoundRobinAssignor` distributes partitions evenly; rebalances on member join/leave
- [x] **Concurrency** — `ReentrantReadWriteLock` per partition; concurrent producers never corrupt offsets
- [x] **Metrics & observability** — zero-dependency HTTP `/metrics` endpoint (JDK `HttpServer`), P99 latency histogram, consumer lag per partition
- [x] **TCP networking** — binary-framed length-prefixed protocol; `BrokerServer` accepts virtual-thread connections
- [x] **Benchmark harness** — throughput (msg/sec) and P50/P95/P99 latency measurements

---

## Quick Start

**Prerequisites:** Java 21+, Maven 3.9+

```bash
# Clone and run all 107 tests
git clone https://github.com/chirag-xo/MiniKafka.git
cd MiniKafka
mvn test
```

Expected output:
```
Tests run: 107, Failures: 0, Errors: 0, Skipped: 0
BUILD SUCCESS
```

**Run the TCP Broker Server & Benchmarks:**
```bash
# Compile project
mvn compile

# 1. Run the performance benchmark suite
java -cp target/classes com.minikafka.benchmark.BenchmarkRunner

# 2. Start the TCP Broker Server (port 9092)
java -cp target/classes com.minikafka.network.BrokerServer 9092 ./data
```

---

## Performance Benchmarks

Measured on Apple Silicon / Java 21 using `BenchmarkRunner`:

```
=========================================================================================================
                                     MiniKafka Performance Benchmarks                                   
=========================================================================================================
Workload     | Messages   | Payload  | Threads  | Duration     | Throughput     | P50 Lat    | P95 Lat    | P99 Lat   
-------------+------------+----------+----------+--------------+----------------+------------+------------+-----------
Produce (Mem)| 100,000    | 256 B    | 8        | 129 ms       | 770,444 msg/s  | 2.0 µs     | 26.0 µs    | 124.0 µs  
Consume (Mem)| 100,000    | 128 B    | 1        | 20 ms        | 4,870,832 msg/s| 0.0 µs     | 0.0 µs     | 0.0 µs    
Produce(Disk)| 50,000     | 256 B    | 4        | 137 s        | 365 msg/s      | 10.8 ms    | 19.0 ms    | 24.0 ms   
=========================================================================================================
```

*Note: Disk Produce runs with strict `FileChannel.force(false)` synchronous fsync per batch to guarantee zero data loss under crash.*

java -cp target/minikafka-0.1.0-SNAPSHOT.jar com.minikafka.benchmark.BenchmarkRunner
```

---

## In-JVM Usage Example

```java
// 1. Start broker (in-memory, no persistence)
Broker broker = new MiniKafkaBroker();
broker.createTopic("orders", 4);

// 2. Produce messages
Producer producer = new MiniKafkaProducer(broker);
producer.send(ProducerRecord.of("orders", "user-42", "order-placed"));
producer.sendAsync(ProducerRecord.of("orders", "user-99", "order-shipped")); // virtual thread

// 3. Consume messages (consumer group)
Consumer consumer = new MiniKafkaConsumer(broker, "billing-service");
consumer.subscribe(List.of("orders"));
ConsumerRecords records = consumer.poll(Duration.ofMillis(100));
consumer.commitSync();

// 4. With disk persistence + crash recovery
Broker persistent = new MiniKafkaBroker(Path.of("/var/data/minikafka"));
```

---

## Storage Engine

Each partition maps to a folder with rolling log segments:

```
data/
└── orders/
    └── partition-0/
        ├── 00000000000000000000.log    ← segment 0 (binary records)
        ├── 00000000000000000000.index  ← sparse offset → byte-position index
        ├── 00000000000001048576.log    ← segment 1 (rolled at MAX_SEGMENT_BYTES)
        └── 00000000000001048576.index
```

**Binary record format:**
```
[record_length: 4B] [magic: 1B = 0x4B] [crc32: 8B] [timestamp: 8B]
[offset: 8B] [id_length: 4B] [id_bytes] [key_length: 4B] [key_bytes]
[val_length: 4B] [val_bytes]
```

**Read path:** Binary search over segment list → seek via sparse index → linear scan within segment chunk.

---

## Concurrency Model

```
Producers (P1 .. Pn)              Consumers (C1 .. Cm)
       │                                    ▲
       ▼ writeLock.lock()                   │ readLock.lock()
  ┌──────────────────────┐                  │
  │  Tail append         │  ────────────────┘
  │  offset++ (atomic)   │   Concurrent readers never block
  │  fileChannel.force() │   Multiple partitions: fully parallel
  └──────────────────────┘
  
Virtual threads handle async produce & TCP connections (Java 21).
ReentrantReadWriteLock per partition: writers exclusive, readers concurrent.
```

---

## TCP Protocol

Binary length-prefixed framing over TCP (port 9092 by default):

```
[total_length: 4B] [request_type: 1B] [payload: variable]

Request types:
  0x01 PRODUCE      → [topic_len][topic][partition: 4B][key_len][key][val_len][val]
  0x02 FETCH        → [topic_len][topic][partition: 4B][from_offset: 8B][max: 4B]
  0x03 CREATE_TOPIC → [topic_len][topic][partition_count: 4B]
  0x04 LIST_TOPICS  → (no payload)
```

One virtual thread per accepted connection — scales to thousands of concurrent clients.

---

## Design Decisions

Five Architecture Decision Records documenting every trade-off:

| ADR | Decision |
|-----|---------|
| [ADR-001](docs/DESIGN_DECISIONS.md) | Java 21 LTS — explicit JVM concurrency primitives over Go/C++ |
| [ADR-002](docs/DESIGN_DECISIONS.md) | Apache Maven — standard, declarative, interview-universal |
| [ADR-003](docs/DESIGN_DECISIONS.md) | Single-JVM broker — solid domain logic before distributed complexity |
| [ADR-004](docs/DESIGN_DECISIONS.md) | Key-affinity hashing — `(key.hashCode() & 0x7fffffff) % N` vs. Murmur2 |
| [ADR-005](docs/DESIGN_DECISIONS.md) | Virtual threads — cheap concurrent blocking, not reactive non-blocking I/O |

---

## Explicit Non-Goals

MiniKafka deliberately excludes:
- **Multi-broker clustering** — no Raft, no ZooKeeper, no KRaft (single JVM)
- **Exactly-Once Semantics** — targets rock-solid **At-Least-Once** delivery
- **External frameworks** — no Spring Boot, no Netty, no message broker libraries
- **Full cooperative sticky rebalance** — round-robin group assignment only

---

## Project Structure

```
src/
├── main/java/com/minikafka/
│   ├── model/          Message.java (Java 21 record)
│   ├── partition/      Partition.java (interface), InMemoryPartition.java
│   ├── topic/          Topic.java
│   ├── broker/         Broker.java (interface), MiniKafkaBroker.java
│   ├── producer/       Producer.java, MiniKafkaProducer.java, Partitioner.java
│   ├── consumer/       Consumer.java, MiniKafkaConsumer.java
│   ├── storage/        PersistentPartition.java, LogSegment.java, RecordSerializer.java
│   ├── offset/         OffsetManager.java, InMemoryOffsetManager.java
│   ├── group/          ConsumerGroupCoordinator.java, RoundRobinAssignor.java
│   ├── metrics/        BrokerMetrics.java, MetricsServer.java
│   ├── network/        BrokerServer.java, BrokerClient.java, RequestType.java
│   ├── benchmark/      BenchmarkRunner.java
│   └── exception/      MiniKafkaException.java (and subtypes)
└── test/java/com/minikafka/
    ├── broker/         MiniKafkaBrokerTest.java
    ├── consumer/       MiniKafkaConsumerTest.java, ConsumerGroupIntegrationTest.java, ...
    ├── group/          ConsumerGroupCoordinatorTest.java, RoundRobinAssignorTest.java
    ├── partition/      InMemoryPartitionTest.java
    ├── producer/       MiniKafkaProducerTest.java, PartitionerTest.java
    ├── storage/        PersistentPartitionTest.java, BrokerPersistenceRecoveryTest.java, ...
    └── model/          MessageTest.java
```

---

## Skills Demonstrated

| Skill | Implementation |
|-------|---------------|
| **Core Java / OOP** | Clean interface hierarchy: `Partition`, `Broker`, `Consumer`, `Producer` |
| **Java 21 Features** | `record`, virtual threads, `sealed interfaces`, pattern matching |
| **Concurrency** | `ReentrantReadWriteLock`, `ConcurrentHashMap`, `AtomicLong`, virtual threads |
| **Java NIO / I/O** | `FileChannel`, `ByteBuffer`, `force(false)` write durability |
| **Binary Protocols** | Custom CRC32-verified binary record format; length-prefixed TCP framing |
| **Data Structures** | Sparse offset index, segment binary search, in-memory index |
| **System Design** | Producer/consumer pattern, partition parallelism, offset semantics |
| **Testing** | 70+ tests — unit, integration, concurrency stress (`@Nested`, AssertJ) |
| **Documentation** | PRD, Architecture doc, 5 ADRs — engineering maturity |
| **Fault Tolerance** | Crash recovery via log replay, at-least-once delivery |

---

*Built with Java 21, Apache Maven, JUnit 5, AssertJ — no external runtime dependencies.*

