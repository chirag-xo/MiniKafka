# MiniKafka — Technical Requirements Document (TRD)

## 1. System Architecture Overview
MiniKafka is structured as a layered, modular Java system designed to run on **Java 21 LTS**.

```
[ Producer ]                [ Consumer / Consumer Group ]
      │                                    ▲
      ▼                                    │
 ┌──────────────────────────────────────────────┐
 │               Broker Subsystem               │
 │                                              │
 │  ┌────────────────────────────────────────┐  │
 │  │ Topic Registry (ConcurrentHashMap)     │  │
 │  └────────────────────────────────────────┘  │
 │                        │                     │
 │          ┌─────────────┴─────────────┐       │
 │          ▼                           ▼       │
 │   ┌──────────────┐            ┌──────────────┐│
 │   │ Partition 0  │            │ Partition 1  ││
 │   │ (Offset Log) │            │ (Offset Log) ││
 │   └──────┬───────┘            └──────┬───────┘│
 │          │                           │        │
 └──────────┼───────────────────────────┼────────┘
            ▼                           ▼
     ┌──────────────┐            ┌──────────────┐
     │ Storage Log  │            │ Storage Log  │
     │ (.log/.index)│            │ (.log/.index)│
     └──────────────┘            └──────────────┘
```

---

## 2. Technology Choices & Justification

| Component | Technology | Rationale |
|---|---|---|
| **Language** | Java 21 LTS | Long-term support; modern features (records, pattern matching, sealed types, virtual threads, sequence collections). |
| **Build & Dependencies** | Apache Maven 3.9+ | Standard industry build tool; clean dependency isolation; reproducibility. |
| **Testing Framework** | JUnit 5 (Jupiter) + AssertJ | Industry standard for unit & concurrency testing with expressive assertions. |
| **Concurrency Primitives** | `java.util.concurrent` (`ReentrantReadWriteLock`, `AtomicLong`, `ConcurrentHashMap`) | Explicit synchronization mechanics that can be explained in depth during interviews. |
| **Storage Engine** | Java NIO (`FileChannel`, `ByteBuffer`) | Append-only sequential I/O, zero-copy buffer transfers, crash-resilient disk writes. |
| **Serialization** | Binary protocol / typed byte encoding | Clean binary packing without bloated JSON or third-party overhead. |

---

## 3. Data Model Specifications

### 3.1 Message Record
```java
public record Message(
    String id,              // Unique identifier (UUID or monotonic ID)
    String key,             // Optional routing key (nullable)
    byte[] value,           // Payload data
    long timestamp,         // Epoch millisecond timestamp
    String topic,           // Topic name
    int partition,          // Target partition index
    long offset             // Assigned monotonic sequence number
)
```
* **Immutability:** Implemented as a Java `record`. Once published and assigned an offset, messages are strictly read-only.
* **Storage format:** `[magic_byte: 1B][timestamp: 8B][offset: 8B][key_length: 4B][key_bytes][val_length: 4B][val_bytes][crc32: 4B]`.

### 3.2 Partition
* An ordered sequence of records where each record receives an offset $O \ge 0$.
* In-memory abstraction: Indexed collection backed by thread-safe append and binary search / direct index lookups.
* Storage abstraction: Append-only disk log with an index file mapping offsets to physical file positions.

---

## 4. Key Subsystem Interfaces (Planned)

### 4.1 Broker Interface
```java
public interface Broker {
    void createTopic(String topicName, int partitionCount);
    Topic getTopic(String topicName);
    long append(String topicName, int partition, Message message);
    List<Message> read(String topicName, int partition, long startOffset, int maxRecords);
}
```

### 4.2 Producer
* Responsible for routing messages to partitions:
  * If `key != null`: `Math.abs(key.hashCode()) % partitionCount`
  * If `key == null`: atomic round-robin counter `% partitionCount`
* Returns a `RecordMetadata(topic, partition, offset, timestamp)`.

### 4.3 Consumer
* Polls messages with signature: `poll(Duration timeout)`.
* Manages current partition offsets.
* Commits offsets either automatically on interval or explicitly via `commitSync()`.

---

## 5. Concurrency & Safety Contract
1. **Single-Writer / Multi-Reader Partition Model:**
   * Partition appends are synchronized per partition (via `ReentrantLock` or `synchronized`) to ensure atomic offset assignment and contiguous file writes.
   * Reads acquire read locks or operate against immutable views/channels to allow concurrent consumer polling without blocking producers.
2. **Atomic Offset Counter:**
   * Partition maintains next offset via `AtomicLong` or synchronized counter to eliminate race conditions between concurrent producers.

---

## 6. Definition of Done & Verification Strategy
* All components have $>85\%$ test coverage.
* Zero race conditions under multi-producer stress tests with 10+ concurrent threads.
* Clean recovery tests proving that restarting a broker reconstructs identical partition offset states.
