# MiniKafka — Design Decisions

This log records the main architectural choices I made while building MiniKafka, along with the alternatives I weighed and the trade-offs I accepted.

---

## 1. Java 21 with Zero Runtime Dependencies

- **Choice:** Standard Java 21 (`FileChannel`, `ReentrantReadWriteLock`, `HttpServer`, Virtual Threads) with Maven and test-only dependencies (JUnit 5, AssertJ).
- **Why:** Pulling in Netty, RocksDB, or Spring would hide the exact mechanics I wanted to build and understand—binary framing, NIO buffer management, locking, and crash recovery.
- **Trade-off:** More boilerplate around binary codecs and socket I/O, in exchange for full control and zero dependency bloat.

---

## 2. Single-Node Broker First

- **Choice:** Single-JVM broker without Raft, ZooKeeper, or multi-node replication.
- **Why:** Getting append-only logs, sparse indexing, torn-write recovery, backpressure, and consumer group offsets right on one node is foundational. Adding distributed consensus prematurely complicates debugging without improving single-node storage correctness.
- **Trade-off:** No multi-node failover or partition replication; the `Broker` interface keeps the door open for a networked cluster layer later.

---

## 3. Write-Ahead Log Ordering & Tail Truncation

- **Choice:** `LogSegment.append()` writes and `force(false)`s the record to `.log` *before* appending to `.index`. On startup, `LogSegment.recover()` scans from byte `0`, stops at the first invalid/incomplete record, truncates the tail, and rebuilds `.index`.
- **Why:** Treating `.log` as the single source of truth and `.index` as derived data guarantees a crash can never leave an index entry pointing past valid log data, and a corrupted `.index` is always self-healing on restart.
- **Trade-off:** Startup recovery scales linearly with active/unclean segment size rather than trusting `.index` blindly.

---

## 4. Atomic Temp-File Swap for Offsets

- **Choice:** `PersistentOffsetManager` writes committed offsets to `{topic}-{partition}.offset.tmp`, calls `FileChannel.force(true)`, and renames via `Files.move(..., ATOMIC_MOVE, REPLACE_EXISTING)`.
- **Why:** Overwriting an `.offset` file in place risks leaving a half-written file if the process crashes mid-write. Atomic rename ensures the broker either sees the old committed offset or the complete new one.
- **Trade-off:** Slightly more filesystem metadata overhead per commit than an append-only `__consumer_offsets` topic, but much simpler to inspect and recover per group.

---

## 5. Bounded Producer Queue with Explicit Overload Policies

- **Choice:** Combined an `ArrayBlockingQueue` and a `Semaphore` of `maxQueueCapacity` permits in `MiniKafkaProducer`, supporting `BLOCK_WITH_TIMEOUT`, `REJECT_IMMEDIATELY`, and `CALLER_RUNS`.
- **Why:** Unbounded async executors hide overload until the JVM runs out of memory. Bounding both queued and in-flight requests forces backpressure back onto fast callers and makes overload visible through rejection and utilization metrics.
- **Trade-off:** Callers must handle `ProducerBufferFullException` under sustained overload when using `BLOCK_WITH_TIMEOUT` or `REJECT_IMMEDIATELY`.

---

## 6. Virtual Threads & Synchronous Sockets vs. Selector Event Loops

- **Choice:** `BrokerServer` accepts standard `Socket` connections and dispatches each session to a Java 21 virtual thread with `SO_TIMEOUT` and a write watchdog, paired with bounded length-prefixed frames (`maxFrameSize = 4 MB`).
- **Why:** Virtual threads unmount from carrier threads on blocking socket I/O, giving clean imperative framing code without managing `Selector` state machines or partial-read ByteBuffer queues.
- **Trade-off:** Requires explicit read/write timeouts and strict `frameLen` bounds before allocation to prevent slow or malicious clients from stalling sockets or exhausting memory.

---

## 7. Deterministic Key-Affinity Hashing

- **Choice:** `(key.hashCode() & 0x7fffffff) % partitionCount` instead of importing MurmurHash2.
- **Why:** Preserves the core invariant—same key always maps to the same partition in FIFO order—without adding external hashing libraries.
- **Trade-off:** Distribution depends on `String.hashCode()` rather than Murmur2 uniformity.
