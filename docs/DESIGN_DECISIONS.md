# MiniKafka — Design Decision Log (ADR)

This document tracks major architectural decisions, the context in which they were made, alternatives considered, chosen approach, and trade-offs.

---

## ADR-001: Language Selection — Java 21 LTS
* **Status:** Accepted
* **Context:** MiniKafka is designed to prepare for SDE-1 backend interviews at top-tier product companies, emphasizing clean OOP, multithreading, concurrent data structures, and low-level I/O.
* **Alternatives Considered:**
  1. *Go:* Excellent for systems/networking, but Java provides more explicit exposure to memory models, JVM synchronization primitives, and OOP class hierarchies prevalent in enterprise backend interviews.
  2. *C++:* Maximum low-level control, but memory management overhead detracts from focusing on core broker and distributed log architecture concepts within interview prep timelines.
* **Chosen Approach:** Java 21 LTS using standard `java.util.concurrent`, Java NIO, and modern Java features (`record`, `sealed interfaces`, `Pattern Matching`).
* **Trade-offs:** Garbage collection pauses exist, but we avoid premature C-level micro-optimization and gain robust standard concurrency libraries.

---

## ADR-002: Build Tool — Apache Maven
* **Status:** Accepted
* **Context:** We need a standard, dependency-isolated build tool that compiles Java 21 and runs JUnit 5 test suites out-of-the-box.
* **Alternatives Considered:**
  1. *Gradle:* Faster incremental builds, but groovy/kotlin DSL adds cognitive overhead for a learner.
  2. *Raw javac scripts:* Zero dependencies, but cumbersome for managing JUnit 5 dependencies and test runners.
* **Chosen Approach:** Maven (`pom.xml`) with Maven Surefire Plugin and modern compiler plugins targeting Java 21.
* **Trade-offs:** XML configuration is verbose, but it is declarative, universal, and standard across enterprise Java teams.

---

## ADR-003: Single JVM In-Process Broker for Initial Phases
* **Status:** Accepted
* **Context:** Overengineering distributed systems (Raft, RPC, leader election) too early obscures core fundamentals like append-only logs, partition ordering, offset tracking, and consumer pull loops.
* **Alternatives Considered:**
  1. *Immediate Client-Server over TCP:* Increases debugging complexity (serialization, socket timeouts, framing) before domain logic is solid.
* **Chosen Approach:** Build the core broker, topic, partition, producer, and consumer abstractions in-memory within a single JVM first, then add persistence, concurrency stress tests, consumer groups, and networking progressively.
* **Trade-offs:** Does not demonstrate networking in Phase 1-2, but provides an isolated, highly testable environment for rock-solid core logic.

---

## ADR-004: Partitioning Strategy — Deterministic Key-Affinity vs. Kafka Murmur2
* **Status:** Accepted
* **Context:** Messages with identical keys must be routed deterministically to the same partition to guarantee chronological ordering.
* **Apache Kafka Reference:** Apache Kafka's production partitioner applies the 32-bit MurmurHash2 algorithm to the serialized byte array of the key: `Utils.toPositive(Utils.murmur2(keyBytes)) % numPartitions`.
* **MiniKafka Approach:** In MiniKafka, we deliberately use Java's `(key.hashCode() & 0x7fffffff) % numPartitions` as an educational **deterministic key-affinity hashing policy**, rather than claiming byte-for-byte fidelity with Apache Kafka's internal Murmur2 implementation.
* **Interview Defense Rationale:** Emphasizes that our system guarantees *the fundamental distributed invariant* (deterministic partition affinity and per-partition FIFO ordering) while keeping the implementation focused on core Java without external hashing libraries.

---

## ADR-005: Concurrency & Asynchronous Dispatch — Virtual Threads vs. True Non-Blocking I/O
* **Status:** Accepted
* **Context:** `sendAsync()` enables client callers to dispatch produce requests concurrently without halting the caller's main thread.
* **Crucial Engineering Distinction:**
  * **What Virtual Threads Do:** In Java 21, Virtual Threads are lightweight user-mode threads managed by the JVM runtime. When a virtual thread performs a blocking operation (such as waiting on a monitor lock, lock acquisition, or file I/O), the JVM unmounts the virtual thread from its underlying OS carrier thread. This makes concurrent blocking tasks *cheap to scale* without exhausting OS thread limits.
  * **What Virtual Threads Do NOT Do:** Virtual threads do **NOT** make the underlying operation non-blocking. The virtual thread itself is still suspended and awaiting completion. It is distinct from true asynchronous, non-blocking multiplexed I/O (e.g., Linux `epoll`, Java NIO `Selector`, Netty event loops).
* **Trade-offs:** Virtual threads allow writing straightforward, readable, synchronous-style code that scales to high concurrency, avoiding complex reactive callback spaghetti. However, we must accurately articulate the distinction between cheap thread suspension and reactive non-blocking I/O in technical interviews.

