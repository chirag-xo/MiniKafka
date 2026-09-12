# MiniKafka — Product Requirements Document (PRD)

## 1. Executive Summary & Vision
**MiniKafka** is a lightweight, Kafka-inspired distributed event-streaming and message broker system built from scratch in Java 21. 

The primary objective is educational rigor and interview defensibility for SDE-1 backend engineering roles: demonstrating mastery of object-oriented design, core Java, concurrent programming, low-level data structures, persistent storage I/O, failure recovery, and architectural trade-offs without hiding behind third-party abstractions.

---

## 2. Target Persona & Use Cases
* **Primary Persona:** Software engineers, hiring managers, and interviewers evaluating backend core competencies (systems programming, concurrency, clean architecture, storage engines).
* **Primary Use Case:** Producing and consuming structured event streams across topics and partitions with deterministic ordering, durable persistence, consumer offset tracking, and consumer group scaling.

---

## 3. Core Functional Requirements (Phased)

### Phase 1–2: In-Memory Core Domain & Broker
* Create topics dynamically with a configurable number of partitions ($N \ge 1$).
* Represent messages with attributes: unique ID, optional string key, payload, timestamp, topic, partition, and sequential offset.
* Enforce partition-level monotonic sequential ordering: $Offset_{n+1} = Offset_n + 1$.

### Phase 3–4: Producer & Consumer SDK
* **Producer:** Publish records to topics. Compute deterministic partition assignment via key hash (`hash(key) % partitionCount`) or round-robin if no key is provided.
* **Consumer:** Subscribe to topics and pull/poll messages starting from a specified offset (`EARLIEST`, `LATEST`, or a specific offset number).

### Phase 5: Offset Management & Replay
* Explicit offset tracking per consumer/consumer-group partition.
* Ability to commit offsets and replay historical messages from arbitrary offsets.

### Phase 6: Persistent Append-Only Log
* Replace purely in-memory partition stores with append-only file persistence.
* Guarantee that writes are flushed and recover cleanly on broker restarts.

### Phase 7: Consumer Groups & Load Balancing
* Group multiple consumers under a single `groupId`.
* Automatically or cooperatively divide partitions among active consumers in the group (e.g. 4 partitions across 2 consumers = 2 partitions each).

### Phase 8: Concurrency & Thread Safety
* Multiple producers publishing concurrently without data corruption or duplicate offsets.
* Non-blocking or lock-minimized reads for consumers while writers append.

### Phase 9: Crash Recovery & Fault Tolerance
* Recover broker and partition state accurately from the persistent log after simulated crashes/restarts.
* Verify at-least-once delivery semantics under crash conditions.

### Phase 10: Log Segmentation & Retention
* Segment partition logs by size threshold (e.g., roll file at $X$ MB).
* Time-based and size-based log retention cleanup policies.

### Phase 11–13: Networking, Observability & Benchmarks
* Lightweight TCP / HTTP client-server communication.
* Core metrics: publish/fetch throughput, P95/P99 latency, consumer lag.
* Benchmark harness evaluating high throughput under load.

---

## 4. Non-Functional Requirements
* **Determinism & Correctness:** Strict per-partition ordering ($O(1)$ append, $O(1)$ indexed reads). Zero silent data loss or duplicate offset assignment.
* **Performance:** Minimal locking on read paths; sequential I/O patterns for storage.
* **Simplicity & Readability:** Clean Java 21 idioms (records, virtual threads where suitable, typed concurrency primitives).
* **Interview Defensibility:** Every architectural choice must have an explicitly documented trade-off.

---

## 5. Explicit Non-Goals (Scope Boundaries)
To avoid over-engineering and unrealistic scope, MiniKafka deliberately will **NOT** implement:
* Multi-broker distributed clustering / Raft consensus / ZooKeeper / KRaft metadata quorums (runs as a single node / single JVM broker).
* Exactly-Once Semantics (EOS) two-phase commits (we target rock-solid **At-Least-Once**).
* Heavy frameworks (no Spring Boot, no external message broker libraries).
* Complex distributed transactions or consumer group rebalance protocols (e.g., full EBG / cooperative sticky rebalance protocols).
