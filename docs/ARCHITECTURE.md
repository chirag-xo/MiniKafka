# MiniKafka — System Architecture

## 1. Architectural Philosophy
MiniKafka adheres to three foundational tenets:
1. **Separation of Concerns:** Core domain logic (Partition, Topic, Message) does not know about network protocols or client SDK implementations.
2. **Log-Centric Storage:** Partitions are append-only commit logs. Mutation only happens at the tail. Historical records are immutable.
3. **Progressive Complexity:** In-memory abstractions first, disk persistence second, consumer coordination third, network RPC last.

---

## 2. Component Hierarchy

```
minikafka
├── model           # Immutable core domain entities (Message, RecordMetadata)
├── partition       # Append-only partition log abstraction & offset indexing
├── topic           # Topic container coordinating N partitions
├── broker          # Broker engine managing topic lifecycle & routing
├── producer        # Client publisher SDK & partitioning strategies (KeyHash, RoundRobin)
├── consumer        # Client consumer SDK, offset tracking & poll loops
├── storage         # Disk persistence (FileChannel, LogSegment, SegmentIndex)
└── group           # Consumer group coordination & partition assignment
```

---

## 3. Component Walkthrough

### 3.1 Partition: The Unit of Parallelism and Ordering
* A **Topic** is a logical namespace, but a **Partition** is the physical unit of scalability and ordering.
* Guarantees:
  * Total ordering is preserved *within* a partition.
  * No global ordering guarantee across different partitions of the same topic.
* Writes append to tail, assigning the next sequential monotonic `long offset`.
* Reads fetch a slice of messages starting from `startOffset` up to `maxRecords`.

### 3.2 Producer Subsystem
* Producer client accepts user data `(topic, key, value)`.
* Computes partition assignment via `Partitioner` strategy interface.
* Delivers payload to the designated partition in the broker.
* Acknowledges success by returning assigned `offset` and `partition`.

### 3.3 Consumer Subsystem
* Consumer tracks `currentOffset` per assigned partition.
* Pull-based architecture: Consumer explicitly calls `poll()`, specifying batch size.
* Separation of consumption and offset commit:
  * Processed messages can be committed.
  * Failed consumers can rewind or restart from the last committed offset.

---

## 4. Concurrency Model
```
Producers (Threads P1..Pn)
        │
        ▼ (Key Hash or Round Robin)
  ┌──────────────────────────────────────────────────────────┐
  │ Partition Lock (Guarantees atomic append + sequence ID)   │
  └──────────────────────────────────────────────────────────┘
        │
        ▼
   Tail Append (Memory List / Segment FileChannel)
        ▲
        │
Consumers (Threads C1..Cm read independently via ReadLock or immutable snapshots)
```

---

## 5. Storage Engine (Roadmap)
* Each partition maps to a folder: `data/<topic-name>/partition-<id>/`.
* Split into segments:
  * `00000000000000000000.log` (binary records)
  * `00000000000000000000.index` (sparse index: offset $\rightarrow$ physical byte position)
* Fast seeks: Binary search on sparse index $\rightarrow$ seek to byte offset in `.log` $\rightarrow$ linear scan of small chunk.
