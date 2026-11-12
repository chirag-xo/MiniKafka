package com.minikafka.producer;

import com.minikafka.broker.Broker;
import com.minikafka.exception.ProducerBufferFullException;
import com.minikafka.metrics.BrokerMetrics;
import com.minikafka.model.Message;
import com.minikafka.topic.Topic;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.UUID;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Thread-safe implementation of {@link Producer} with bounded buffering and configurable backpressure.
 *
 * <p>Overload Management:
 * <ul>
 *   <li>All queued and in-flight requests are bounded by {@link ProducerConfig#maxQueueCapacity()}.</li>
 *   <li>Under overload, enforces the configured {@link BackpressurePolicy}:
 *       {@code BLOCK_WITH_TIMEOUT}, {@code REJECT_IMMEDIATELY}, or {@code CALLER_RUNS}.</li>
 *   <li>Exposes live queue utilization and rejection metrics via {@link #getMetrics()} and {@link BrokerMetrics}.</li>
 * </ul>
 */
public class MiniKafkaProducer implements Producer {

    private record QueuedProduceTask(ProducerRecord record, CompletableFuture<RecordMetadata> future) {}

    private final Broker broker;
    private final Partitioner partitioner;
    private final ProducerConfig config;
    private final ProducerMetrics producerMetrics;
    private final BrokerMetrics brokerMetrics;

    private final BlockingQueue<QueuedProduceTask> boundedQueue;
    private final Semaphore capacityPermits;
    private final AtomicInteger activeTasks = new AtomicInteger(0);
    private final Object drainLock = new Object();
    private final List<Thread> workers;

    private volatile boolean closed = false;
    private volatile boolean stopWorkers = false;

    public MiniKafkaProducer(Broker broker) {
        this(broker, new DefaultPartitioner(), ProducerConfig.defaults());
    }

    public MiniKafkaProducer(Broker broker, Partitioner partitioner) {
        this(broker, partitioner, ProducerConfig.defaults());
    }

    public MiniKafkaProducer(Broker broker, ProducerConfig config) {
        this(broker, new DefaultPartitioner(), config);
    }

    public MiniKafkaProducer(Broker broker, Partitioner partitioner, ProducerConfig config) {
        this.broker = Objects.requireNonNull(broker, "Broker cannot be null");
        this.partitioner = Objects.requireNonNull(partitioner, "Partitioner cannot be null");
        this.config = Objects.requireNonNull(config, "ProducerConfig cannot be null");

        this.producerMetrics = new ProducerMetrics(config.maxQueueCapacity());
        this.brokerMetrics = broker.getMetrics();
        if (this.brokerMetrics != null) {
            this.brokerMetrics.registerProducerCapacity(config.maxQueueCapacity());
        }

        this.boundedQueue = new ArrayBlockingQueue<>(config.maxQueueCapacity());
        this.capacityPermits = new Semaphore(config.maxQueueCapacity());

        this.workers = new ArrayList<>(config.workerThreads());
        for (int i = 0; i < config.workerThreads(); i++) {
            Thread worker = Thread.ofVirtual()
                    .name("minikafka-producer-worker-" + i)
                    .start(this::workerLoop);
            workers.add(worker);
        }
    }

    @Override
    public RecordMetadata send(ProducerRecord record) {
        ensureOpen();
        Objects.requireNonNull(record, "ProducerRecord cannot be null");

        SlotAcquisition acquisition = acquireSlot();
        if (acquisition == SlotAcquisition.CALLER_RUNS) {
            recordCallerRuns();
            RecordMetadata metadata = doSend(record);
            producerMetrics.recordCompleted();
            return metadata;
        }

        try {
            RecordMetadata metadata = doSend(record);
            producerMetrics.recordCompleted();
            return metadata;
        } finally {
            releaseSlot();
        }
    }

    @Override
    public CompletableFuture<RecordMetadata> sendAsync(ProducerRecord record) {
        ensureOpen();
        Objects.requireNonNull(record, "ProducerRecord cannot be null");

        SlotAcquisition acquisition;
        try {
            acquisition = acquireSlot();
        } catch (ProducerBufferFullException e) {
            return CompletableFuture.failedFuture(e);
        }

        if (acquisition == SlotAcquisition.CALLER_RUNS) {
            recordCallerRuns();
            try {
                RecordMetadata md = doSend(record);
                producerMetrics.recordCompleted();
                return CompletableFuture.completedFuture(md);
            } catch (Throwable t) {
                return CompletableFuture.failedFuture(t);
            }
        }

        CompletableFuture<RecordMetadata> future = new CompletableFuture<>();
        QueuedProduceTask task = new QueuedProduceTask(record, future);
        if (!boundedQueue.offer(task)) {
            // Defensive guard; permit count strictly bounds boundedQueue capacity.
            releaseSlot();
            ProducerBufferFullException ex = buildFullException(false);
            recordRejection(false);
            return CompletableFuture.failedFuture(ex);
        }
        return future;
    }

    private enum SlotAcquisition {
        ACQUIRED,
        CALLER_RUNS
    }

    private SlotAcquisition acquireSlot() {
        switch (config.backpressurePolicy()) {
            case REJECT_IMMEDIATELY -> {
                if (!capacityPermits.tryAcquire()) {
                    recordRejection(false);
                    throw buildFullException(false);
                }
                onSlotAcquired();
                return SlotAcquisition.ACQUIRED;
            }
            case BLOCK_WITH_TIMEOUT -> {
                boolean acquired;
                try {
                    long timeoutNanos = config.sendTimeout().toNanos();
                    acquired = capacityPermits.tryAcquire(timeoutNanos, TimeUnit.NANOSECONDS);
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    recordRejection(true);
                    throw new ProducerBufferFullException("Interrupted while waiting for producer queue capacity", e);
                }
                if (!acquired) {
                    recordRejection(true);
                    throw buildFullException(true);
                }
                onSlotAcquired();
                return SlotAcquisition.ACQUIRED;
            }
            case CALLER_RUNS -> {
                if (capacityPermits.tryAcquire()) {
                    onSlotAcquired();
                    return SlotAcquisition.ACQUIRED;
                }
                return SlotAcquisition.CALLER_RUNS;
            }
        }
        throw new IllegalStateException("Unhandled BackpressurePolicy: " + config.backpressurePolicy());
    }

    private void onSlotAcquired() {
        activeTasks.incrementAndGet();
        producerMetrics.recordEnqueued();
        if (brokerMetrics != null) {
            brokerMetrics.recordProducerQueueEnqueue();
        }
    }

    private void releaseSlot() {
        producerMetrics.recordDequeued();
        if (brokerMetrics != null) {
            brokerMetrics.recordProducerQueueDequeue();
        }
        capacityPermits.release();
        if (activeTasks.decrementAndGet() == 0) {
            synchronized (drainLock) {
                drainLock.notifyAll();
            }
        }
    }

    private void recordRejection(boolean timedOut) {
        producerMetrics.recordRejected(timedOut);
        if (brokerMetrics != null) {
            brokerMetrics.recordProducerRejected(timedOut);
        }
    }

    private void recordCallerRuns() {
        producerMetrics.recordCallerRuns();
        if (brokerMetrics != null) {
            brokerMetrics.recordProducerCallerRuns();
        }
    }

    private ProducerBufferFullException buildFullException(boolean timedOut) {
        if (timedOut) {
            return new ProducerBufferFullException(String.format(
                    "Producer queue full (capacity=%d); timed out after %d ms waiting for space",
                    config.maxQueueCapacity(), config.sendTimeout().toMillis()));
        }
        return new ProducerBufferFullException(String.format(
                "Producer queue full (capacity=%d); request rejected immediately",
                config.maxQueueCapacity()));
    }

    private void workerLoop() {
        while (!stopWorkers || !boundedQueue.isEmpty()) {
            QueuedProduceTask task;
            try {
                task = boundedQueue.take();
            } catch (InterruptedException e) {
                if (stopWorkers && boundedQueue.isEmpty()) {
                    break;
                }
                continue;
            }

            try {
                RecordMetadata metadata = doSend(task.record());
                producerMetrics.recordCompleted();
                task.future().complete(metadata);
            } catch (Throwable t) {
                task.future().completeExceptionally(t);
            } finally {
                releaseSlot();
            }
        }
    }

    private RecordMetadata doSend(ProducerRecord record) {
        Topic topic = broker.getTopic(record.topic());
        int partitionCount = topic.getPartitionCount();

        int targetPartition = partitioner.partition(
                record.topic(),
                record.key(),
                record.value(),
                partitionCount
        );

        Message domainMessage = new Message(
                UUID.randomUUID().toString(),
                record.key(),
                record.value(),
                record.timestamp(),
                record.topic(),
                targetPartition,
                Message.UNASSIGNED_OFFSET
        );

        long assignedOffset = broker.append(record.topic(), targetPartition, domainMessage);

        return new RecordMetadata(
                record.topic(),
                targetPartition,
                assignedOffset,
                record.timestamp()
        );
    }

    /**
     * Blocks until all currently queued and in-flight produce requests complete.
     */
    public void flush() {
        synchronized (drainLock) {
            while (activeTasks.get() > 0) {
                try {
                    drainLock.wait();
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    throw new IllegalStateException("Interrupted while flushing producer", e);
                }
            }
        }
    }

    public ProducerMetrics getMetrics() {
        return producerMetrics;
    }

    public ProducerConfig getConfig() {
        return config;
    }

    private void ensureOpen() {
        if (closed) {
            throw new IllegalStateException("Producer is closed and cannot accept new records");
        }
    }

    @Override
    public void close() {
        if (closed) return;
        closed = true;

        long deadline = System.currentTimeMillis() + 5000;
        synchronized (drainLock) {
            while (activeTasks.get() > 0) {
                long remaining = deadline - System.currentTimeMillis();
                if (remaining <= 0) break;
                try {
                    drainLock.wait(remaining);
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    break;
                }
            }
        }

        stopWorkers = true;
        for (Thread worker : workers) {
            worker.interrupt();
        }
        for (Thread worker : workers) {
            try {
                worker.join(1000);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                break;
            }
        }

        if (brokerMetrics != null) {
            brokerMetrics.unregisterProducerCapacity(config.maxQueueCapacity());
        }
    }
}
