package com.minikafka.producer;

import com.minikafka.broker.Broker;
import com.minikafka.model.Message;
import com.minikafka.topic.Topic;

import java.util.Objects;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

/**
 * Standard implementation of {@link Producer}.
 *
 * <p>Execution Workflow:
 * <ol>
 *   <li>Discover topic partition count from the {@link Broker}.</li>
 *   <li>Evaluate {@link Partitioner#partition} to assign target partition.</li>
 *   <li>Convert {@link ProducerRecord} into immutable domain {@link Message}.</li>
 *   <li>Append message to broker partition.</li>
 *   <li>Return {@link RecordMetadata} acknowledgement.</li>
 * </ol>
 */
public class MiniKafkaProducer implements Producer {

    private final Broker broker;
    private final Partitioner partitioner;
    private final ExecutorService asyncExecutor;
    private volatile boolean closed = false;

    public MiniKafkaProducer(Broker broker) {
        this(broker, new DefaultPartitioner());
    }

    public MiniKafkaProducer(Broker broker, Partitioner partitioner) {
        this.broker = Objects.requireNonNull(broker, "Broker cannot be null");
        this.partitioner = Objects.requireNonNull(partitioner, "Partitioner cannot be null");
        // Java 21 Virtual Threads for lightweight asynchronous dispatching
        this.asyncExecutor = Executors.newVirtualThreadPerTaskExecutor();
    }

    @Override
    public RecordMetadata send(ProducerRecord record) {
        ensureOpen();
        Objects.requireNonNull(record, "ProducerRecord cannot be null");

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

    @Override
    public CompletableFuture<RecordMetadata> sendAsync(ProducerRecord record) {
        ensureOpen();
        return CompletableFuture.supplyAsync(() -> send(record), asyncExecutor);
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
        asyncExecutor.shutdown();
        try {
            if (!asyncExecutor.awaitTermination(5, TimeUnit.SECONDS)) {
                asyncExecutor.shutdownNow();
            }
        } catch (InterruptedException e) {
            asyncExecutor.shutdownNow();
            Thread.currentThread().interrupt();
        }
    }
}
