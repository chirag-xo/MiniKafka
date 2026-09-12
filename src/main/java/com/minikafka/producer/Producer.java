package com.minikafka.producer;

import java.util.concurrent.CompletableFuture;

/**
 * Client producer interface for publishing records to MiniKafka.
 */
public interface Producer extends AutoCloseable {

    /**
     * Synchronously publishes a record to a topic.
     * Blocks until the broker confirms persistence and assigns an offset.
     *
     * @param record The record to produce.
     * @return {@link RecordMetadata} containing topic, partition, and assigned offset.
     */
    RecordMetadata send(ProducerRecord record);

    /**
     * Asynchronously publishes a record to a topic.
     *
     * @param record The record to produce.
     * @return A {@link CompletableFuture} that resolves with the {@link RecordMetadata}.
     */
    CompletableFuture<RecordMetadata> sendAsync(ProducerRecord record);

    @Override
    void close();
}
