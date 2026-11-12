package com.minikafka.producer;

/**
 * Defines how {@link MiniKafkaProducer} responds when its bounded internal queue reaches capacity.
 */
public enum BackpressurePolicy {

    /**
     * Blocks the calling thread up to {@link ProducerConfig#sendTimeout()} waiting for queue space.
     * If the timeout expires before a slot opens, fails with {@link com.minikafka.exception.ProducerBufferFullException}.
     */
    BLOCK_WITH_TIMEOUT,

    /**
     * Fails fast immediately when the queue is full by rejecting the record with
     * {@link com.minikafka.exception.ProducerBufferFullException}.
     */
    REJECT_IMMEDIATELY,

    /**
     * Applies direct caller-side backpressure by running the publish synchronously on the
     * calling thread when the queue is full, naturally throttling fast producers without dropping records.
     */
    CALLER_RUNS
}

