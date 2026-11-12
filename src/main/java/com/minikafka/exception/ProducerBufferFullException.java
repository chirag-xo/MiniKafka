package com.minikafka.exception;

/**
 * Thrown when a producer's bounded queue is full and cannot accept additional records
 * under the configured {@link com.minikafka.producer.BackpressurePolicy} or timeout.
 */
public class ProducerBufferFullException extends MiniKafkaException {

    public ProducerBufferFullException(String message) {
        super(message);
    }

    public ProducerBufferFullException(String message, Throwable cause) {
        super(message, cause);
    }
}

