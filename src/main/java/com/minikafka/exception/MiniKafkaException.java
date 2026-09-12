package com.minikafka.exception;

/**
 * Base unchecked exception for all MiniKafka runtime errors.
 */
public class MiniKafkaException extends RuntimeException {
    public MiniKafkaException(String message) {
        super(message);
    }

    public MiniKafkaException(String message, Throwable cause) {
        super(message, cause);
    }
}
