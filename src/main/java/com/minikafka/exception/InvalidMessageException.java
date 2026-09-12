package com.minikafka.exception;

/**
 * Thrown when a message fails validation constraints (e.g. null topic, empty ID, negative offset).
 */
public class InvalidMessageException extends MiniKafkaException {
    public InvalidMessageException(String message) {
        super(message);
    }
}
