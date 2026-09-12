package com.minikafka.exception;

/**
 * Thrown when a record read from disk fails integrity verification (e.g., CRC32 checksum mismatch).
 */
public class CorruptRecordException extends MiniKafkaException {
    public CorruptRecordException(String message) {
        super(message);
    }
}
