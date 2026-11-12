package com.minikafka.exception;

import com.minikafka.network.ResponseStatus;

/**
 * Thrown when a network frame violates the MiniKafka wire protocol (invalid version,
 * oversized frame, malformed payload, unknown opcode, or request ID mismatch).
 */
public class ProtocolException extends MiniKafkaException {

    private final ResponseStatus status;

    public ProtocolException(ResponseStatus status, String message) {
        super(message);
        this.status = status;
    }

    public ProtocolException(ResponseStatus status, String message, Throwable cause) {
        super(message, cause);
        this.status = status;
    }

    public ResponseStatus getStatus() {
        return status;
    }
}

