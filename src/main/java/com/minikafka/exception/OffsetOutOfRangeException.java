package com.minikafka.exception;

/**
 * Thrown when a consumer attempts to read from an offset that does not exist in the partition
 * (e.g. negative offset, or offset greater than or equal to nextOffset).
 */
public class OffsetOutOfRangeException extends MiniKafkaException {
    private final long requestedOffset;
    private final long earliestOffset;
    private final long latestOffset;

    public OffsetOutOfRangeException(String message, long requestedOffset, long earliestOffset, long latestOffset) {
        super(message);
        this.requestedOffset = requestedOffset;
        this.earliestOffset = earliestOffset;
        this.latestOffset = latestOffset;
    }

    public long getRequestedOffset() {
        return requestedOffset;
    }

    public long getEarliestOffset() {
        return earliestOffset;
    }

    public long getLatestOffset() {
        return latestOffset;
    }
}
