package com.minikafka.network;

/**
 * Standardized wire-protocol response status codes for MiniKafka TCP frames.
 */
public enum ResponseStatus {
    OK((byte) 0),
    BAD_REQUEST((byte) 1),
    UNSUPPORTED_VERSION((byte) 2),
    UNKNOWN_REQUEST_TYPE((byte) 3),
    TOPIC_NOT_FOUND((byte) 4),
    TOPIC_ALREADY_EXISTS((byte) 5),
    PARTITION_NOT_FOUND((byte) 6),
    OFFSET_OUT_OF_RANGE((byte) 7),
    FRAME_TOO_LARGE((byte) 8),
    INTERNAL_ERROR((byte) 9);

    private final byte code;

    ResponseStatus(byte code) {
        this.code = code;
    }

    public byte getCode() {
        return code;
    }

    public static ResponseStatus fromCode(byte code) {
        for (ResponseStatus status : values()) {
            if (status.code == code) {
                return status;
            }
        }
        return INTERNAL_ERROR;
    }
}

