package com.minikafka.network;

/**
 * Protocol operation codes for MiniKafka binary framed network requests and responses.
 */
public enum RequestType {
    PRODUCE((byte) 1),
    FETCH((byte) 2),
    CREATE_TOPIC((byte) 3),
    LIST_TOPICS((byte) 4),
    PING((byte) 5),
    ERROR((byte) 127);

    private final byte code;

    RequestType(byte code) {
        this.code = code;
    }

    public byte getCode() {
        return code;
    }

    public static RequestType fromCode(byte code) {
        for (RequestType type : values()) {
            if (type.code == code) {
                return type;
            }
        }
        throw new IllegalArgumentException("Unknown RequestType code: " + code);
    }
}
