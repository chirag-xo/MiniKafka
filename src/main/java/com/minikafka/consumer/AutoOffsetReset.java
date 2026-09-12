package com.minikafka.consumer;

/**
 * Policy defining initial consumer offset positioning when no previous offset exists.
 */
public enum AutoOffsetReset {
    /**
     * Automatically reset the offset to the earliest available message in the partition (offset 0).
     */
    EARLIEST,

    /**
     * Automatically reset the offset to the latest message (log end offset),
     * consuming only new messages published after subscription.
     */
    LATEST
}
