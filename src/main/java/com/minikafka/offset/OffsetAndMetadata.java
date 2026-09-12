package com.minikafka.offset;

import java.util.Objects;

/**
 * Encapsulates a committed offset along with metadata and timestamp.
 *
 * <p>Convention:
 * Committing offset {@code K} indicates that all records with offsets {@code < K}
 * have been processed, and the consumer will resume from offset {@code K}.
 *
 * @param offset The committed offset (next offset to fetch).
 * @param metadata Optional client-provided description (e.g., commit origin).
 * @param commitTimestamp Epoch millisecond when the commit occurred.
 */
public record OffsetAndMetadata(
        long offset,
        String metadata,
        long commitTimestamp
) {
    public OffsetAndMetadata {
        if (offset < 0) {
            throw new IllegalArgumentException("Committed offset must be non-negative: " + offset);
        }
        if (commitTimestamp <= 0) {
            throw new IllegalArgumentException("Commit timestamp must be positive");
        }
    }

    public static OffsetAndMetadata of(long offset) {
        return new OffsetAndMetadata(offset, "", System.currentTimeMillis());
    }

    public static OffsetAndMetadata of(long offset, String metadata) {
        return new OffsetAndMetadata(offset, Objects.requireNonNullElse(metadata, ""), System.currentTimeMillis());
    }
}
