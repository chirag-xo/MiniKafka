package com.minikafka.partition;

import com.minikafka.model.Message;

import java.util.List;

/**
 * Fundamental abstraction representing an ordered, append-only sequence of messages.
 *
 * <p>Contract Guarantees:
 * <ul>
 *   <li>Offsets are strictly monotonically increasing ($0, 1, 2, \dots$).</li>
 *   <li>Total ordering is preserved within a single partition.</li>
 *   <li>Past messages cannot be modified or reordered.</li>
 * </ul>
 */
public interface Partition {

    /**
     * @return The zero-based partition identifier.
     */
    int getId();

    /**
     * @return The topic this partition belongs to.
     */
    String getTopic();

    /**
     * Appends a message to the end of the partition log and assigns it the next monotonic offset.
     *
     * @param message The message to append.
     * @return The assigned monotonic offset ($Offset \ge 0$).
     */
    long append(Message message);

    /**
     * Reads a sequential batch of messages starting from {@code fromOffset}.
     *
     * @param fromOffset The offset of the first message to read (inclusive).
     * @param maxMessages The maximum number of messages to return ($maxMessages > 0$).
     * @return An unmodifiable list of messages ordered by ascending offset.
     * @throws com.minikafka.exception.OffsetOutOfRangeException if {@code fromOffset} is out of bounds.
     */
    List<Message> read(long fromOffset, int maxMessages);

    /**
     * @return The earliest offset available in this partition ($0L$ if empty).
     */
    long getEarliestOffset();

    /**
     * @return The offset that will be assigned to the next appended message (Log End Offset).
     */
    long getNextOffset();

    /**
     * @return The total number of messages stored in this partition.
     */
    long size();

    /**
     * @return True if the partition contains no messages.
     */
    boolean isEmpty();
}
