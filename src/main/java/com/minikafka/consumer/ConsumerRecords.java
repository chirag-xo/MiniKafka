package com.minikafka.consumer;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * A container of records returned by a single {@link Consumer#poll} operation,
 * partitioned by {@link TopicPartition}.
 *
 * <p>Implements {@link Iterable} allowing simple iteration:
 * <pre>{@code
 * for (ConsumerRecord record : records) {
 *     System.out.println(record.valueAsString());
 * }
 * }</pre>
 */
public class ConsumerRecords implements Iterable<ConsumerRecord> {

    private static final ConsumerRecords EMPTY = new ConsumerRecords(Collections.emptyMap());

    private final Map<TopicPartition, List<ConsumerRecord>> recordsByPartition;
    private final int totalCount;

    public ConsumerRecords(Map<TopicPartition, List<ConsumerRecord>> recordsByPartition) {
        if (recordsByPartition == null || recordsByPartition.isEmpty()) {
            this.recordsByPartition = Collections.emptyMap();
            this.totalCount = 0;
        } else {
            Map<TopicPartition, List<ConsumerRecord>> copy = new LinkedHashMap<>();
            int count = 0;
            for (Map.Entry<TopicPartition, List<ConsumerRecord>> entry : recordsByPartition.entrySet()) {
                List<ConsumerRecord> listCopy = Collections.unmodifiableList(new ArrayList<>(entry.getValue()));
                copy.put(entry.getKey(), listCopy);
                count += listCopy.size();
            }
            this.recordsByPartition = Collections.unmodifiableMap(copy);
            this.totalCount = count;
        }
    }

    public static ConsumerRecords empty() {
        return EMPTY;
    }

    /**
     * @return The records for the given partition, or an empty list if none exist.
     */
    public List<ConsumerRecord> records(TopicPartition partition) {
        return recordsByPartition.getOrDefault(partition, Collections.emptyList());
    }

    /**
     * @return The set of topic-partitions that have records in this batch.
     */
    public Set<TopicPartition> partitions() {
        return recordsByPartition.keySet();
    }

    /**
     * @return The total number of records across all partitions in this batch.
     */
    public int count() {
        return totalCount;
    }

    /**
     * @return True if no records were returned.
     */
    public boolean isEmpty() {
        return totalCount == 0;
    }

    @Override
    public Iterator<ConsumerRecord> iterator() {
        return new Iterator<>() {
            private final Iterator<List<ConsumerRecord>> partitionIterator = recordsByPartition.values().iterator();
            private Iterator<ConsumerRecord> currentRecordIterator = Collections.emptyIterator();

            @Override
            public boolean hasNext() {
                while (!currentRecordIterator.hasNext() && partitionIterator.hasNext()) {
                    currentRecordIterator = partitionIterator.next().iterator();
                }
                return currentRecordIterator.hasNext();
            }

            @Override
            public ConsumerRecord next() {
                if (!hasNext()) {
                    throw new java.util.NoSuchElementException();
                }
                return currentRecordIterator.next();
            }
        };
    }
}
