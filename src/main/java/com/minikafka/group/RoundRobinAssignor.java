package com.minikafka.group;

import com.minikafka.consumer.TopicPartition;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Deterministic round-robin partition assignor.
 *
 * <p>Sorts both members and partitions to guarantee predictable, repeatable assignments:
 * <ul>
 *   <li>Distributes partitions evenly across active members.</li>
 *   <li>If members > partitions, surplus members receive 0 partitions (idle).</li>
 * </ul>
 */
public class RoundRobinAssignor implements PartitionAssignor {

    @Override
    public Map<String, Set<TopicPartition>> assign(Set<String> memberIds, Set<TopicPartition> partitions) {
        if (memberIds == null || memberIds.isEmpty()) {
            return Collections.emptyMap();
        }

        // Sort members deterministically
        List<String> sortedMembers = new ArrayList<>(memberIds);
        Collections.sort(sortedMembers);

        // Sort partitions by topic name, then partition ID
        List<TopicPartition> sortedPartitions = new ArrayList<>(partitions == null ? Collections.emptySet() : partitions);
        sortedPartitions.sort(Comparator.comparing(TopicPartition::topic).thenComparingInt(TopicPartition::partition));

        Map<String, Set<TopicPartition>> result = new LinkedHashMap<>();
        for (String member : sortedMembers) {
            result.put(member, new LinkedHashSet<>());
        }

        if (sortedPartitions.isEmpty()) {
            return Collections.unmodifiableMap(result);
        }

        // Round-robin distribution
        for (int i = 0; i < sortedPartitions.size(); i++) {
            String targetMember = sortedMembers.get(i % sortedMembers.size());
            result.get(targetMember).add(sortedPartitions.get(i));
        }

        Map<String, Set<TopicPartition>> finalResult = new LinkedHashMap<>();
        for (Map.Entry<String, Set<TopicPartition>> entry : result.entrySet()) {
            finalResult.put(entry.getKey(), Collections.unmodifiableSet(entry.getValue()));
        }

        return Collections.unmodifiableMap(finalResult);
    }
}
