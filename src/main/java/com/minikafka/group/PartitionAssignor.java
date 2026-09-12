package com.minikafka.group;

import com.minikafka.consumer.TopicPartition;

import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Strategy interface for assigning topic partitions to consumer group members.
 */
public interface PartitionAssignor {

    /**
     * Assigns partitions to consumers in a group.
     *
     * @param memberIds Set of active consumer member IDs in the group.
     * @param partitions Set of topic-partitions to distribute.
     * @return Map of member ID to assigned topic-partitions.
     */
    Map<String, Set<TopicPartition>> assign(Set<String> memberIds, Set<TopicPartition> partitions);
}
