package com.minikafka.group;

import com.minikafka.consumer.TopicPartition;

import java.util.Set;

/**
 * Coordinates consumer group membership and partition assignments.
 */
public interface ConsumerGroupCoordinator {

    /**
     * Registers a consumer instance as an active member of a consumer group.
     * Triggers a partition rebalance.
     *
     * @param groupId The consumer group ID.
     * @param memberId The unique identifier of this consumer instance.
     * @param subscribedTopics The set of topics this member wants to consume.
     */
    void registerMember(String groupId, String memberId, Set<String> subscribedTopics);

    /**
     * Unregisters a consumer instance from a consumer group (graceful leave).
     * Triggers a partition rebalance among surviving members.
     *
     * @param groupId The consumer group ID.
     * @param memberId The consumer instance to unregister.
     */
    void unregisterMember(String groupId, String memberId);

    /**
     * @param groupId The consumer group ID.
     * @param memberId The consumer instance ID.
     * @return The currently assigned topic-partitions for this member.
     */
    Set<TopicPartition> getAssignment(String groupId, String memberId);

    /**
     * @param groupId The consumer group ID.
     * @return All active member IDs in the group.
     */
    Set<String> getMembers(String groupId);
}
