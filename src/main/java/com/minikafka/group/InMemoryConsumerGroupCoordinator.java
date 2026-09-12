package com.minikafka.group;

import com.minikafka.broker.Broker;
import com.minikafka.consumer.TopicPartition;
import com.minikafka.topic.Topic;

import java.util.Collections;
import java.util.HashSet;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Thread-safe coordinator managing consumer group lifecycles, membership, and partition rebalancing.
 */
public class InMemoryConsumerGroupCoordinator implements ConsumerGroupCoordinator {

    private final Broker broker;
    private final PartitionAssignor assignor;

    private static class GroupState {
        final Map<String, Set<String>> memberSubscriptions = new ConcurrentHashMap<>();
        final Map<String, Set<TopicPartition>> assignments = new ConcurrentHashMap<>();
    }

    private final ConcurrentHashMap<String, GroupState> groups = new ConcurrentHashMap<>();

    public InMemoryConsumerGroupCoordinator(Broker broker) {
        this(broker, new RoundRobinAssignor());
    }

    public InMemoryConsumerGroupCoordinator(Broker broker, PartitionAssignor assignor) {
        this.broker = Objects.requireNonNull(broker, "Broker cannot be null");
        this.assignor = Objects.requireNonNull(assignor, "Assignor cannot be null");
    }

    @Override
    public void registerMember(String groupId, String memberId, Set<String> subscribedTopics) {
        validateInputs(groupId, memberId);
        Objects.requireNonNull(subscribedTopics, "Subscribed topics cannot be null");

        GroupState state = groups.computeIfAbsent(groupId, g -> new GroupState());
        synchronized (state) {
            state.memberSubscriptions.put(memberId, new HashSet<>(subscribedTopics));
            rebalance(state);
        }
    }

    @Override
    public void unregisterMember(String groupId, String memberId) {
        validateInputs(groupId, memberId);

        GroupState state = groups.get(groupId);
        if (state == null) return;

        synchronized (state) {
            state.memberSubscriptions.remove(memberId);
            state.assignments.remove(memberId);

            if (state.memberSubscriptions.isEmpty()) {
                groups.remove(groupId);
            } else {
                rebalance(state);
            }
        }
    }

    @Override
    public Set<TopicPartition> getAssignment(String groupId, String memberId) {
        validateInputs(groupId, memberId);

        GroupState state = groups.get(groupId);
        if (state == null) {
            return Collections.emptySet();
        }

        Set<TopicPartition> assigned = state.assignments.get(memberId);
        if (assigned == null) {
            return Collections.emptySet();
        }
        return Collections.unmodifiableSet(assigned);
    }

    @Override
    public Set<String> getMembers(String groupId) {
        validateGroupId(groupId);
        GroupState state = groups.get(groupId);
        if (state == null) {
            return Collections.emptySet();
        }
        return Collections.unmodifiableSet(state.memberSubscriptions.keySet());
    }

    private void rebalance(GroupState state) {
        Set<TopicPartition> allPartitions = new HashSet<>();

        for (Set<String> topics : state.memberSubscriptions.values()) {
            for (String topicName : topics) {
                if (broker.topicExists(topicName)) {
                    Topic topic = broker.getTopic(topicName);
                    for (int p = 0; p < topic.getPartitionCount(); p++) {
                        allPartitions.add(new TopicPartition(topicName, p));
                    }
                }
            }
        }

        Map<String, Set<TopicPartition>> newAssignments = assignor.assign(
                state.memberSubscriptions.keySet(), allPartitions);

        state.assignments.clear();
        state.assignments.putAll(newAssignments);
    }

    private void validateInputs(String groupId, String memberId) {
        validateGroupId(groupId);
        if (memberId == null || memberId.isBlank()) {
            throw new IllegalArgumentException("MemberId cannot be null or blank");
        }
    }

    private void validateGroupId(String groupId) {
        if (groupId == null || groupId.isBlank()) {
            throw new IllegalArgumentException("GroupId cannot be null or blank");
        }
    }
}
