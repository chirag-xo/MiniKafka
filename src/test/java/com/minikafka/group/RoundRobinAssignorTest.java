package com.minikafka.group;

import com.minikafka.consumer.TopicPartition;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.util.Map;
import java.util.Set;

import static org.assertj.core.api.Assertions.assertThat;

@DisplayName("RoundRobinAssignor Tests")
class RoundRobinAssignorTest {

    private final RoundRobinAssignor assignor = new RoundRobinAssignor();

    @Test
    @DisplayName("Should divide partitions equally among consumers (4 partitions, 2 consumers)")
    void shouldDividePartitionsEqually() {
        Set<String> members = Set.of("consumer-A", "consumer-B");
        Set<TopicPartition> partitions = Set.of(
                new TopicPartition("orders", 0),
                new TopicPartition("orders", 1),
                new TopicPartition("orders", 2),
                new TopicPartition("orders", 3)
        );

        Map<String, Set<TopicPartition>> assignment = assignor.assign(members, partitions);

        assertThat(assignment.get("consumer-A")).hasSize(2);
        assertThat(assignment.get("consumer-B")).hasSize(2);

        // Disjoint sets: no partition assigned to more than one consumer
        assertThat(assignment.get("consumer-A"))
                .doesNotContainAnyElementsOf(assignment.get("consumer-B"));
    }

    @Test
    @DisplayName("Should assign surplus consumers zero partitions when consumers exceed partition count")
    void shouldLeaveExcessConsumersIdle() {
        Set<String> members = Set.of("c1", "c2", "c3");
        Set<TopicPartition> partitions = Set.of(
                new TopicPartition("orders", 0),
                new TopicPartition("orders", 1)
        );

        Map<String, Set<TopicPartition>> assignment = assignor.assign(members, partitions);

        int activeConsumers = 0;
        int idleConsumers = 0;

        for (Set<TopicPartition> assigned : assignment.values()) {
            if (assigned.isEmpty()) idleConsumers++;
            else activeConsumers++;
        }

        assertThat(activeConsumers).isEqualTo(2);
        assertThat(idleConsumers).isEqualTo(1); // 1 consumer remains idle!
    }

    @Test
    @DisplayName("Should return empty assignment when no members exist")
    void shouldHandleEmptyMembers() {
        Map<String, Set<TopicPartition>> assignment = assignor.assign(Set.of(), Set.of(new TopicPartition("t", 0)));
        assertThat(assignment).isEmpty();
    }
}
