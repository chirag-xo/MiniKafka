package com.minikafka.group;

import com.minikafka.broker.Broker;
import com.minikafka.broker.MiniKafkaBroker;
import com.minikafka.consumer.TopicPartition;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.util.Set;

import static org.assertj.core.api.Assertions.assertThat;

@DisplayName("ConsumerGroupCoordinator Tests")
class ConsumerGroupCoordinatorTest {

    private Broker broker;
    private ConsumerGroupCoordinator coordinator;

    @BeforeEach
    void setUp() {
        broker = new MiniKafkaBroker();
        broker.createTopic("telemetry", 4);
        coordinator = new InMemoryConsumerGroupCoordinator(broker);
    }

    @Test
    @DisplayName("Should dynamically rebalance partitions when members join and leave")
    void shouldRebalanceOnJoinAndLeave() {
        String groupId = "iot-service";

        // 1. Member 1 joins: should get all 4 partitions
        coordinator.registerMember(groupId, "member-1", Set.of("telemetry"));
        assertThat(coordinator.getMembers(groupId)).containsExactly("member-1");
        assertThat(coordinator.getAssignment(groupId, "member-1")).hasSize(4);

        // 2. Member 2 joins: partitions should rebalance to 2 partitions each
        coordinator.registerMember(groupId, "member-2", Set.of("telemetry"));
        assertThat(coordinator.getMembers(groupId)).containsExactlyInAnyOrder("member-1", "member-2");
        assertThat(coordinator.getAssignment(groupId, "member-1")).hasSize(2);
        assertThat(coordinator.getAssignment(groupId, "member-2")).hasSize(2);

        // Disjoint assignment verification
        assertThat(coordinator.getAssignment(groupId, "member-1"))
                .doesNotContainAnyElementsOf(coordinator.getAssignment(groupId, "member-2"));

        // 3. Member 2 leaves: partitions should rebalance back to member 1 having all 4
        coordinator.unregisterMember(groupId, "member-2");
        assertThat(coordinator.getMembers(groupId)).containsExactly("member-1");
        assertThat(coordinator.getAssignment(groupId, "member-1")).hasSize(4);
    }
}
