package com.minikafka.offset;

import com.minikafka.consumer.TopicPartition;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.util.Map;
import java.util.Optional;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

@DisplayName("InMemoryOffsetManager Tests")
class InMemoryOffsetManagerTest {

    private InMemoryOffsetManager offsetManager;

    @BeforeEach
    void setUp() {
        offsetManager = new InMemoryOffsetManager();
    }

    @Test
    @DisplayName("Should store and retrieve committed offsets for topic-partition")
    void shouldStoreAndRetrieveCommittedOffsets() {
        TopicPartition tp = new TopicPartition("orders", 0);
        OffsetAndMetadata om = OffsetAndMetadata.of(42L, "worker-1");

        offsetManager.commitOffset("order-group", tp, om);

        Optional<OffsetAndMetadata> retrieved = offsetManager.getCommittedOffset("order-group", tp);
        assertThat(retrieved).isPresent();
        assertThat(retrieved.get().offset()).isEqualTo(42L);
        assertThat(retrieved.get().metadata()).isEqualTo("worker-1");
    }

    @Test
    @DisplayName("Should return empty Optional when no offset was committed")
    void shouldReturnEmptyWhenNotCommitted() {
        TopicPartition tp = new TopicPartition("orders", 0);
        Optional<OffsetAndMetadata> retrieved = offsetManager.getCommittedOffset("order-group", tp);
        assertThat(retrieved).isEmpty();
    }

    @Test
    @DisplayName("Should maintain complete isolation between different consumer groups")
    void shouldIsolateOffsetsAcrossGroups() {
        TopicPartition tp = new TopicPartition("orders", 0);

        offsetManager.commitOffset("group-A", tp, OffsetAndMetadata.of(10L));
        offsetManager.commitOffset("group-B", tp, OffsetAndMetadata.of(99L));

        assertThat(offsetManager.getCommittedOffset("group-A", tp).get().offset()).isEqualTo(10L);
        assertThat(offsetManager.getCommittedOffset("group-B", tp).get().offset()).isEqualTo(99L);
    }

    @Test
    @DisplayName("Should overwrite previous offset when advancing commit")
    void shouldOverwriteOnAdvanceCommit() {
        TopicPartition tp = new TopicPartition("orders", 0);

        offsetManager.commitOffset("group-A", tp, OffsetAndMetadata.of(10L));
        offsetManager.commitOffset("group-A", tp, OffsetAndMetadata.of(20L));

        assertThat(offsetManager.getCommittedOffset("group-A", tp).get().offset()).isEqualTo(20L);
    }

    @Test
    @DisplayName("Should reset offsets for a specific topic")
    void shouldResetTopicOffsets() {
        TopicPartition tpOrders = new TopicPartition("orders", 0);
        TopicPartition tpPayments = new TopicPartition("payments", 0);

        offsetManager.commitOffset("svc-group", tpOrders, OffsetAndMetadata.of(5L));
        offsetManager.commitOffset("svc-group", tpPayments, OffsetAndMetadata.of(12L));

        offsetManager.resetOffsets("svc-group", "orders");

        assertThat(offsetManager.getCommittedOffset("svc-group", tpOrders)).isEmpty();
        assertThat(offsetManager.getCommittedOffset("svc-group", tpPayments)).isPresent();
    }

    @Test
    @DisplayName("Should reject invalid groupId")
    void shouldRejectInvalidGroupId() {
        TopicPartition tp = new TopicPartition("orders", 0);
        assertThatThrownBy(() -> offsetManager.commitOffset(null, tp, OffsetAndMetadata.of(1L)))
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> offsetManager.commitOffset("   ", tp, OffsetAndMetadata.of(1L)))
                .isInstanceOf(IllegalArgumentException.class);
    }
}
