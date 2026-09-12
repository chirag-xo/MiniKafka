package com.minikafka.topic;

import com.minikafka.exception.PartitionNotFoundException;
import com.minikafka.model.Message;
import com.minikafka.partition.Partition;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

@DisplayName("Topic Tests")
class TopicTest {

    @Nested
    @DisplayName("Creation & Validation")
    class CreationTests {

        @Test
        @DisplayName("Should create topic with configured number of partitions")
        void shouldCreateTopicWithPartitions() {
            Topic topic = new Topic("payments", 3);

            assertThat(topic.getName()).isEqualTo("payments");
            assertThat(topic.getPartitionCount()).isEqualTo(3);
            assertThat(topic.getPartitions()).hasSize(3);

            for (int i = 0; i < 3; i++) {
                Partition partition = topic.getPartition(i);
                assertThat(partition.getId()).isEqualTo(i);
                assertThat(partition.getTopic()).isEqualTo("payments");
            }
        }

        @Test
        @DisplayName("Should reject invalid constructor arguments")
        void shouldRejectInvalidArguments() {
            assertThatThrownBy(() -> new Topic(null, 3))
                    .isInstanceOf(IllegalArgumentException.class);
            assertThatThrownBy(() -> new Topic("  ", 3))
                    .isInstanceOf(IllegalArgumentException.class);
            assertThatThrownBy(() -> new Topic("payments", 0))
                    .isInstanceOf(IllegalArgumentException.class)
                    .hasMessageContaining("at least 1 partition");
            assertThatThrownBy(() -> new Topic("payments", -1))
                    .isInstanceOf(IllegalArgumentException.class);
        }
    }

    @Nested
    @DisplayName("Partition Access & Routing")
    class PartitionAccessTests {

        @Test
        @DisplayName("Should throw PartitionNotFoundException when partition ID is out of range")
        void shouldThrowOnInvalidPartitionId() {
            Topic topic = new Topic("orders", 2);

            assertThatThrownBy(() -> topic.getPartition(-1))
                    .isInstanceOf(PartitionNotFoundException.class)
                    .hasMessageContaining("Partition -1 not found");

            assertThatThrownBy(() -> topic.getPartition(2))
                    .isInstanceOf(PartitionNotFoundException.class)
                    .hasMessageContaining("Partition 2 not found");
        }

        @Test
        @DisplayName("Should append and read messages through topic interface")
        void shouldAppendAndReadViaTopic() {
            Topic topic = new Topic("orders", 2);
            Message msg0 = Message.create("orders", "k0", "val-p0");
            Message msg1 = Message.create("orders", "k1", "val-p1");

            long off0 = topic.append(0, msg0);
            long off1 = topic.append(1, msg1);

            assertThat(off0).isEqualTo(0L);
            assertThat(off1).isEqualTo(0L); // first offset in partition 1 is also 0!

            List<Message> p0Messages = topic.read(0, 0L, 10);
            List<Message> p1Messages = topic.read(1, 0L, 10);

            assertThat(p0Messages).hasSize(1);
            assertThat(p0Messages.get(0).valueAsString()).isEqualTo("val-p0");

            assertThat(p1Messages).hasSize(1);
            assertThat(p1Messages.get(0).valueAsString()).isEqualTo("val-p1");
        }
    }
}
