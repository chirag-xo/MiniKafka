package com.minikafka.broker;

import com.minikafka.exception.TopicAlreadyExistsException;
import com.minikafka.exception.TopicNotFoundException;
import com.minikafka.model.Message;
import com.minikafka.partition.Partition;
import com.minikafka.topic.Topic;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

@DisplayName("MiniKafkaBroker Tests")
class MiniKafkaBrokerTest {

    private MiniKafkaBroker broker;

    @BeforeEach
    void setUp() {
        broker = new MiniKafkaBroker();
    }

    @Nested
    @DisplayName("Topic Lifecycle Management")
    class LifecycleTests {

        @Test
        @DisplayName("Should create and list topics successfully")
        void shouldCreateAndListTopics() {
            broker.createTopic("orders", 3);
            broker.createTopic("telemetry", 1);

            assertThat(broker.topicExists("orders")).isTrue();
            assertThat(broker.topicExists("telemetry")).isTrue();
            assertThat(broker.topicExists("nonexistent")).isFalse();

            assertThat(broker.listTopics()).containsExactlyInAnyOrder("orders", "telemetry");

            Topic orders = broker.getTopic("orders");
            assertThat(orders.getName()).isEqualTo("orders");
            assertThat(orders.getPartitionCount()).isEqualTo(3);
        }

        @Test
        @DisplayName("Should reject duplicate topic creation")
        void shouldRejectDuplicateTopic() {
            broker.createTopic("orders", 2);

            assertThatThrownBy(() -> broker.createTopic("orders", 4))
                    .isInstanceOf(TopicAlreadyExistsException.class)
                    .hasMessageContaining("Topic already exists: orders");
        }

        @Test
        @DisplayName("Should delete topic successfully")
        void shouldDeleteTopic() {
            broker.createTopic("temp", 1);
            assertThat(broker.topicExists("temp")).isTrue();

            broker.deleteTopic("temp");
            assertThat(broker.topicExists("temp")).isFalse();

            assertThatThrownBy(() -> broker.deleteTopic("temp"))
                    .isInstanceOf(TopicNotFoundException.class);
        }

        @Test
        @DisplayName("Should throw TopicNotFoundException for unknown topics")
        void shouldThrowForUnknownTopic() {
            assertThatThrownBy(() -> broker.getTopic("unknown"))
                    .isInstanceOf(TopicNotFoundException.class);
        }
    }

    @Nested
    @DisplayName("Message Routing & Offset Isolation")
    class RoutingTests {

        @Test
        @DisplayName("Should route appends to specified partition and isolate offset spaces")
        void shouldRouteAppendsToPartitions() {
            broker.createTopic("events", 2);

            long p0Off0 = broker.append("events", 0, Message.create("events", "k1", "e0"));
            long p0Off1 = broker.append("events", 0, Message.create("events", "k2", "e1"));
            long p1Off0 = broker.append("events", 1, Message.create("events", "k3", "e2"));

            assertThat(p0Off0).isEqualTo(0L);
            assertThat(p0Off1).isEqualTo(1L);
            assertThat(p1Off0).isEqualTo(0L); // Independent offset sequence for partition 1

            List<Message> p0Msgs = broker.read("events", 0, 0L, 10);
            List<Message> p1Msgs = broker.read("events", 1, 0L, 10);

            assertThat(p0Msgs).hasSize(2);
            assertThat(p1Msgs).hasSize(1);
            assertThat(p1Msgs.get(0).valueAsString()).isEqualTo("e2");
        }

        @Test
        @DisplayName("Should reject append/read on non-existent topic")
        void shouldRejectOperationsOnMissingTopic() {
            Message msg = Message.create("ghost", "k", "v");

            assertThatThrownBy(() -> broker.append("ghost", 0, msg))
                    .isInstanceOf(TopicNotFoundException.class);

            assertThatThrownBy(() -> broker.read("ghost", 0, 0L, 5))
                    .isInstanceOf(TopicNotFoundException.class);
        }
    }

    @Nested
    @DisplayName("Multi-Topic Multi-Partition Concurrency")
    class ConcurrencyTests {

        @Test
        @DisplayName("Concurrent writes across distinct topics and partitions must execute in parallel without corruption")
        void shouldHandleConcurrentMultiTopicWrites() throws InterruptedException {
            int topicsCount = 3;
            int partitionsPerTopic = 4;
            int messagesPerPartition = 50;

            for (int t = 0; t < topicsCount; t++) {
                broker.createTopic("topic-" + t, partitionsPerTopic);
            }

            int totalTasks = topicsCount * partitionsPerTopic;
            ExecutorService executor = Executors.newFixedThreadPool(12);
            CountDownLatch doneLatch = new CountDownLatch(totalTasks);

            for (int t = 0; t < topicsCount; t++) {
                final String topicName = "topic-" + t;
                for (int p = 0; p < partitionsPerTopic; p++) {
                    final int partitionId = p;
                    executor.submit(() -> {
                        try {
                            for (int m = 0; m < messagesPerPartition; m++) {
                                broker.append(topicName, partitionId,
                                        Message.create(topicName, "k-" + m, "val-" + m));
                            }
                        } finally {
                            doneLatch.countDown();
                        }
                    });
                }
            }

            boolean finished = doneLatch.await(10, TimeUnit.SECONDS);
            executor.shutdown();

            assertThat(finished).isTrue();

            // Verify each partition contains exactly messagesPerPartition with offsets 0..49
            for (int t = 0; t < topicsCount; t++) {
                String topicName = "topic-" + t;
                Topic topic = broker.getTopic(topicName);
                for (int p = 0; p < partitionsPerTopic; p++) {
                    Partition partition = topic.getPartition(p);
                    assertThat(partition.size()).isEqualTo(messagesPerPartition);
                    assertThat(partition.getNextOffset()).isEqualTo(messagesPerPartition);
                }
            }
        }
    }
}
