package com.minikafka.consumer;

import com.minikafka.broker.Broker;
import com.minikafka.broker.MiniKafkaBroker;
import com.minikafka.producer.MiniKafkaProducer;
import com.minikafka.producer.Producer;
import com.minikafka.producer.ProducerRecord;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

@DisplayName("MiniKafkaConsumer Tests")
class MiniKafkaConsumerTest {

    private Broker broker;
    private Producer producer;
    private MiniKafkaConsumer consumer;

    @BeforeEach
    void setUp() {
        broker = new MiniKafkaBroker();
        broker.createTopic("orders", 2);
        producer = new MiniKafkaProducer(broker);
        consumer = new MiniKafkaConsumer(broker, AutoOffsetReset.EARLIEST);
    }

    @AfterEach
    void tearDown() {
        producer.close();
        consumer.close();
    }

    @Nested
    @DisplayName("Partition Assignment & Subscription")
    class AssignmentTests {

        @Test
        @DisplayName("Should manually assign partitions to consumer")
        void shouldAssignPartitions() {
            TopicPartition tp0 = new TopicPartition("orders", 0);
            consumer.assign(Set.of(tp0));

            assertThat(consumer.assignment()).containsExactly(tp0);
        }

        @Test
        @DisplayName("Should expand topic subscription to all available partitions")
        void shouldSubscribeToTopic() {
            consumer.subscribe(List.of("orders"));

            assertThat(consumer.assignment()).containsExactlyInAnyOrder(
                    new TopicPartition("orders", 0),
                    new TopicPartition("orders", 1)
            );
        }
    }

    @Nested
    @DisplayName("Polling & Offset Advancement (EARLIEST Policy)")
    class PollingEarliestTests {

        @Test
        @DisplayName("Should poll existing messages and automatically advance consumer position")
        void shouldPollAndAdvancePosition() {
            TopicPartition tp0 = new TopicPartition("orders", 0);
            consumer.assign(Set.of(tp0));

            // Produce 3 messages directly to partition 0
            broker.append("orders", 0, com.minikafka.model.Message.create("orders", "k0", "m0"));
            broker.append("orders", 0, com.minikafka.model.Message.create("orders", "k1", "m1"));
            broker.append("orders", 0, com.minikafka.model.Message.create("orders", "k2", "m2"));

            assertThat(consumer.position(tp0)).isEqualTo(0L);

            ConsumerRecords batch = consumer.poll(Duration.ofMillis(100));

            assertThat(batch.count()).isEqualTo(3);
            List<ConsumerRecord> records = batch.records(tp0);
            assertThat(records.get(0).offset()).isEqualTo(0L);
            assertThat(records.get(0).valueAsString()).isEqualTo("m0");
            assertThat(records.get(1).offset()).isEqualTo(1L);
            assertThat(records.get(1).valueAsString()).isEqualTo("m1");
            assertThat(records.get(2).offset()).isEqualTo(2L);
            assertThat(records.get(2).valueAsString()).isEqualTo("m2");

            // Consumer position must have advanced to 3
            assertThat(consumer.position(tp0)).isEqualTo(3L);

            // Subsequent poll with no new messages returns empty
            ConsumerRecords emptyBatch = consumer.poll(Duration.ofMillis(50));
            assertThat(emptyBatch.isEmpty()).isTrue();
        }
    }

    @Nested
    @DisplayName("Polling (LATEST Policy)")
    class PollingLatestTests {

        @Test
        @DisplayName("Should skip pre-existing messages when AutoOffsetReset is LATEST")
        void shouldSkipOldMessagesWithLatestPolicy() {
            // Produce before consumer starts
            broker.append("orders", 0, com.minikafka.model.Message.create("orders", "k", "old-data"));

            MiniKafkaConsumer latestConsumer = new MiniKafkaConsumer(broker, AutoOffsetReset.LATEST);
            TopicPartition tp0 = new TopicPartition("orders", 0);
            latestConsumer.assign(Set.of(tp0));

            // Position initializes to end of log (offset 1)
            assertThat(latestConsumer.position(tp0)).isEqualTo(1L);

            ConsumerRecords batch1 = latestConsumer.poll(Duration.ofMillis(50));
            assertThat(batch1.isEmpty()).isTrue();

            // Produce new message
            broker.append("orders", 0, com.minikafka.model.Message.create("orders", "k", "new-data"));

            ConsumerRecords batch2 = latestConsumer.poll(Duration.ofMillis(50));
            assertThat(batch2.count()).isEqualTo(1);
            assertThat(batch2.records(tp0).get(0).valueAsString()).isEqualTo("new-data");

            latestConsumer.close();
        }
    }

    @Nested
    @DisplayName("Seeking & Message Replay")
    class SeekingTests {

        @Test
        @DisplayName("Should rewind offset using seek() to replay historical messages")
        void shouldSeekAndReplayMessages() {
            TopicPartition tp0 = new TopicPartition("orders", 0);
            consumer.assign(Set.of(tp0));

            broker.append("orders", 0, com.minikafka.model.Message.create("orders", "k", "first"));
            broker.append("orders", 0, com.minikafka.model.Message.create("orders", "k", "second"));

            ConsumerRecords firstPass = consumer.poll(Duration.ofMillis(50));
            assertThat(firstPass.count()).isEqualTo(2);
            assertThat(consumer.position(tp0)).isEqualTo(2L);

            // Seek back to offset 0 (Replay!)
            consumer.seek(tp0, 0L);
            assertThat(consumer.position(tp0)).isEqualTo(0L);

            ConsumerRecords replayPass = consumer.poll(Duration.ofMillis(50));
            assertThat(replayPass.count()).isEqualTo(2);
            assertThat(replayPass.records(tp0).get(0).valueAsString()).isEqualTo("first");
            assertThat(replayPass.records(tp0).get(1).valueAsString()).isEqualTo("second");
        }

        @Test
        @DisplayName("Should seekToBeginning and seekToEnd across partitions")
        void shouldSeekToBeginningAndEnd() {
            TopicPartition tp0 = new TopicPartition("orders", 0);
            consumer.assign(Set.of(tp0));

            broker.append("orders", 0, com.minikafka.model.Message.create("orders", "k", "A"));
            broker.append("orders", 0, com.minikafka.model.Message.create("orders", "k", "B"));

            consumer.seekToEnd(List.of(tp0));
            assertThat(consumer.position(tp0)).isEqualTo(2L);

            consumer.seekToBeginning(List.of(tp0));
            assertThat(consumer.position(tp0)).isEqualTo(0L);
        }
    }

    @Nested
    @DisplayName("End-to-End Producer -> Broker -> Consumer Flow")
    class EndToEndTests {

        @Test
        @DisplayName("Full pipeline: Producer publishes keyed records, Consumer polls across partitions")
        void shouldRunFullPipelineSuccessfully() {
            consumer.subscribe(List.of("orders"));

            int messageCount = 20;
            for (int i = 0; i < messageCount; i++) {
                producer.send(ProducerRecord.of("orders", "user-" + (i % 4), "order-" + i));
            }

            List<ConsumerRecord> received = new ArrayList<>();
            long deadline = System.currentTimeMillis() + 3000;

            while (received.size() < messageCount && System.currentTimeMillis() < deadline) {
                ConsumerRecords batch = consumer.poll(Duration.ofMillis(100));
                for (ConsumerRecord record : batch) {
                    received.add(record);
                }
            }

            assertThat(received).hasSize(messageCount);

            // Verify both partitions 0 and 1 received records
            long p0Count = received.stream().filter(r -> r.partition() == 0).count();
            long p1Count = received.stream().filter(r -> r.partition() == 1).count();

            assertThat(p0Count).isPositive();
            assertThat(p1Count).isPositive();
            assertThat(p0Count + p1Count).isEqualTo(messageCount);
        }
    }
}
