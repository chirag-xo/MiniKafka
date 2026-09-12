package com.minikafka.consumer;

import com.minikafka.broker.Broker;
import com.minikafka.broker.MiniKafkaBroker;
import com.minikafka.producer.MiniKafkaProducer;
import com.minikafka.producer.Producer;
import com.minikafka.producer.ProducerRecord;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

@DisplayName("Consumer Group End-to-End Integration Tests")
class ConsumerGroupIntegrationTest {

    private Broker broker;
    private Producer producer;

    @BeforeEach
    void setUp() {
        broker = new MiniKafkaBroker();
        broker.createTopic("orders", 4);
        producer = new MiniKafkaProducer(broker);
    }

    @AfterEach
    void tearDown() {
        producer.close();
    }

    @Test
    @DisplayName("Two consumers in the same group should evenly split topic partitions and avoid duplicate consumption")
    void shouldDistributePartitionsAcrossGroupConsumers() {
        String groupId = "order-processing-group";

        // Consumer 1 and Consumer 2 in the same group
        Consumer consumer1 = new MiniKafkaConsumer(broker, groupId);
        Consumer consumer2 = new MiniKafkaConsumer(broker, groupId);

        consumer1.subscribe(List.of("orders"));
        consumer2.subscribe(List.of("orders"));

        // Both consumers should be assigned exactly 2 partitions each
        assertThat(consumer1.assignment()).hasSize(2);
        assertThat(consumer2.assignment()).hasSize(2);
        assertThat(consumer1.assignment()).doesNotContainAnyElementsOf(consumer2.assignment());

        // Produce 40 messages across all 4 partitions
        int totalMessages = 40;
        for (int i = 0; i < totalMessages; i++) {
            producer.send(ProducerRecord.of("orders", "key-" + i, "order-" + i));
        }

        // Poll messages using both consumers
        List<ConsumerRecord> c1Records = new ArrayList<>();
        List<ConsumerRecord> c2Records = new ArrayList<>();

        for (int i = 0; i < 5; i++) {
            for (ConsumerRecord r : consumer1.poll(Duration.ofMillis(50))) {
                c1Records.add(r);
            }
            for (ConsumerRecord r : consumer2.poll(Duration.ofMillis(50))) {
                c2Records.add(r);
            }
        }

        // Both consumers processed messages from their respective partitions
        assertThat(c1Records).isNotEmpty();
        assertThat(c2Records).isNotEmpty();
        assertThat(c1Records.size() + c2Records.size()).isEqualTo(totalMessages);

        // Verify partition disjointness: C1 only consumed from its assigned partitions, C2 from its
        for (ConsumerRecord r : c1Records) {
            assertThat(consumer1.assignment()).contains(r.topicPartition());
        }
        for (ConsumerRecord r : c2Records) {
            assertThat(consumer2.assignment()).contains(r.topicPartition());
        }

        consumer1.close();
        consumer2.close();
    }

    @Test
    @DisplayName("Independent consumer groups should each consume 100% of topic records without interference")
    void shouldIndependentlyConsumeTopicAcrossDistinctGroups() {
        // Group 1: billing
        Consumer billingConsumer = new MiniKafkaConsumer(broker, "billing-service");
        billingConsumer.subscribe(List.of("orders"));

        // Group 2: analytics
        Consumer analyticsConsumer = new MiniKafkaConsumer(broker, "analytics-service");
        analyticsConsumer.subscribe(List.of("orders"));

        for (int i = 0; i < 10; i++) {
            producer.send(ProducerRecord.of("orders", "item-" + i));
        }

        List<ConsumerRecord> billingRecords = new ArrayList<>();
        List<ConsumerRecord> analyticsRecords = new ArrayList<>();

        for (ConsumerRecord r : billingConsumer.poll(Duration.ofMillis(100))) {
            billingRecords.add(r);
        }
        for (ConsumerRecord r : analyticsConsumer.poll(Duration.ofMillis(100))) {
            analyticsRecords.add(r);
        }

        assertThat(billingRecords).hasSize(10);
        assertThat(analyticsRecords).hasSize(10);

        billingConsumer.close();
        analyticsConsumer.close();
    }
}
