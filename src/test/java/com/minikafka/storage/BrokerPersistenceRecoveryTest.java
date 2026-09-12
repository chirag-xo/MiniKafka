package com.minikafka.storage;

import com.minikafka.broker.Broker;
import com.minikafka.broker.MiniKafkaBroker;
import com.minikafka.consumer.AutoOffsetReset;
import com.minikafka.consumer.Consumer;
import com.minikafka.consumer.ConsumerRecord;
import com.minikafka.consumer.ConsumerRecords;
import com.minikafka.consumer.MiniKafkaConsumer;
import com.minikafka.producer.MiniKafkaProducer;
import com.minikafka.producer.Producer;
import com.minikafka.producer.ProducerRecord;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

@DisplayName("Broker Full Persistence & Restart Recovery Tests")
class BrokerPersistenceRecoveryTest {

    @Test
    @DisplayName("Broker restart should discover existing topics and retain all partition logs on disk")
    void shouldRecoverFullBrokerStateOnRestart(@TempDir Path storageDir) {
        String topicName = "durable-events";

        // 1. Initial Broker session: create topic, produce 10 records across 2 partitions
        Broker broker1 = new MiniKafkaBroker(storageDir);
        broker1.createTopic(topicName, 2);

        try (Producer producer = new MiniKafkaProducer(broker1)) {
            for (int i = 0; i < 10; i++) {
                producer.send(ProducerRecord.of(topicName, "key-" + i, "data-" + i));
            }
        }

        // Simulate Broker shutdown
        broker1 = null;

        // 2. Restarted Broker session: new MiniKafkaBroker instance pointing to exact same storageDir
        Broker broker2 = new MiniKafkaBroker(storageDir);

        // Topic should be automatically discovered from directory structure!
        assertThat(broker2.topicExists(topicName)).isTrue();
        assertThat(broker2.getTopic(topicName).getPartitionCount()).isEqualTo(2);

        // 3. Consumer connects to the restarted broker and polls all 10 messages from disk
        try (Consumer consumer = new MiniKafkaConsumer(broker2, "audit-service", AutoOffsetReset.EARLIEST)) {
            consumer.subscribe(List.of(topicName));

            List<ConsumerRecord> polled = new ArrayList<>();
            long deadline = System.currentTimeMillis() + 3000;

            while (polled.size() < 10 && System.currentTimeMillis() < deadline) {
                ConsumerRecords batch = consumer.poll(Duration.ofMillis(100));
                for (ConsumerRecord r : batch) {
                    polled.add(r);
                }
            }

            assertThat(polled).hasSize(10);
            consumer.commitSync();
        }
    }
}
