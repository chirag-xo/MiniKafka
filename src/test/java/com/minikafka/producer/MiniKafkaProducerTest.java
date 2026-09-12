package com.minikafka.producer;

import com.minikafka.broker.Broker;
import com.minikafka.broker.MiniKafkaBroker;
import com.minikafka.exception.TopicNotFoundException;
import com.minikafka.model.Message;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

@DisplayName("MiniKafkaProducer Tests")
class MiniKafkaProducerTest {

    private Broker broker;
    private MiniKafkaProducer producer;

    @BeforeEach
    void setUp() {
        broker = new MiniKafkaBroker();
        broker.createTopic("orders", 3);
        producer = new MiniKafkaProducer(broker);
    }

    @AfterEach
    void tearDown() {
        producer.close();
    }

    @Nested
    @DisplayName("Synchronous Send Operations")
    class SyncSendTests {

        @Test
        @DisplayName("Should synchronously publish record and return valid RecordMetadata")
        void shouldSendSynchronously() {
            ProducerRecord record = ProducerRecord.of("orders", "cust-42", "order-payload");

            RecordMetadata metadata = producer.send(record);

            assertThat(metadata.topic()).isEqualTo("orders");
            assertThat(metadata.partition()).isBetween(0, 2);
            assertThat(metadata.offset()).isEqualTo(0L);
            assertThat(metadata.timestamp()).isEqualTo(record.timestamp());

            // Verify stored in broker partition
            List<Message> read = broker.read("orders", metadata.partition(), 0L, 10);
            assertThat(read).hasSize(1);
            assertThat(read.get(0).key()).isEqualTo("cust-42");
            assertThat(read.get(0).valueAsString()).isEqualTo("order-payload");
            assertThat(read.get(0).offset()).isEqualTo(0L);
        }

        @Test
        @DisplayName("Should preserve key-partition affinity across multiple sequential sends")
        void shouldPreserveKeyPartitionAffinity() {
            String customerKey = "cust-premium-99";

            RecordMetadata md1 = producer.send(ProducerRecord.of("orders", customerKey, "item-1"));
            RecordMetadata md2 = producer.send(ProducerRecord.of("orders", customerKey, "item-2"));
            RecordMetadata md3 = producer.send(ProducerRecord.of("orders", customerKey, "item-3"));

            assertThat(md1.partition())
                    .isEqualTo(md2.partition())
                    .isEqualTo(md3.partition());

            assertThat(md1.offset()).isEqualTo(0L);
            assertThat(md2.offset()).isEqualTo(1L);
            assertThat(md3.offset()).isEqualTo(2L);
        }

        @Test
        @DisplayName("Should throw TopicNotFoundException when producing to unknown topic")
        void shouldThrowOnUnknownTopic() {
            ProducerRecord record = ProducerRecord.of("ghost-topic", "k", "v");

            assertThatThrownBy(() -> producer.send(record))
                    .isInstanceOf(TopicNotFoundException.class);
        }

        @Test
        @DisplayName("Should reject send after producer is closed")
        void shouldRejectSendWhenClosed() {
            producer.close();

            ProducerRecord record = ProducerRecord.of("orders", "k", "v");
            assertThatThrownBy(() -> producer.send(record))
                    .isInstanceOf(IllegalStateException.class)
                    .hasMessageContaining("closed");
        }
    }

    @Nested
    @DisplayName("Asynchronous Send Operations")
    class AsyncSendTests {

        @Test
        @DisplayName("Should asynchronously publish record via CompletableFuture")
        void shouldSendAsynchronously() throws Exception {
            ProducerRecord record = ProducerRecord.of("orders", "async-key", "async-value");

            CompletableFuture<RecordMetadata> future = producer.sendAsync(record);
            RecordMetadata metadata = future.get(3, TimeUnit.SECONDS);

            assertThat(metadata).isNotNull();
            assertThat(metadata.topic()).isEqualTo("orders");
            assertThat(metadata.offset()).isEqualTo(0L);
        }

        @Test
        @DisplayName("Should handle multiple parallel async sends without dropped records")
        void shouldHandleParallelAsyncSends() throws Exception {
            int count = 50;
            List<CompletableFuture<RecordMetadata>> futures = new ArrayList<>();

            for (int i = 0; i < count; i++) {
                futures.add(producer.sendAsync(ProducerRecord.of("orders", "item-" + i)));
            }

            CompletableFuture.allOf(futures.toArray(new CompletableFuture[0])).get(5, TimeUnit.SECONDS);

            // Sum records across all 3 partitions
            long totalRecords = 0;
            for (int p = 0; p < 3; p++) {
                totalRecords += broker.getTopic("orders").getPartition(p).size();
            }

            assertThat(totalRecords).isEqualTo(count);
        }
    }
}
