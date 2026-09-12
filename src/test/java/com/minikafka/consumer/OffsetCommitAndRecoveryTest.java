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
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

@DisplayName("Offset Commit & Crash Recovery Tests")
class OffsetCommitAndRecoveryTest {

    private Broker broker;
    private Producer producer;

    @BeforeEach
    void setUp() {
        broker = new MiniKafkaBroker();
        broker.createTopic("orders", 1);
        producer = new MiniKafkaProducer(broker);
    }

    @AfterEach
    void tearDown() {
        producer.close();
    }

    @Test
    @DisplayName("Consumer with no groupId should reject commitSync calls")
    void shouldRejectCommitWithoutGroupId() {
        Consumer consumer = new MiniKafkaConsumer(broker);
        consumer.assign(Set.of(new TopicPartition("orders", 0)));

        assertThatThrownBy(consumer::commitSync)
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("no groupId configured");

        consumer.close();
    }

    @Test
    @DisplayName("Crash Recovery: Replacement consumer with same groupId resumes from committed offset")
    void shouldRecoverAndResumeFromCommittedOffset() {
        String groupId = "order-processor-group";
        TopicPartition tp0 = new TopicPartition("orders", 0);

        // 1. Producer publishes 4 records (offsets 0, 1, 2, 3)
        for (int i = 0; i < 4; i++) {
            producer.send(ProducerRecord.of("orders", "order-" + i));
        }

        // 2. Consumer-A starts, assigns partition, polls all 4 records
        Consumer consumerA = new MiniKafkaConsumer(broker, groupId);
        consumerA.assign(Set.of(tp0));

        ConsumerRecords batchA = consumerA.poll(Duration.ofMillis(100));
        assertThat(batchA.count()).isEqualTo(4);

        // Consumer position is now 4 (next fetch offset)
        assertThat(consumerA.position(tp0)).isEqualTo(4L);

        // 3. Consumer-A commits offset 4 (indicating records 0..3 are processed)
        consumerA.commitSync();
        assertThat(consumerA.committed(tp0)).contains(4L);

        // 4. Consumer-A crashes!
        consumerA.close();

        // 5. Producer writes 2 new records (offsets 4 and 5)
        producer.send(ProducerRecord.of("orders", "order-4"));
        producer.send(ProducerRecord.of("orders", "order-5"));

        // 6. Consumer-B boots up with identical groupId
        Consumer consumerB = new MiniKafkaConsumer(broker, groupId);
        consumerB.assign(Set.of(tp0));

        // Its position should automatically be 4 from broker's offset manager!
        assertThat(consumerB.position(tp0)).isEqualTo(4L);

        // 7. Consumer-B polls: should receive ONLY orders 4 and 5, NOT re-reading 0..3!
        ConsumerRecords batchB = consumerB.poll(Duration.ofMillis(100));
        assertThat(batchB.count()).isEqualTo(2);

        List<ConsumerRecord> recordsB = batchB.records(tp0);
        assertThat(recordsB.get(0).offset()).isEqualTo(4L);
        assertThat(recordsB.get(0).valueAsString()).isEqualTo("order-4");
        assertThat(recordsB.get(1).offset()).isEqualTo(5L);
        assertThat(recordsB.get(1).valueAsString()).isEqualTo("order-5");

        consumerB.close();
    }

    @Test
    @DisplayName("At-Least-Once Delivery: Uncommitted records are re-processed upon restart")
    void shouldReconsumeUncommittedRecordsUponRestart() {
        String groupId = "at-least-once-group";
        TopicPartition tp0 = new TopicPartition("orders", 0);

        // 1. Producer writes 5 records (offsets 0, 1, 2, 3, 4)
        for (int i = 0; i < 5; i++) {
            producer.send(ProducerRecord.of("orders", "msg-" + i));
        }

        // 2. Consumer starts, polls all 5 messages
        Consumer consumerA = new MiniKafkaConsumer(broker, groupId);
        consumerA.assign(Set.of(tp0));
        ConsumerRecords batchA = consumerA.poll(Duration.ofMillis(100));
        assertThat(batchA.count()).isEqualTo(5);

        // 3. Suppose consumer processed messages 0 and 1 successfully, but crashes before processing 2..4.
        // It manually commits offset 2 (meaning messages 0 and 1 are done, 2 is next to process).
        consumerA.commitSync(Map.of(tp0, 2L));

        // Consumer crashes
        consumerA.close();

        // 4. Replacement consumer starts up
        Consumer consumerB = new MiniKafkaConsumer(broker, groupId);
        consumerB.assign(Set.of(tp0));

        // Automatically resumes from committed offset 2
        assertThat(consumerB.position(tp0)).isEqualTo(2L);

        // 5. Polling re-reads records 2, 3, and 4!
        ConsumerRecords batchB = consumerB.poll(Duration.ofMillis(100));
        assertThat(batchB.count()).isEqualTo(3);

        List<ConsumerRecord> recordsB = batchB.records(tp0);
        assertThat(recordsB.get(0).offset()).isEqualTo(2L);
        assertThat(recordsB.get(0).valueAsString()).isEqualTo("msg-2");
        assertThat(recordsB.get(1).offset()).isEqualTo(3L);
        assertThat(recordsB.get(1).valueAsString()).isEqualTo("msg-3");
        assertThat(recordsB.get(2).offset()).isEqualTo(4L);
        assertThat(recordsB.get(2).valueAsString()).isEqualTo("msg-4");

        consumerB.close();
    }
}
