package com.minikafka.storage;

import com.minikafka.model.Message;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

@DisplayName("PersistentPartition Tests")
class PersistentPartitionTest {

    @Test
    @DisplayName("Should persist messages sequentially and recover identical state after reopen")
    void shouldPersistAndRecoverStateAfterReopen(@TempDir Path tempDir) {
        String topic = "orders";
        int partitionId = 0;

        // 1. First session: open partition, append 3 messages, then close
        try (PersistentPartition partition = new PersistentPartition(topic, partitionId, tempDir)) {
            assertThat(partition.isEmpty()).isTrue();

            long off0 = partition.append(Message.create(topic, "k0", "msg-0"));
            long off1 = partition.append(Message.create(topic, "k1", "msg-1"));
            long off2 = partition.append(Message.create(topic, "k2", "msg-2"));

            assertThat(off0).isEqualTo(0L);
            assertThat(off1).isEqualTo(1L);
            assertThat(off2).isEqualTo(2L);
            assertThat(partition.size()).isEqualTo(3L);
            assertThat(partition.getNextOffset()).isEqualTo(3L);
        }

        // 2. Second session: re-open new PersistentPartition on the exact same directory (simulating restart)
        try (PersistentPartition recoveredPartition = new PersistentPartition(topic, partitionId, tempDir)) {
            assertThat(recoveredPartition.isEmpty()).isFalse();
            assertThat(recoveredPartition.size()).isEqualTo(3L);
            assertThat(recoveredPartition.getNextOffset()).isEqualTo(3L);

            // Read all 3 messages
            List<Message> messages = recoveredPartition.read(0L, 10);
            assertThat(messages).hasSize(3);

            assertThat(messages.get(0).offset()).isEqualTo(0L);
            assertThat(messages.get(0).key()).isEqualTo("k0");
            assertThat(messages.get(0).valueAsString()).isEqualTo("msg-0");

            assertThat(messages.get(1).offset()).isEqualTo(1L);
            assertThat(messages.get(1).key()).isEqualTo("k1");
            assertThat(messages.get(1).valueAsString()).isEqualTo("msg-1");

            assertThat(messages.get(2).offset()).isEqualTo(2L);
            assertThat(messages.get(2).key()).isEqualTo("k2");
            assertThat(messages.get(2).valueAsString()).isEqualTo("msg-2");

            // 3. Append another message: offset must continue monotonically at 3
            long off3 = recoveredPartition.append(Message.create(topic, "k3", "msg-3"));
            assertThat(off3).isEqualTo(3L);
            assertThat(recoveredPartition.size()).isEqualTo(4L);
            assertThat(recoveredPartition.getNextOffset()).isEqualTo(4L);
        }
    }
}
