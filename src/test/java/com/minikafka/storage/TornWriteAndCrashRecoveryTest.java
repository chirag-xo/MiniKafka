package com.minikafka.storage;

import com.minikafka.broker.Broker;
import com.minikafka.broker.MiniKafkaBroker;
import com.minikafka.consumer.AutoOffsetReset;
import com.minikafka.consumer.Consumer;
import com.minikafka.consumer.ConsumerRecord;
import com.minikafka.consumer.ConsumerRecords;
import com.minikafka.consumer.MiniKafkaConsumer;
import com.minikafka.exception.CorruptRecordException;
import com.minikafka.model.Message;
import com.minikafka.producer.MiniKafkaProducer;
import com.minikafka.producer.Producer;
import com.minikafka.producer.ProducerRecord;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

@DisplayName("Torn-Write and Crash Recovery Tests")
class TornWriteAndCrashRecoveryTest {

    private static final String TOPIC = "crash-test-topic";
    private static final int PARTITION_ID = 0;

    @TempDir
    Path tempDir;

    private Path segment0Log() {
        return tempDir.resolve(TOPIC).resolve("partition-" + PARTITION_ID).resolve("00000000000000000000.log");
    }

    private Path segment0Index() {
        return tempDir.resolve(TOPIC).resolve("partition-" + PARTITION_ID).resolve("00000000000000000000.index");
    }

    private long appendValidRecords(PersistentPartition partition, int count, String prefix) {
        long lastOffset = -1;
        for (int i = 0; i < count; i++) {
            lastOffset = partition.append(Message.create(TOPIC, "key-" + i, prefix + i));
        }
        return lastOffset;
    }

    @Nested
    @DisplayName("1. Crash Halfway Through Writing a Record")
    class TornWriteTests {

        @Test
        @DisplayName("Should recover valid prefix when crash leaves partial 2-byte length header")
        void shouldRecoverFromPartialLengthHeader() throws IOException {
            long cleanSize;
            try (PersistentPartition partition = new PersistentPartition(TOPIC, PARTITION_ID, tempDir)) {
                appendValidRecords(partition, 5, "msg-");
                cleanSize = Files.size(segment0Log());
            }

            // Simulate crash mid-header: write only 2 bytes of a 4-byte length prefix
            try (FileChannel ch = FileChannel.open(segment0Log(), StandardOpenOption.WRITE, StandardOpenOption.APPEND)) {
                ch.write(ByteBuffer.wrap(new byte[]{0x00, 0x00}));
            }
            assertThat(Files.size(segment0Log())).isEqualTo(cleanSize + 2);

            // Reopen and verify recovery truncates torn bytes and keeps valid prefix
            try (PersistentPartition recovered = new PersistentPartition(TOPIC, PARTITION_ID, tempDir)) {
                assertThat(recovered.getNextOffset()).isEqualTo(5L);
                assertThat(Files.size(segment0Log())).isEqualTo(cleanSize);

                List<Message> messages = recovered.read(0, 10);
                assertThat(messages).hasSize(5);
                for (int i = 0; i < 5; i++) {
                    assertThat(messages.get(i).offset()).isEqualTo(i);
                    assertThat(messages.get(i).valueAsString()).isEqualTo("msg-" + i);
                }

                // Subsequent appends continue seamlessly at offset 5
                long nextOff = recovered.append(Message.create(TOPIC, "key-5", "msg-5"));
                assertThat(nextOff).isEqualTo(5L);
                assertThat(recovered.read(5, 1).get(0).valueAsString()).isEqualTo("msg-5");
            }
        }

        @Test
        @DisplayName("Should recover valid prefix when crash happens midway through record payload")
        void shouldRecoverFromPartialRecordBody() throws IOException {
            long cleanSize;
            try (PersistentPartition partition = new PersistentPartition(TOPIC, PARTITION_ID, tempDir)) {
                appendValidRecords(partition, 5, "payload-");
                cleanSize = Files.size(segment0Log());
            }

            // Simulate crash mid-record: write header claiming 120-byte body, but flush only 30 bytes
            try (FileChannel ch = FileChannel.open(segment0Log(), StandardOpenOption.WRITE, StandardOpenOption.APPEND)) {
                ByteBuffer partial = ByteBuffer.allocate(4 + 30);
                partial.putInt(120);
                partial.put(new byte[30]);
                partial.flip();
                ch.write(partial);
            }
            assertThat(Files.size(segment0Log())).isEqualTo(cleanSize + 34);

            try (PersistentPartition recovered = new PersistentPartition(TOPIC, PARTITION_ID, tempDir)) {
                assertThat(recovered.getNextOffset()).isEqualTo(5L);
                assertThat(Files.size(segment0Log())).isEqualTo(cleanSize);

                List<Message> messages = recovered.read(0, 10);
                assertThat(messages).hasSize(5);

                long newOffset = recovered.append(Message.create(TOPIC, "k-new", "recovered-write"));
                assertThat(newOffset).isEqualTo(5L);
                assertThat(recovered.read(5, 1).get(0).valueAsString()).isEqualTo("recovered-write");
            }
        }
    }

    @Nested
    @DisplayName("2. Corrupted Final Record")
    class CorruptedFinalRecordTests {

        @Test
        @DisplayName("Should reject and truncate final record when CRC32 checksum is corrupted")
        void shouldTruncateFinalRecordWithBadCrc() throws IOException {
            long sizeAfterFour;
            try (PersistentPartition partition = new PersistentPartition(TOPIC, PARTITION_ID, tempDir)) {
                appendValidRecords(partition, 4, "valid-");
                sizeAfterFour = Files.size(segment0Log());
                partition.append(Message.create(TOPIC, "key-4", "will-be-corrupted"));
            }

            long totalSize = Files.size(segment0Log());
            assertThat(totalSize).isGreaterThan(sizeAfterFour);

            // Corrupt the very last byte of the 5th record (breaks CRC32)
            try (FileChannel ch = FileChannel.open(segment0Log(), StandardOpenOption.READ, StandardOpenOption.WRITE)) {
                long lastBytePos = totalSize - 1;
                ByteBuffer oneByte = ByteBuffer.allocate(1);
                ch.read(oneByte, lastBytePos);
                oneByte.flip();
                byte flipped = (byte) (oneByte.get(0) ^ 0xFF);
                ch.write(ByteBuffer.wrap(new byte[]{flipped}), lastBytePos);
            }

            // Recovery should retain offsets 0..3 and truncate corrupted record 4
            try (PersistentPartition recovered = new PersistentPartition(TOPIC, PARTITION_ID, tempDir)) {
                assertThat(recovered.getNextOffset()).isEqualTo(4L);
                assertThat(Files.size(segment0Log())).isEqualTo(sizeAfterFour);

                List<Message> msgs = recovered.read(0, 10);
                assertThat(msgs).hasSize(4);
                assertThat(msgs.get(3).valueAsString()).isEqualTo("valid-3");

                // New append reuses offset 4 cleanly
                long reused = recovered.append(Message.create(TOPIC, "key-4", "clean-replacement"));
                assertThat(reused).isEqualTo(4L);
                assertThat(recovered.read(4, 1).get(0).valueAsString()).isEqualTo("clean-replacement");
            }
        }

        @Test
        @DisplayName("Should reject and truncate trailing garbage bytes with plausible length prefix")
        void shouldTruncatePlausibleLengthGarbageTail() throws IOException {
            long cleanSize;
            try (PersistentPartition partition = new PersistentPartition(TOPIC, PARTITION_ID, tempDir)) {
                appendValidRecords(partition, 3, "rec-");
                cleanSize = Files.size(segment0Log());
            }

            // Append a valid-looking 4B length prefix (50 bytes) followed by 50 random bytes (bad magic byte)
            try (FileChannel ch = FileChannel.open(segment0Log(), StandardOpenOption.WRITE, StandardOpenOption.APPEND)) {
                ByteBuffer garbage = ByteBuffer.allocate(4 + 50);
                garbage.putInt(50);
                byte[] noise = new byte[50];
                new Random(42L).nextBytes(noise);
                noise[0] = 0x01; // not MAGIC_BYTE (0x4B)
                garbage.put(noise);
                garbage.flip();
                ch.write(garbage);
            }

            try (PersistentPartition recovered = new PersistentPartition(TOPIC, PARTITION_ID, tempDir)) {
                assertThat(recovered.getNextOffset()).isEqualTo(3L);
                assertThat(Files.size(segment0Log())).isEqualTo(cleanSize);
                assertThat(recovered.read(0, 10)).hasSize(3);
            }
        }
    }

    @Nested
    @DisplayName("3. Truncated Log File")
    class TruncatedLogTests {

        @Test
        @DisplayName("Should recover all intact records when log file is externally truncated mid-record")
        void shouldRecoverPrefixAfterExternalTruncation() throws IOException {
            long sizeAfterFive;
            try (PersistentPartition partition = new PersistentPartition(TOPIC, PARTITION_ID, tempDir)) {
                appendValidRecords(partition, 5, "item-");
                sizeAfterFive = Files.size(segment0Log());
                partition.append(Message.create(TOPIC, "k5", "item-5-truncated"));
            }

            // Chop off the last 15 bytes from the 6th record
            long fullSize = Files.size(segment0Log());
            try (FileChannel ch = FileChannel.open(segment0Log(), StandardOpenOption.WRITE)) {
                ch.truncate(fullSize - 15);
            }

            try (PersistentPartition recovered = new PersistentPartition(TOPIC, PARTITION_ID, tempDir)) {
                assertThat(recovered.getNextOffset()).isEqualTo(5L);
                assertThat(Files.size(segment0Log())).isEqualTo(sizeAfterFive);

                List<Message> messages = recovered.read(0, 10);
                assertThat(messages).hasSize(5);
                assertThat(messages.get(4).valueAsString()).isEqualTo("item-4");
            }
        }
    }

    @Nested
    @DisplayName("4. Corrupted Sparse Index")
    class CorruptedSparseIndexTests {

        @Test
        @DisplayName("Should rebuild corrupted sparse index from log on restart")
        void shouldRebuildCorruptedSparseIndex() throws IOException {
            try (PersistentPartition partition = new PersistentPartition(TOPIC, PARTITION_ID, tempDir)) {
                appendValidRecords(partition, 25, "indexed-");
            }

            // Overwrite .index with random unaligned garbage (37 bytes)
            byte[] garbage = new byte[37];
            new Random(99L).nextBytes(garbage);
            Files.write(segment0Index(), garbage);

            // Reopen: recovery must repair the index and allow exact seeks across all strides
            try (PersistentPartition recovered = new PersistentPartition(TOPIC, PARTITION_ID, tempDir)) {
                assertThat(recovered.getNextOffset()).isEqualTo(25L);
                // 25 records with INDEX_STRIDE=8 -> entries at 0, 8, 16, 24 -> 4 entries * 8B = 32 bytes
                assertThat(Files.size(segment0Index())).isEqualTo(32L);

                assertThat(recovered.read(0, 1).get(0).valueAsString()).isEqualTo("indexed-0");
                assertThat(recovered.read(7, 1).get(0).valueAsString()).isEqualTo("indexed-7");
                assertThat(recovered.read(8, 1).get(0).valueAsString()).isEqualTo("indexed-8");
                assertThat(recovered.read(15, 1).get(0).valueAsString()).isEqualTo("indexed-15");
                assertThat(recovered.read(16, 1).get(0).valueAsString()).isEqualTo("indexed-16");
                assertThat(recovered.read(24, 1).get(0).valueAsString()).isEqualTo("indexed-24");
            }
        }

        @Test
        @DisplayName("Should recreate sparse index if .index file is completely missing")
        void shouldRecreateMissingSparseIndex() throws IOException {
            try (PersistentPartition partition = new PersistentPartition(TOPIC, PARTITION_ID, tempDir)) {
                appendValidRecords(partition, 20, "rec-");
            }

            Files.deleteIfExists(segment0Index());
            assertThat(segment0Index()).doesNotExist();

            try (PersistentPartition recovered = new PersistentPartition(TOPIC, PARTITION_ID, tempDir)) {
                assertThat(segment0Index()).exists();
                assertThat(Files.size(segment0Index())).isEqualTo(24L); // 0, 8, 16 -> 3 entries * 8B
                assertThat(recovered.read(12, 5)).hasSize(5);
                assertThat(recovered.read(12, 1).get(0).valueAsString()).isEqualTo("rec-12");
            }
        }

        @Test
        @DisplayName("Should reject corrupt record length during direct read")
        void shouldRejectCorruptRecordLengthOnRead() throws IOException {
            Path segDir = tempDir.resolve("direct-seg");
            Files.createDirectories(segDir);

            try (LogSegment seg = new LogSegment(segDir, 0L)) {
                seg.append(Message.create(TOPIC, "k", "hello").withPartitionAndOffset(PARTITION_ID, 0L), 0L);

                // Corrupt the 4-byte length prefix at byte 0 to -1 while segment is active
                Path logFile = segDir.resolve("00000000000000000000.log");
                try (FileChannel ch = FileChannel.open(logFile, StandardOpenOption.WRITE)) {
                    ch.write(ByteBuffer.allocate(4).putInt(-1).flip(), 0);
                }

                assertThatThrownBy(() -> seg.read(TOPIC, PARTITION_ID, 0L, 1))
                        .isInstanceOf(CorruptRecordException.class);
            }
        }
    }

    @Nested
    @DisplayName("5. Restart After Interrupted Segment Rollover")
    class InterruptedRolloverTests {

        @Test
        @DisplayName("Should recover cleanly when crash happens right after creating empty rolled segment and orphaned index")
        void shouldRecoverAfterInterruptedRolloverWithEmptySegmentAndOrphanIndex() throws IOException {
            long nextOffsetAfterRoll;
            // Use tiny maxSegmentBytes (256B) to force multiple real segment rollovers
            try (PersistentPartition partition = new PersistentPartition(TOPIC, PARTITION_ID, tempDir, 256L)) {
                appendValidRecords(partition, 15, "segmented-payload-");
                assertThat(partition.getSegmentCount()).isGreaterThan(1);
                nextOffsetAfterRoll = partition.getNextOffset();
            }

            Path partDir = tempDir.resolve(TOPIC).resolve("partition-" + PARTITION_ID);
            // Simulate interrupted rollover: empty next segment .log created, orphaned .index and .tmp left behind
            String nextPrefix = String.format("%020d", nextOffsetAfterRoll);
            String orphanPrefix = String.format("%020d", nextOffsetAfterRoll + 10);
            Files.createFile(partDir.resolve(nextPrefix + ".log"));
            Files.createFile(partDir.resolve(nextPrefix + ".index"));
            Files.createFile(partDir.resolve(orphanPrefix + ".index")); // orphaned index without .log
            Files.createFile(partDir.resolve("rollover.tmp"));

            try (PersistentPartition recovered = new PersistentPartition(TOPIC, PARTITION_ID, tempDir, 256L)) {
                assertThat(partDir.resolve(orphanPrefix + ".index")).doesNotExist();
                assertThat(partDir.resolve("rollover.tmp")).doesNotExist();
                assertThat(recovered.getNextOffset()).isEqualTo(nextOffsetAfterRoll);

                List<Message> all = recovered.read(0, 50);
                assertThat(all).hasSize(15);

                // Subsequent append goes into the rolled segment at exact nextOffsetAfterRoll
                long appendedOff = recovered.append(Message.create(TOPIC, "k-after", "after-rollover"));
                assertThat(appendedOff).isEqualTo(nextOffsetAfterRoll);
                assertThat(recovered.read(nextOffsetAfterRoll, 1).get(0).valueAsString()).isEqualTo("after-rollover");
            }
        }

        @Test
        @DisplayName("Should recover cleanly when torn write happened inside newly rolled segment")
        void shouldRecoverTornWriteInNewlyRolledSegment() throws IOException {
            long firstRolledBase;
            try (PersistentPartition partition = new PersistentPartition(TOPIC, PARTITION_ID, tempDir, 256L)) {
                appendValidRecords(partition, 12, "seg-msg-");
                assertThat(partition.getSegmentCount()).isGreaterThanOrEqualTo(2);
                firstRolledBase = partition.getNextOffset();
            }

            Path partDir = tempDir.resolve(TOPIC).resolve("partition-" + PARTITION_ID);
            // Force-create a brand new rolled segment with a torn half-written record
            String rolledName = String.format("%020d.log", firstRolledBase);
            Path rolledLog = partDir.resolve(rolledName);
            try (FileChannel ch = FileChannel.open(rolledLog, StandardOpenOption.CREATE, StandardOpenOption.WRITE)) {
                ByteBuffer torn = ByteBuffer.allocate(24);
                torn.putInt(200); // claims 200 bytes, only 20 present
                torn.put(new byte[20]);
                torn.flip();
                ch.write(torn);
            }

            try (PersistentPartition recovered = new PersistentPartition(TOPIC, PARTITION_ID, tempDir, 256L)) {
                assertThat(recovered.getNextOffset()).isEqualTo(firstRolledBase);
                assertThat(Files.size(rolledLog)).isZero();

                List<Message> existing = recovered.read(0, 50);
                assertThat(existing).hasSize(12);

                long off = recovered.append(Message.create(TOPIC, "k12", "clean-post-crash"));
                assertThat(off).isEqualTo(firstRolledBase);
                assertThat(recovered.read(firstRolledBase, 1).get(0).valueAsString()).isEqualTo("clean-post-crash");
            }
        }

        @Test
        @DisplayName("Should discard stale empty rolled segment if previous segment lost a torn record leaving an offset gap")
        void shouldDiscardEmptyRolledSegmentOnOffsetGap() throws IOException {
            try (PersistentPartition partition = new PersistentPartition(TOPIC, PARTITION_ID, tempDir, 1024L)) {
                appendValidRecords(partition, 5, "msg-");
            }

            // Corrupt final record (offset 4) in segment 0 so segment 0 recovers only offsets 0..3 (nextOffset = 4)
            long logSize = Files.size(segment0Log());
            try (FileChannel ch = FileChannel.open(segment0Log(), StandardOpenOption.WRITE)) {
                ch.truncate(logSize - 5);
            }

            // Meanwhile a stale empty segment 0000...0005.log sits in the directory
            Path partDir = tempDir.resolve(TOPIC).resolve("partition-" + PARTITION_ID);
            Path staleSeg = partDir.resolve("00000000000000000005.log");
            Files.createFile(staleSeg);

            try (PersistentPartition recovered = new PersistentPartition(TOPIC, PARTITION_ID, tempDir, 1024L)) {
                // Stale empty segment at baseOffset 5 must be discarded so nextOffset stays contiguous at 4
                assertThat(staleSeg).doesNotExist();
                assertThat(recovered.getNextOffset()).isEqualTo(4L);

                long next = recovered.append(Message.create(TOPIC, "k4", "contiguous"));
                assertThat(next).isEqualTo(4L);
            }
        }
    }

    @Nested
    @DisplayName("6. End-to-End Broker Crash & Recovery")
    class BrokerEndToEndCrashTests {

        @Test
        @DisplayName("Broker should recover valid log prefix, repair corrupt tail/index, and serve consumers cleanly")
        void shouldRecoverBrokerWithTornTailAndCorruptIndex() throws IOException {
            String topic = "payments";
            Broker broker1 = new MiniKafkaBroker(tempDir);
            broker1.createTopic(topic, 1);

            try (Producer producer = new MiniKafkaProducer(broker1)) {
                for (int i = 0; i < 10; i++) {
                    producer.send(ProducerRecord.of(topic, "tx-" + i, "amount-" + (i * 10)));
                }
            }

            // Simulate crash: append torn bytes to partition-0 log AND corrupt its sparse index
            Path logFile = tempDir.resolve(topic).resolve("partition-0/00000000000000000000.log");
            Path idxFile = tempDir.resolve(topic).resolve("partition-0/00000000000000000000.index");

            long cleanSize = Files.size(logFile);
            try (FileChannel ch = FileChannel.open(logFile, StandardOpenOption.WRITE, StandardOpenOption.APPEND)) {
                ByteBuffer torn = ByteBuffer.allocate(18);
                torn.putInt(500); // torn record body
                torn.put(new byte[14]);
                torn.flip();
                ch.write(torn);
            }
            Files.write(idxFile, new byte[]{1, 2, 3, 4, 5, 6, 7}); // corrupt index

            // Restart broker on the same directory
            Broker broker2 = new MiniKafkaBroker(tempDir);
            assertThat(Files.size(logFile)).isEqualTo(cleanSize);

            // Consumer polls all 10 valid messages
            try (Consumer consumer = new MiniKafkaConsumer(broker2, "ledger-group", AutoOffsetReset.EARLIEST)) {
                consumer.subscribe(List.of(topic));
                List<ConsumerRecord> polled = new ArrayList<>();
                ConsumerRecords batch = consumer.poll(Duration.ofMillis(200));
                for (ConsumerRecord r : batch) {
                    polled.add(r);
                }
                assertThat(polled).hasSize(10);
                assertThat(polled.get(9).offset()).isEqualTo(9L);
                assertThat(polled.get(9).valueAsString()).isEqualTo("amount-90");
                consumer.commitSync();
            }

            // Producer resumes appending cleanly at offset 10
            try (Producer producer2 = new MiniKafkaProducer(broker2)) {
                var meta = producer2.send(ProducerRecord.of(topic, "tx-10", "amount-100"));
                assertThat(meta.offset()).isEqualTo(10L);
            }
        }
    }
}
