package com.minikafka.storage;

import com.minikafka.exception.CorruptRecordException;
import com.minikafka.model.Message;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.zip.CRC32;

/**
 * Binary serializer and deserializer for disk-persisted {@link Message} records.
 *
 * <p>Binary Protocol Specification:
 * <pre>
 * [record_length: 4B] Total length of body following this field
 * [magic: 1B]         Protocol magic identifier (0x4B / 'K')
 * [crc32: 8B]         CRC32 checksum of all subsequent payload fields
 * [timestamp: 8B]     Record creation timestamp
 * [offset: 8B]        Assigned monotonic partition offset
 * [id_length: 4B]     Byte length of message ID string
 * [id_bytes]          UTF-8 message ID bytes
 * [key_length: 4B]    Key byte length (-1 if key is null)
 * [key_bytes]         UTF-8 key bytes (omitted if key is null)
 * [val_length: 4B]    Payload byte length
 * [val_bytes]         Raw payload bytes
 * </pre>
 */
public final class RecordSerializer {

    public static final byte MAGIC_BYTE = (byte) 0x4B; // 'K' for Kafka

    private RecordSerializer() {}

    /**
     * Serializes a message into a ByteBuffer ready to be written to a FileChannel.
     */
    public static ByteBuffer serialize(Message message) {
        byte[] idBytes = message.id().getBytes(StandardCharsets.UTF_8);
        byte[] keyBytes = (message.key() == null) ? null : message.key().getBytes(StandardCharsets.UTF_8);
        byte[] valBytes = message.value();

        int keyBytesLen = (keyBytes == null) ? 0 : keyBytes.length;
        int payloadBodyLength = 8 + 8 // timestamp (8B) + offset (8B)
                + 4 + idBytes.length   // id length + id
                + 4 + keyBytesLen      // key length + key
                + 4 + valBytes.length; // value length + value

        // Total body = magic (1B) + crc32 (8B) + payloadBodyLength
        int totalBodyLength = 1 + 8 + payloadBodyLength;

        // Compute CRC32 over the payload body
        CRC32 crc32 = new CRC32();
        ByteBuffer crcBuffer = ByteBuffer.allocate(payloadBodyLength);
        crcBuffer.putLong(message.timestamp());
        crcBuffer.putLong(message.offset());
        crcBuffer.putInt(idBytes.length);
        crcBuffer.put(idBytes);
        crcBuffer.putInt(keyBytes == null ? -1 : keyBytes.length);
        if (keyBytes != null) {
            crcBuffer.put(keyBytes);
        }
        crcBuffer.putInt(valBytes.length);
        crcBuffer.put(valBytes);
        crc32.update(crcBuffer.array(), 0, payloadBodyLength);
        long checksum = crc32.getValue();

        // Allocate full buffer: length prefix (4B) + totalBodyLength
        ByteBuffer recordBuffer = ByteBuffer.allocate(4 + totalBodyLength);
        recordBuffer.putInt(totalBodyLength);
        recordBuffer.put(MAGIC_BYTE);
        recordBuffer.putLong(checksum);
        recordBuffer.put(crcBuffer.array(), 0, payloadBodyLength);

        recordBuffer.flip();
        return recordBuffer;
    }

    /**
     * Deserializes a message from a ByteBuffer.
     *
     * @param buffer ByteBuffer positioned at the start of a record.
     * @param topic Target topic name.
     * @param partition Target partition index.
     * @return Deserialized {@link Message}.
     * @throws CorruptRecordException if magic byte or CRC32 checksum fails.
     */
    public static Message deserialize(ByteBuffer buffer, String topic, int partition) {
        int totalBodyLength = buffer.getInt();
        if (totalBodyLength < 1 + 8 + 8 + 8 + 4 + 4 + 4) {
            throw new CorruptRecordException("Corrupt record: body length too short: " + totalBodyLength);
        }

        byte magic = buffer.get();
        if (magic != MAGIC_BYTE) {
            throw new CorruptRecordException(String.format(
                    "Magic byte mismatch: expected 0x%02X, got 0x%02X", MAGIC_BYTE, magic));
        }

        long expectedCrc = buffer.getLong();

        // The remaining payload body
        int payloadBodyLength = totalBodyLength - 1 - 8;
        byte[] payloadBytes = new byte[payloadBodyLength];
        buffer.get(payloadBytes);

        // Verify CRC32
        CRC32 crc32 = new CRC32();
        crc32.update(payloadBytes, 0, payloadBodyLength);
        long computedCrc = crc32.getValue();

        if (computedCrc != expectedCrc) {
            throw new CorruptRecordException(String.format(
                    "CRC32 mismatch: expected %d, computed %d", expectedCrc, computedCrc));
        }

        // Parse payload body
        ByteBuffer payloadBuf = ByteBuffer.wrap(payloadBytes);
        long timestamp = payloadBuf.getLong();
        long offset = payloadBuf.getLong();

        int idLen = payloadBuf.getInt();
        byte[] idBytes = new byte[idLen];
        payloadBuf.get(idBytes);
        String id = new String(idBytes, StandardCharsets.UTF_8);

        int keyLen = payloadBuf.getInt();
        String key = null;
        if (keyLen >= 0) {
            byte[] keyBytes = new byte[keyLen];
            payloadBuf.get(keyBytes);
            key = new String(keyBytes, StandardCharsets.UTF_8);
        }

        int valLen = payloadBuf.getInt();
        byte[] valBytes = new byte[valLen];
        payloadBuf.get(valBytes);

        return new Message(id, key, valBytes, timestamp, topic, partition, offset);
    }
}
