package com.messaging.network.codec;

import com.messaging.common.exception.ErrorCode;
import com.messaging.common.exception.NetworkException;
import com.messaging.common.model.BrokerMessage;
import com.messaging.common.model.ConsumerRecord;
import com.messaging.common.model.EventType;
import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.ByteToMessageDecoder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.zip.CRC32;

/**
 * Decoder for zero-copy batch messages from broker.
 *
 * Handles two types of messages:
 * 1. Regular BrokerMessages (SUBSCRIBE_ACK, etc.) - decoded normally
 * 2. Zero-copy DATA messages - header + raw segment binary data
 *
 * Zero-copy protocol:
 * - First message: DATA with 20-byte header [recordCount:4][totalBytes:8][lastOffset:8]
 * - Followed by: Raw segment bytes with records in format:
 *   [offset:8][keyLen:4][key:var][eventType:1][dataLen:4][data:var][createdAt:8][crc32:4]
 */
public class ZeroCopyBatchDecoder extends ByteToMessageDecoder {
    private static final Logger log = LoggerFactory.getLogger(ZeroCopyBatchDecoder.class);

    private static final int BROKER_MESSAGE_HEADER_SIZE = 13; // type(1) + messageId(8) + payloadLen(4)
    private static final int ZERO_COPY_HEADER_SIZE = 20; // recordCount(4) + totalBytes(8) + lastOffset(8)

    // State for multi-step decoding
    private enum DecoderState {
        READING_BROKER_MESSAGE,  // Default state: reading BrokerMessage header
        READING_ZERO_COPY_BATCH   // Reading raw segment data after zero-copy header
    }

    private DecoderState state = DecoderState.READING_BROKER_MESSAGE;
    private int expectedRecordCount = 0;
    private long expectedTotalBytes = 0;
    private long lastOffset = 0;
    private String currentBatchTopic = null; // Track which topic this batch belongs to

    @Override
    protected void decode(ChannelHandlerContext ctx, ByteBuf in, List<Object> out) throws Exception {

        if (state == DecoderState.READING_BROKER_MESSAGE) {
            decodeBrokerMessage(ctx, in, out);
        } else if (state == DecoderState.READING_ZERO_COPY_BATCH) {
            decodeZeroCopyBatch(ctx, in, out);
        }
    }

    /**
     * Decode BrokerMessage header and check if it's a zero-copy batch
     */
    private void decodeBrokerMessage(ChannelHandlerContext ctx, ByteBuf in, List<Object> out) throws Exception {
        // Need at least 13 bytes for BrokerMessage header
        if (in.readableBytes() < BROKER_MESSAGE_HEADER_SIZE) {
            return;
        }

        // Mark reader index in case we need to reset
        in.markReaderIndex();

        // Read BrokerMessage header
        byte typeCode = in.readByte();
        long messageId = in.readLong();
        int payloadLength = in.readInt();

        // Validate payload length
        if (payloadLength < 0 || payloadLength > 10 * 1024 * 1024) {
            log.error("Invalid payload length: {}", payloadLength);
            in.resetReaderIndex();
            throw new NetworkException(ErrorCode.NETWORK_DECODING_ERROR, "Invalid payload length: " + payloadLength)
                .withContext("payloadLength", payloadLength);
        }

        // Check if we have enough bytes for the payload
        if (in.readableBytes() < payloadLength) {
            in.resetReaderIndex();
            return; // Wait for more data
        }

        BrokerMessage.MessageType messageType = BrokerMessage.MessageType.fromCode(typeCode);

        // Handle BATCH_HEADER message - this signals that raw FileRegion bytes will follow
        if (messageType == BrokerMessage.MessageType.BATCH_HEADER) {
            // Read batch metadata from payload
            byte[] payload = new byte[payloadLength];
            in.readBytes(payload);

            // UNIFIED FORMAT: [recordCount:4][totalBytes:8][topicLen:4][topic:var]
            // Removed lastOffset - consumer doesn't need it
            java.nio.ByteBuffer buffer = java.nio.ByteBuffer.wrap(payload);
            expectedRecordCount = buffer.getInt();
            expectedTotalBytes = buffer.getLong();

            // Parse topic name from header
            int topicLen = buffer.getInt();
            byte[] topicBytes = new byte[topicLen];
            buffer.get(topicBytes);
            currentBatchTopic = new String(topicBytes, StandardCharsets.UTF_8);

            log.debug("Received BATCH_HEADER: topic={}, recordCount={}, totalBytes={}",
                    currentBatchTopic, expectedRecordCount, expectedTotalBytes);

            // Transition to READING_ZERO_COPY_BATCH state to expect raw bytes
            state = DecoderState.READING_ZERO_COPY_BATCH;

            return; // Don't add anything to output, wait for batch data
        }

        // Check if this is a zero-copy batch (DATA message with payload > 20 bytes containing header + data)
        if (messageType == BrokerMessage.MessageType.DATA && payloadLength > ZERO_COPY_HEADER_SIZE) {
            // This might be a zero-copy batch - read the header first
            int headerRecordCount = in.readInt();
            long headerTotalBytes = in.readLong();
            long headerLastOffset = in.readLong();

            // Verify that remaining payload matches the expected batch size
            int remainingPayload = payloadLength - ZERO_COPY_HEADER_SIZE;
            if (remainingPayload == headerTotalBytes) {
                // This is a zero-copy batch! Parse it directly
                log.debug("Received zero-copy batch: recordCount={}, totalBytes={}, lastOffset={}",
                         headerRecordCount, headerTotalBytes, headerLastOffset);

                // Parse the batch data directly from the current buffer
                List<ConsumerRecord> records = parseBatchData(in, (int)headerTotalBytes, headerRecordCount);
                log.debug("Decoded zero-copy batch: {} records, {} bytes", records.size(), headerTotalBytes);

                // Add records list to output
                out.add(records);
            } else {
                // Not a zero-copy batch, treat as regular DATA message
                // Reset position and read as normal payload
                in.resetReaderIndex();
                in.skipBytes(BROKER_MESSAGE_HEADER_SIZE); // Skip past header again

                byte[] payload = new byte[payloadLength];
                in.readBytes(payload);

                BrokerMessage message = new BrokerMessage();
                message.setType(messageType);
                message.setMessageId(messageId);
                message.setPayload(payload);

                out.add(message);
            }
        } else {
            // Regular BrokerMessage - read payload and create message
            byte[] payload = new byte[payloadLength];
            in.readBytes(payload);

            BrokerMessage message = new BrokerMessage();
            message.setType(messageType);
            message.setMessageId(messageId);
            message.setPayload(payload);

            out.add(message);
        }
    }

    /**
     * Parse batch data from ByteBuf into ConsumerRecords
     */
    private List<ConsumerRecord> parseBatchData(ByteBuf in, int totalBytes, int recordCount) throws Exception {
        List<ConsumerRecord> records = new ArrayList<>(recordCount);
        long bytesRead = 0;

        while (bytesRead < totalBytes && records.size() < recordCount) {
            // Record format: [offset:8][keyLen:4][key:var][eventType:1][dataLen:4][data:var][createdAt:8][crc32:4]

            // Read offset
            if (in.readableBytes() < 8) break;
            long offset = in.readLong();
            bytesRead += 8;

            // Read key
            if (in.readableBytes() < 4) break;
            int keyLen = in.readInt();
            bytesRead += 4;

            if (keyLen < 0 || keyLen > 1024 * 1024) {
                log.error("Invalid keyLen: {}", keyLen);
                break;
            }

            if (in.readableBytes() < keyLen) break;
            byte[] keyBytes = new byte[keyLen];
            in.readBytes(keyBytes);
            String msgKey = new String(keyBytes, StandardCharsets.UTF_8);
            bytesRead += keyLen;

            // Read event type
            if (in.readableBytes() < 1) break;
            byte eventType = in.readByte();
            bytesRead += 1;

            // Read data
            if (in.readableBytes() < 4) break;
            int dataLen = in.readInt();
            bytesRead += 4;

            if (dataLen < 0 || dataLen > 10 * 1024 * 1024) {
                log.error("Invalid dataLen: {}", dataLen);
                break;
            }

            if (in.readableBytes() < dataLen) break;
            String data = null;
            if (dataLen > 0) {
                byte[] dataBytes = new byte[dataLen];
                in.readBytes(dataBytes);
                data = new String(dataBytes, StandardCharsets.UTF_8);
            }
            bytesRead += dataLen;

            // Read timestamp
            if (in.readableBytes() < 8) break;
            long createdAtMillis = in.readLong();
            bytesRead += 8;

            // Read CRC32 (skip validation for now)
            if (in.readableBytes() < 4) break;
            int crc32 = in.readInt();
            bytesRead += 4;

            // Convert byte to EventType and long to Instant
            EventType eventTypeEnum = (eventType == 'M') ? EventType.MESSAGE : EventType.DELETE;
            Instant createdAt = Instant.ofEpochMilli(createdAtMillis);

            // Create ConsumerRecord (doesn't include offset - consumer tracks separately)
            ConsumerRecord record = new ConsumerRecord(msgKey, eventTypeEnum, data, createdAt);
            records.add(record);
        }

        log.debug("Parsed batch data: {} records, {} bytes", records.size(), bytesRead);
        return records;
    }

    /**
     * Decode zero-copy batch: raw segment binary data
     */
    private void decodeZeroCopyBatch(ChannelHandlerContext ctx, ByteBuf in, List<Object> out) throws Exception {
        // Check if we have all the expected bytes
        if (in.readableBytes() < expectedTotalBytes) {
            log.debug("Waiting for zero-copy batch data: have {} bytes, need {} bytes",
                     in.readableBytes(), expectedTotalBytes);
            return; // Wait for more data
        }

        log.debug("Decoding zero-copy batch: {} bytes", expectedTotalBytes);

        // DEBUG: Log first 32 bytes in hex to see what we're receiving
        if (in.readableBytes() >= 32) {
            in.markReaderIndex();
            byte[] first32 = new byte[32];
            in.readBytes(first32);
            in.resetReaderIndex();

            StringBuilder hex = new StringBuilder();
            for (int i = 0; i < 32; i++) {
                hex.append(String.format("%02X ", first32[i]));
                if ((i + 1) % 16 == 0) hex.append("\n                ");
            }
            log.debug("First 32 bytes received:\n                {}", hex.toString());
        }

        // Parse raw segment data into ConsumerRecords
        List<ConsumerRecord> records = new ArrayList<>(expectedRecordCount);
        long bytesRead = 0;

        while (bytesRead < expectedTotalBytes && records.size() < expectedRecordCount) {
            // UNIFIED FORMAT: [keyLen:4][key:var][eventType:1][dataLen:4][data:var][timestamp:8]
            // NO offset at beginning, NO CRC at end

            // Read key
            if (in.readableBytes() < 4) break;
            int keyLen = in.readInt();
            bytesRead += 4;

            log.debug("Record {}: keyLen={} (0x{}) at bytesRead={}",
                     records.size(), keyLen, Integer.toHexString(keyLen), bytesRead - 4);

            if (keyLen < 0 || keyLen > 1024 * 1024) {
                log.error("Invalid keyLen: {} (0x{})", keyLen, Integer.toHexString(keyLen));
                break;
            }

            if (in.readableBytes() < keyLen) break;
            byte[] keyBytes = new byte[keyLen];
            in.readBytes(keyBytes);
            String msgKey = new String(keyBytes, StandardCharsets.UTF_8);
            bytesRead += keyLen;

            // Read event type
            if (in.readableBytes() < 1) break;
            byte eventType = in.readByte();
            bytesRead += 1;

            // Read data
            if (in.readableBytes() < 4) break;
            int dataLen = in.readInt();
            bytesRead += 4;

            if (dataLen < 0 || dataLen > 10 * 1024 * 1024) {
                log.error("Invalid dataLen: {}", dataLen);
                break;
            }

            if (in.readableBytes() < dataLen) break;
            String data = null;
            if (dataLen > 0) {
                byte[] dataBytes = new byte[dataLen];
                in.readBytes(dataBytes);
                data = new String(dataBytes, StandardCharsets.UTF_8);
            }
            bytesRead += dataLen;

            // Read timestamp
            if (in.readableBytes() < 8) break;
            long createdAtMillis = in.readLong();
            bytesRead += 8;

            // Convert byte to EventType and long to Instant
            EventType eventTypeEnum = (eventType == 'M') ? EventType.MESSAGE : EventType.DELETE;
            Instant createdAt = Instant.ofEpochMilli(createdAtMillis);

            // Create ConsumerRecord (doesn't include offset - consumer tracks separately)
            ConsumerRecord record = new ConsumerRecord(msgKey, eventTypeEnum, data, createdAt);
            records.add(record);
        }

        log.debug("Decoded zero-copy batch: {} records, {} bytes", records.size(), bytesRead);

        if (records.size() != expectedRecordCount) {
            // Partial parse — some records were not decoded (corrupt data or sizing bug).
            // Do NOT emit BatchDecodedEvent: emitting would trigger BATCH_ACK and advance the
            // broker offset past the unparsed records, causing permanent data loss.
            // Close the channel instead — the broker will detect the disconnect, and the consumer
            // will reconnect and re-subscribe. Delivery resumes from the last committed offset.
            log.error("Partial batch parse for topic {}: expected {} records, decoded {} records ({} bytes consumed of {})." +
                      " Closing channel to trigger reconnect and redelivery from last committed offset.",
                      currentBatchTopic, expectedRecordCount, records.size(), bytesRead, expectedTotalBytes);
            // Reset decoder state before closing
            state = DecoderState.READING_BROKER_MESSAGE;
            expectedRecordCount = 0;
            expectedTotalBytes = 0;
            lastOffset = 0;
            currentBatchTopic = null;
            ctx.close();
            return;
        }

        // Emit BatchDecodedEvent to pipeline (BatchAckHandler will send BATCH_ACK)
        out.add(new BatchDecodedEvent(records, currentBatchTopic));

        // Reset state for next message
        state = DecoderState.READING_BROKER_MESSAGE;
        expectedRecordCount = 0;
        expectedTotalBytes = 0;
        lastOffset = 0;
        currentBatchTopic = null;
    }
}
