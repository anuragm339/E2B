package com.messaging.network.codec;

import com.messaging.common.exception.ErrorCode;
import com.messaging.common.exception.NetworkException;
import com.messaging.common.model.BrokerMessage;
import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.ByteToMessageDecoder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

/**
 * Decodes binary messages from ByteBuf to BrokerMessage
 * Format: [Type:1B][MessageId:8B][PayloadLength:4B][Payload:variable]
 */
public class BinaryMessageDecoder extends ByteToMessageDecoder {
    private static final Logger log = LoggerFactory.getLogger(BinaryMessageDecoder.class);

    @Override
    protected void decode(ChannelHandlerContext ctx, ByteBuf in, List<Object> out) throws Exception {
        // Need at least 13 bytes for header (1 + 8 + 4)
        if (in.readableBytes() < 13) {
            log.debug("Not enough bytes for header: {} bytes available", in.readableBytes());
            return;
        }

        // Mark reader index in case we need to reset
        in.markReaderIndex();

        // Read header
        byte typeCode = in.readByte();
        long messageId = in.readLong();
        int payloadLength = in.readInt();

//        log.debug("Decoding message: typeCode={}, messageId={}, payloadLength={}, readable={}",
//                  typeCode, messageId, payloadLength, in.readableBytes());

        // Validate payload length
        if (payloadLength < 0 || payloadLength > 10 * 1024 * 1024) { // Max 10MB
            // Log first 20 bytes for debugging
            in.resetReaderIndex();
            byte[] debugBytes = new byte[Math.min(20, in.readableBytes())];
            in.getBytes(in.readerIndex(), debugBytes);
            StringBuilder hex = new StringBuilder();
            for (byte b : debugBytes) {
                hex.append(String.format("%02X ", b));
            }
            log.error("Invalid payload length: {}. First 20 bytes: {}", payloadLength, hex);
            throw new NetworkException(ErrorCode.NETWORK_DECODING_ERROR, "Invalid payload length: " + payloadLength)
                .withContext("payloadLength", payloadLength)
                .withContext("firstBytes", hex.toString());
        }

        // Check if we have enough bytes for the payload
        if (in.readableBytes() < payloadLength) {
            in.resetReaderIndex();
            return; // Wait for more data
        }

        // Read payload
        byte[] payload = new byte[payloadLength];
        in.readBytes(payload);

        // Create BrokerMessage
        BrokerMessage message = new BrokerMessage();
        message.setType(BrokerMessage.MessageType.fromCode(typeCode));
        message.setMessageId(messageId);
        message.setPayload(payload);

        out.add(message);
    }
}
