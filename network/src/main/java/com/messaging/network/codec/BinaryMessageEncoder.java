package com.messaging.network.codec;

import com.messaging.common.model.BrokerMessage;
import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.MessageToByteEncoder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Encodes BrokerMessage to binary format
 * Format: [Type:1B][MessageId:8B][PayloadLength:4B][Payload:variable]
 */
public class BinaryMessageEncoder extends MessageToByteEncoder<BrokerMessage> {
    private static final Logger log = LoggerFactory.getLogger(BinaryMessageEncoder.class);

    @Override
    protected void encode(ChannelHandlerContext ctx, BrokerMessage msg, ByteBuf out) throws Exception {
        // Validate message
        if (msg == null) {
            log.error("Attempted to encode null BrokerMessage");
            throw new IllegalArgumentException("BrokerMessage cannot be null");
        }

        if (msg.getType() == null) {
            log.error("Attempted to encode BrokerMessage with null type: messageId={}, payload={}",
                     msg.getMessageId(), msg.getPayload() != null ? msg.getPayload().length + " bytes" : "null");
            throw new IllegalArgumentException("BrokerMessage type cannot be null");
        }

        byte typeCode = msg.getType().getCode();
        if (typeCode == 0) {
            log.error("Attempted to encode BrokerMessage with invalid type code 0: type={}, messageId={}",
                     msg.getType(), msg.getMessageId());
            throw new IllegalArgumentException("Invalid BrokerMessage type code: 0");
        }

        // Write type
        out.writeByte(typeCode);

        // Write message ID
        out.writeLong(msg.getMessageId());

        // Write payload length and payload
        byte[] payload = msg.getPayload();
        int payloadLength = (payload != null) ? payload.length : 0;
        out.writeInt(payloadLength);

        if (payload != null) {
            out.writeBytes(payload);
        }

        log.debug("Encoded message: type={} (code={}), messageId={}, payloadLength={}, totalBytes={}",
                  msg.getType(), typeCode, msg.getMessageId(), payloadLength, 1 + 8 + 4 + payloadLength);
    }
}
