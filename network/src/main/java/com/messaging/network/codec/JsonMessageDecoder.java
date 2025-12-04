package com.messaging.network.codec;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.messaging.common.model.BrokerMessage;
import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.ByteToMessageDecoder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.charset.StandardCharsets;
import java.util.Base64;
import java.util.List;
import java.util.Map;

/**
 * Decodes JSON text format to BrokerMessage
 * Format: JSON object with newline delimiter
 * {"type":"DATA","messageId":123,"payload":"base64encodeddata"}\n
 */
public class JsonMessageDecoder extends ByteToMessageDecoder {
    private static final Logger log = LoggerFactory.getLogger(JsonMessageDecoder.class);
    private static final ObjectMapper mapper = new ObjectMapper();
    private static final int MAX_MESSAGE_SIZE = 10 * 1024 * 1024; // 10MB

    @Override
    protected void decode(ChannelHandlerContext ctx, ByteBuf in, List<Object> out) throws Exception {
        // Mark the current reader index
        in.markReaderIndex();

        // Look for newline delimiter
        int newlineIndex = -1;
        int readableBytes = in.readableBytes();

        // Sanity check
        if (readableBytes > MAX_MESSAGE_SIZE) {
            log.error("Message too large: {} bytes", readableBytes);
            throw new IllegalArgumentException("Message too large: " + readableBytes);
        }

        for (int i = in.readerIndex(); i < in.readerIndex() + readableBytes; i++) {
            if (in.getByte(i) == '\n') {
                newlineIndex = i;
                break;
            }
        }

        // If no newline found, wait for more data
        if (newlineIndex == -1) {
            in.resetReaderIndex();
            return;
        }

        // Read JSON string (excluding newline)
        int jsonLength = newlineIndex - in.readerIndex();
        byte[] jsonBytes = new byte[jsonLength];
        in.readBytes(jsonBytes);
        in.readByte(); // consume the newline

        String jsonStr = new String(jsonBytes, StandardCharsets.UTF_8);

        log.debug("Decoding JSON message: length={}, json={}", jsonLength,
                  jsonStr.length() > 200 ? jsonStr.substring(0, 200) + "..." : jsonStr);

        // Parse JSON
        @SuppressWarnings("unchecked")
        Map<String, Object> json = mapper.readValue(jsonStr, Map.class);

        String typeName = (String) json.get("type");
        Object messageIdObj = json.get("messageId");
        String payloadBase64 = (String) json.get("payload");

        // Convert messageId to long (Jackson may parse as Integer or Long)
        long messageId;
        if (messageIdObj instanceof Integer) {
            messageId = ((Integer) messageIdObj).longValue();
        } else if (messageIdObj instanceof Long) {
            messageId = (Long) messageIdObj;
        } else {
            messageId = Long.parseLong(messageIdObj.toString());
        }

        // Decode payload from base64
        byte[] payload = null;
        if (payloadBase64 != null && !payloadBase64.isEmpty()) {
            payload = Base64.getDecoder().decode(payloadBase64);
        }

        // Create BrokerMessage
        BrokerMessage.MessageType type = BrokerMessage.MessageType.valueOf(typeName);
        BrokerMessage message = new BrokerMessage(type, messageId, payload);

        log.debug("Decoded message: type={}, messageId={}, payloadLength={}",
                  type, messageId, payload != null ? payload.length : 0);

        out.add(message);
    }
}
