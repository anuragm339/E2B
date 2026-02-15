package com.messaging.network.codec;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.messaging.common.exception.ErrorCode;
import com.messaging.common.exception.NetworkException;
import com.messaging.common.model.BrokerMessage;
import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.MessageToByteEncoder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.charset.StandardCharsets;
import java.util.Base64;
import java.util.HashMap;
import java.util.Map;

/**
 * Encodes BrokerMessage to JSON text format (easier to debug than binary)
 * Format: JSON object with newline delimiter
 * {"type":"DATA","messageId":123,"payload":"base64encodeddata"}\n
 */
public class JsonMessageEncoder extends MessageToByteEncoder<BrokerMessage> {
    private static final Logger log = LoggerFactory.getLogger(JsonMessageEncoder.class);
    private static final ObjectMapper mapper = new ObjectMapper();

    @Override
    protected void encode(ChannelHandlerContext ctx, BrokerMessage msg, ByteBuf out) throws Exception {
        // Validate message
        if (msg == null) {
            log.error("Attempted to encode null BrokerMessage");
            throw new NetworkException(ErrorCode.NETWORK_ENCODING_ERROR, "BrokerMessage cannot be null");
        }

        if (msg.getType() == null) {
            log.error("Attempted to encode BrokerMessage with null type: messageId={}, payload={}",
                     msg.getMessageId(), msg.getPayload() != null ? msg.getPayload().length + " bytes" : "null");
            throw new NetworkException(ErrorCode.NETWORK_ENCODING_ERROR, "BrokerMessage type cannot be null")
                .withContext("messageId", msg.getMessageId());
        }

        // Create JSON object
        Map<String, Object> json = new HashMap<>();
        json.put("type", msg.getType().name());
        json.put("messageId", msg.getMessageId());

        // Encode payload as base64 if present
        if (msg.getPayload() != null && msg.getPayload().length > 0) {
            json.put("payload", Base64.getEncoder().encodeToString(msg.getPayload()));
        } else {
            json.put("payload", "");
        }

        // Convert to JSON string and add newline delimiter
        String jsonStr = mapper.writeValueAsString(json) + "\n";
        byte[] bytes = jsonStr.getBytes(StandardCharsets.UTF_8);
        out.writeBytes(bytes);

        log.debug("Encoded JSON message: type={}, messageId={}, payloadLength={}, jsonLength={}",
                  msg.getType(), msg.getMessageId(),
                  msg.getPayload() != null ? msg.getPayload().length : 0,
                  bytes.length);
    }
}
