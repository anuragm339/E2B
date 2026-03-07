package com.messaging.network.legacy;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import com.messaging.network.legacy.events.*;
import com.messaging.common.model.BrokerMessage;
import com.messaging.common.model.EventType;
import com.messaging.common.model.MessageRecord;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufOutputStream;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.MessageToByteEncoder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.DataOutputStream;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 * Encodes internal BrokerMessage to legacy Event wire protocol.
 *
 * Wire format: [EventType ordinal:1B][Event-specific payload...]
 *
 * Conversion mapping (reverse of decoder):
 * - DATA → DataMessageEvent or DeleteMessageEvent (based on payload)
 * - BATCH_HEADER → BatchEvent
 * - RESET → ResetEvent
 * - READY → ReadyEvent
 * - DISCONNECT → EofEvent
 *
 * Note: Broker typically doesn't send SUBSCRIBE/ACK to clients,
 * so those conversions are not needed.
 */
public class LegacyEventEncoder extends MessageToByteEncoder<BrokerMessage> {
    private static final Logger log = LoggerFactory.getLogger(LegacyEventEncoder.class);
    private static final ObjectMapper objectMapper = new ObjectMapper()
            .registerModule(new JavaTimeModule());

    @Override
    protected void encode(ChannelHandlerContext ctx, BrokerMessage msg, ByteBuf out) throws Exception {
        Event event = convertToEvent(msg);
        if (event == null) {
            log.warn("Cannot convert BrokerMessage type {} to Event", msg.getType());
            return;
        }

        log.trace("Encoding BrokerMessage type={} to Event type={}",
                msg.getType(), event.getType());

        // Write EventType ordinal
        out.writeByte(event.getType().ordinal());

        // Write Event payload
        DataOutputStream dos = new DataOutputStream(new ByteBufOutputStream(out));
        event.toWire(dos);
        dos.flush();
    }

    /**
     * Convert BrokerMessage to Event based on message type
     */
    private Event convertToEvent(BrokerMessage msg) throws Exception {
        BrokerMessage.MessageType type = msg.getType();
        byte[] payload = msg.getPayload();

        switch (type) {
            case DATA:
                // Parse payload to determine if it's DataMessage or DeleteMessage
                String payloadStr = new String(payload, StandardCharsets.UTF_8);
                if (payloadStr.startsWith("DELETE|")) {
                    // DeleteMessage format: DELETE|{type}|{key}
                    String[] parts = payloadStr.split("\\|", 3);
                    if (parts.length >= 3) {
                        String msgType = parts[1];
                        String key = parts[2];
                        DeleteMessage deleteMsg = new DeleteMessage(msgType, key);
                        return new DeleteMessageEvent(deleteMsg);
                    }
                } else {
                    // DataMessage format: {type}|{key}|{contentType}|{content}
                    String[] parts = payloadStr.split("\\|", 4);
                    if (parts.length >= 4) {
                        String msgType = parts[0];
                        String key = parts[1];
                        String contentType = parts[2];
                        String content = parts[3];
                        DataMessage dataMsg = new DataMessage(msgType, key, contentType, content);
                        return new DataMessageEvent(dataMsg);
                    }
                }
                log.error("Invalid DATA payload format: {}", payloadStr);
                return null;

            case BATCH_HEADER:
                // Parse JSON payload to reconstruct batch
                String json = new String(payload, StandardCharsets.UTF_8);
                List<MessageRecord> messages = objectMapper.readValue(json,
                    new TypeReference<List<MessageRecord>>() {});

                // Convert MessageRecords to Events
                List<Event> events = new ArrayList<>();
                for (MessageRecord record : messages) {
                    // Use topic as message type, msgKey as key
                    String messageType = record.getTopic() != null ? record.getTopic() : "default";
                    String messageKey = record.getMsgKey();

                    if (record.getEventType() == EventType.MESSAGE) {
                        DataMessage dataMsg = new DataMessage(
                            messageType,
                            messageKey,
                            "application/json",
                            record.getData()
                        );
                        events.add(new DataMessageEvent(dataMsg));
                    } else if (record.getEventType() == EventType.DELETE) {
                        DeleteMessage delMsg = new DeleteMessage(
                            messageType,
                            messageKey
                        );
                        events.add(new DeleteMessageEvent(delMsg));
                    }
                }

                log.debug("Reconstructed BatchEvent with {} messages from JSON", events.size());
                return new BatchEvent(events);

            case RESET:
                return ResetEvent.INSTANCE;

            case READY:
                return ReadyEvent.INSTANCE;

            case DISCONNECT:
                return EOFEvent.INSTANCE;

            case HEARTBEAT:
                // Legacy protocol doesn't have HeartbeatEvent
                // Could send ACK as keep-alive
                return AckEvent.INSTANCE;

            case ACK:
            case BATCH_ACK:
            case RESET_ACK:
            case READY_ACK:
                // Broker sending ACK to client (unusual but possible)
                return AckEvent.INSTANCE;

            default:
                log.warn("Unhandled BrokerMessage type for legacy encoding: {}", type);
                return null;
        }
    }
}
