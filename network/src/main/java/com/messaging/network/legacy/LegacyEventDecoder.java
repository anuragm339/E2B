package com.messaging.network.legacy;

import com.messaging.network.legacy.events.*;
import com.messaging.common.model.BrokerMessage;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufInputStream;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.ByteToMessageDecoder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.DataInputStream;
import java.nio.charset.StandardCharsets;
import java.util.List;

/**
 * Decodes legacy Event wire protocol to internal BrokerMessage format.
 *
 * Wire format: [EventType ordinal:1B][Event-specific payload...]
 *
 * Conversion mapping:
 * - RegisterEvent → SUBSCRIBE (with serviceName as consumer group)
 * - DataMessageEvent → DATA (single message)
 * - BatchEvent → BATCH_HEADER (multiple messages)
 * - ResetEvent → RESET
 * - ReadyEvent → READY
 * - AckEvent → ACK/BATCH_ACK/RESET_ACK/READY_ACK (context-dependent, tracked by LegacyConnectionState)
 * - DeleteMessageEvent → DATA (with DELETE flag)
 * - EOF → DISCONNECT
 */
public class LegacyEventDecoder extends ByteToMessageDecoder {
    private static final Logger log = LoggerFactory.getLogger(LegacyEventDecoder.class);

    @Override
    protected void decode(ChannelHandlerContext ctx, ByteBuf in, List<Object> out) throws Exception {
        // Need at least 1 byte for EventType ordinal
        if (in.readableBytes() < 1) {
            return;
        }

        // Mark reader index in case we need to reset
        in.markReaderIndex();

        try {
            // Read EventType ordinal
            byte ordinal = in.readByte();
            EventType eventType = EventType.get(ordinal);

            log.debug("Decoding legacy event: type={}, readable={} bytes",
                    eventType, in.readableBytes());

            // Decode Event from remaining bytes
            Event event = decodeEvent(eventType, in);
            if (event == null) {
                // Not enough data yet - reset and wait
                in.resetReaderIndex();
                return;
            }

            // Convert Event to BrokerMessage
            BrokerMessage message = convertToBrokerMessage(event);
            if (message != null) {
                out.add(message);
                log.trace("Decoded legacy event: {} → BrokerMessage type={}",
                        eventType, message.getType());
            } else {
                log.warn("Failed to convert event to BrokerMessage: {}", eventType);
            }

        } catch (Exception e) {
            log.error("Error decoding legacy event", e);
            in.resetReaderIndex();
            throw e;
        }
    }

    /**
     * Decode Event from ByteBuf based on EventType
     */
    private Event decodeEvent(EventType eventType, ByteBuf in) throws Exception {
        // Wrap ByteBuf in DataInputStream for Event deserialization
        DataInputStream dis = new DataInputStream(new ByteBufInputStream(in));

        switch (eventType) {
            case REGISTER:
                return RegisterEvent.from(dis);

            case MESSAGE:
                return DataMessageEvent.from(dis);

            case RESET:
                return ResetEvent.from(dis);

            case READY:
                return ReadyEvent.from(dis);

            case ACK:
                // AckEvent has no payload
                return AckEvent.INSTANCE;

            case EOF:
                // EOF has no payload
                return EOFEvent.INSTANCE;

            case DELETE:
                return DeleteMessageEvent.from(dis);

            case BATCH:
                return BatchEvent.from(dis);

            default:
                log.error("Unknown EventType: {}", eventType);
                return null;
        }
    }

    /**
     * Convert Event to BrokerMessage.
     * Payload is serialized as needed for broker internal processing.
     */
    private BrokerMessage convertToBrokerMessage(Event event) throws Exception {
        EventType type = event.getType();
        BrokerMessage message = new BrokerMessage();

        // Generate synthetic messageId (legacy protocol doesn't have messageId)
        message.setMessageId(System.nanoTime());

        switch (type) {
            case REGISTER:
                RegisterEvent registerEvent = (RegisterEvent) event;
                message.setType(BrokerMessage.MessageType.SUBSCRIBE);

                // Payload: serviceName (will be used as consumer group)
                String serviceName = registerEvent.getClientId();
                message.setPayload(serviceName.getBytes(StandardCharsets.UTF_8));
                break;

            case MESSAGE:
                DataMessageEvent dataEvent = (DataMessageEvent) event;
                message.setType(BrokerMessage.MessageType.DATA);

                // Payload: serialize DataMessage as JSON-like format
                // Format: {type}|{key}|{contentType}|{content}
                DataMessage dataMsg = dataEvent.getMessage();
                String payload = String.format("%s|%s|%s|%s",
                        dataMsg.getType(),
                        dataMsg.getKey(),
                        dataMsg.getContentType() != null ? dataMsg.getContentType() : "application/json",
                        dataMsg.getContent());
                message.setPayload(payload.getBytes(StandardCharsets.UTF_8));
                break;

            case BATCH:
                BatchEvent batchEvent = (BatchEvent) event;
                message.setType(BrokerMessage.MessageType.BATCH_HEADER);

                // Payload: batch metadata (count, offsets)
                String batchMeta = String.format("count=%d,offsets=%s",
                        batchEvent.count(),
                        batchEvent.getOffsets());
                message.setPayload(batchMeta.getBytes(StandardCharsets.UTF_8));
                break;

            case RESET:
                message.setType(BrokerMessage.MessageType.RESET);
                message.setPayload(new byte[0]);
                break;

            case READY:
                message.setType(BrokerMessage.MessageType.READY);
                message.setPayload(new byte[0]);
                break;

            case ACK:
                // ACK type is context-dependent (tracked by LegacyConnectionState)
                // Default to generic ACK
                message.setType(BrokerMessage.MessageType.ACK);
                message.setPayload(new byte[0]);
                break;

            case DELETE:
                DeleteMessageEvent deleteEvent = (DeleteMessageEvent) event;
                message.setType(BrokerMessage.MessageType.DATA);

                // Payload: serialize DeleteMessage with DELETE flag
                DeleteMessage deleteMsg = deleteEvent.getMessage();
                String deletePayload = String.format("DELETE|%s|%s",
                        deleteMsg.getType(),
                        deleteMsg.getKey());
                message.setPayload(deletePayload.getBytes(StandardCharsets.UTF_8));
                break;

            case EOF:
                message.setType(BrokerMessage.MessageType.DISCONNECT);
                message.setPayload(new byte[0]);
                break;

            default:
                log.warn("Unhandled EventType: {}", type);
                return null;
        }

        return message;
    }
}
