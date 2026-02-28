package com.messaging.network.legacy;

import com.messaging.network.legacy.events.EventType;
import com.messaging.common.model.BrokerMessage;
import com.messaging.network.codec.BinaryMessageDecoder;
import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelPipeline;
import io.netty.handler.codec.ByteToMessageDecoder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

/**
 * Protocol detection decoder that determines if client is using
 * legacy Event protocol or modern BrokerMessage protocol.
 *
 * Detection strategy:
 * - Legacy: First byte is EventType ordinal (0-7)
 * - Modern: First byte is MessageType code (0x01-0x0C)
 *
 * Once detected, replaces itself in pipeline with appropriate decoder.
 */
public class ProtocolDetectionDecoder extends ByteToMessageDecoder {
    private static final Logger log = LoggerFactory.getLogger(ProtocolDetectionDecoder.class);

    @Override
    protected void decode(ChannelHandlerContext ctx, ByteBuf in, List<Object> out) throws Exception {
        // Need at least 1 byte to detect protocol
        if (in.readableBytes() < 1) {
            return;
        }

        // Peek at first byte without consuming
        byte firstByte = in.getByte(in.readerIndex());

        ProtocolType protocol = detectProtocol(firstByte);
        log.info("Detected protocol: {} (first byte: 0x{}) for client: {}",
                protocol, String.format("%02X", firstByte), ctx.channel().remoteAddress());

        // Replace this decoder with appropriate protocol decoder
        ChannelPipeline pipeline = ctx.pipeline();

        if (protocol == ProtocolType.LEGACY) {
            // Legacy client detected - switch to Event protocol
            pipeline.replace("decoder", "legacyEventDecoder", new LegacyEventDecoder());
            pipeline.replace("encoder", "legacyEventEncoder", new LegacyEventEncoder());

            // Add connection state handler for stateful ACK tracking
            pipeline.addBefore("handler", "legacyConnectionState", new LegacyConnectionState());

            log.info("Switched to legacy Event protocol for client: {}", ctx.channel().remoteAddress());
        } else {
            // Modern client - use standard BinaryMessageDecoder
            pipeline.replace(this, "binaryDecoder", new BinaryMessageDecoder());
            log.info("Switched to modern BrokerMessage protocol for client: {}", ctx.channel().remoteAddress());
        }

        // Remove this handler from pipeline (job done)
        pipeline.remove(this);

        // Trigger pipeline re-processing with the new decoder
        // This ensures the current ByteBuf is processed by the new decoder
        ctx.fireChannelRead(in.retain());
    }

    /**
     * Detect protocol type based on first byte.
     *
     * Legacy EventType ordinals:
     *   0 = REGISTER
     *   1 = MESSAGE
     *   2 = RESET
     *   3 = READY
     *   4 = ACK
     *   5 = EOF
     *   6 = DELETE
     *   7 = BATCH
     *
     * Modern MessageType codes:
     *   0x01 = DATA
     *   0x02 = ACK
     *   0x03 = SUBSCRIBE
     *   0x04 = COMMIT_OFFSET
     *   0x05 = RESET
     *   0x06 = READY
     *   0x07 = DISCONNECT
     *   0x08 = HEARTBEAT
     *   0x09 = BATCH_HEADER
     *   0x0A = BATCH_ACK
     *   0x0B = RESET_ACK
     *   0x0C = READY_ACK
     */
    private ProtocolType detectProtocol(byte firstByte) {
        // Legacy protocol: first byte is EventType ordinal (0-7)
        // We expect REGISTER (0) as first message from legacy client
        if (firstByte >= 0 && firstByte <= 7) {
            // Could be legacy REGISTER or ambiguous with modern codes
            // Check if it's a valid EventType
            try {
                EventType.get(firstByte);
                // Valid EventType - likely legacy if it's REGISTER (0)
                if (firstByte == EventType.REGISTER.ordinal()) {
                    return ProtocolType.LEGACY;
                }
                // For other ordinals (1-7), check if they make sense as first message
                // Legacy clients should send REGISTER first, so 1-7 as first byte
                // is more likely to be modern protocol
            } catch (IllegalArgumentException e) {
                // Invalid EventType
            }
        }

        // Modern protocol: first byte is MessageType code
        // Check if it matches known MessageType codes
        for (BrokerMessage.MessageType type : BrokerMessage.MessageType.values()) {
            if (type.getCode() == firstByte) {
                return ProtocolType.MODERN;
            }
        }

        // Default to modern if ambiguous
        log.warn("Ambiguous protocol detection (firstByte=0x{}), defaulting to MODERN",
                String.format("%02X", firstByte));
        return ProtocolType.MODERN;
    }

    private enum ProtocolType {
        LEGACY,
        MODERN
    }
}
