package com.messaging.network.legacy;

import com.messaging.network.codec.BinaryMessageDecoder;
import com.messaging.network.legacy.ProtocolDetectionService.ProtocolType;
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
 * Phase 11: Refactored to use ProtocolDetectionService instead of
 * embedding detection logic in the Netty handler.
 *
 * Once detected, replaces itself in pipeline with appropriate decoder.
 */
public class ProtocolDetectionDecoder extends ByteToMessageDecoder {
    private static final Logger log = LoggerFactory.getLogger(ProtocolDetectionDecoder.class);

    // Phase 11: Inject protocol detection service
    private final ProtocolDetectionService protocolDetectionService;

    public ProtocolDetectionDecoder() {
        // Default constructor - use default implementation
        this(new DefaultProtocolDetectionService());
    }

    public ProtocolDetectionDecoder(ProtocolDetectionService protocolDetectionService) {
        this.protocolDetectionService = protocolDetectionService;
    }

    @Override
    protected void decode(ChannelHandlerContext ctx, ByteBuf in, List<Object> out) throws Exception {
        // Need at least 1 byte to detect protocol
        if (in.readableBytes() < 1) {
            return;
        }

        // Peek at first byte without consuming
        byte firstByte = in.getByte(in.readerIndex());

        // Phase 11: Use ProtocolDetectionService instead of local logic
        ProtocolType protocol = protocolDetectionService.detectProtocol(firstByte);
        log.info("Detected protocol: {} (first byte: 0x{}) for client: {}",
                protocol, String.format("%02X", firstByte & 0xFF), ctx.channel().remoteAddress());

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

        // Note: pipeline.replace() already removed this handler, no need to call remove(this)

        // Trigger pipeline re-processing with the new decoder
        // This ensures the current ByteBuf is processed by the new decoder
        ctx.fireChannelRead(in.retain());
    }
}
