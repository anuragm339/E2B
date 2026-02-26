package com.messaging.network.codec;

import com.messaging.common.model.BrokerMessage;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.charset.StandardCharsets;

/**
 * Handles batch decoded events by sending BATCH_ACK to broker.
 * Must be placed after ZeroCopyBatchDecoder in the pipeline.
 *
 * MULTI-GROUP FIX: BATCH_ACK now includes both topic and group for proper offset routing.
 */
public final class BatchAckHandler extends ChannelInboundHandlerAdapter {

    private static final Logger log = LoggerFactory.getLogger(BatchAckHandler.class);

    @Override
    public void channelRead(ChannelHandlerContext ctx, Object msg) throws Exception {

        if (msg instanceof BatchDecodedEvent event) {

            if (event.topic() != null && event.group() != null) {
                // MULTI-GROUP FIX: Encode both topic and group in ACK payload
                // Format: [topicLen:4][topic:var][groupLen:4][group:var]
                byte[] topicBytes = event.topic().getBytes(StandardCharsets.UTF_8);
                byte[] groupBytes = event.group().getBytes(StandardCharsets.UTF_8);

                java.nio.ByteBuffer buffer = java.nio.ByteBuffer.allocate(4 + topicBytes.length + 4 + groupBytes.length);
                buffer.putInt(topicBytes.length);
                buffer.put(topicBytes);
                buffer.putInt(groupBytes.length);
                buffer.put(groupBytes);

                BrokerMessage ack = new BrokerMessage(
                        BrokerMessage.MessageType.BATCH_ACK,
                        System.currentTimeMillis(),
                        buffer.array()
                );

                ctx.channel().writeAndFlush(ack).addListener(f -> {
                    if (f.isSuccess()) {
                        log.debug("BATCH_ACK sent for topic={}, group={}", event.topic(), event.group());
                    } else {
                        log.error("Failed to send BATCH_ACK for topic={}, group={}", event.topic(), event.group(), f.cause());
                    }
                });
            }

            // Forward the records to the next handler
            ctx.fireChannelRead(event.records());
            return;
        }

        // Pass through other message types
        ctx.fireChannelRead(msg);
    }
}
