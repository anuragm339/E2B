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
 */
public final class BatchAckHandler extends ChannelInboundHandlerAdapter {

    private static final Logger log = LoggerFactory.getLogger(BatchAckHandler.class);

    @Override
    public void channelRead(ChannelHandlerContext ctx, Object msg) throws Exception {

        if (msg instanceof BatchDecodedEvent event) {

            if (event.topic() != null) {
                byte[] topicBytes = event.topic().getBytes(StandardCharsets.UTF_8);

                BrokerMessage ack = new BrokerMessage(
                        BrokerMessage.MessageType.BATCH_ACK,
                        System.currentTimeMillis(),
                        topicBytes
                );

                ctx.channel().writeAndFlush(ack).addListener(f -> {
                    if (f.isSuccess()) {
                        log.debug("BATCH_ACK sent for topic={}", event.topic());
                    } else {
                        log.error("Failed to send BATCH_ACK", f.cause());
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
