package com.messaging.network.codec;

import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;

/**
 * Unwraps a decoded zero-copy batch ({@link BatchDecodedEvent}) into its
 * {@code List<ConsumerRecord>} and forwards it down the pipeline. Must be placed after
 * {@code ZeroCopyBatchDecoder}; {@code ClientMessageHandler} downstream expects the unwrapped
 * list, not the event, which is why this handler is retained.
 *
 * <p><b>#1 (at-least-once):</b> this handler used to send the {@code BATCH_ACK} here — i.e.
 * <i>before</i> the consumer application had processed the batch. That commits the offset on the
 * broker regardless of whether the application succeeded, so a handler failure (or a crash) after
 * the ack silently loses data the broker considers delivered. The {@code BATCH_ACK} is now sent by
 * the application layer ({@code ClientConsumerManager}) only <i>after</i> {@code handler.handleBatch}
 * succeeds; on failure no ack is sent and the broker's ack-timeout reverts the offset and
 * redelivers.
 */
public final class BatchAckHandler extends ChannelInboundHandlerAdapter {

    @Override
    public void channelRead(ChannelHandlerContext ctx, Object msg) throws Exception {
        if (msg instanceof BatchDecodedEvent event) {
            // Forward the unwrapped records. The BATCH_ACK is intentionally NOT sent here — it is
            // sent after successful application processing (see class javadoc / #1).
            ctx.fireChannelRead(event.records());
            return;
        }
        // Pass through other message types unchanged.
        ctx.fireChannelRead(msg);
    }
}
