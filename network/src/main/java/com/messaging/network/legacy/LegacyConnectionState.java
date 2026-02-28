package com.messaging.network.legacy;

import com.messaging.common.model.BrokerMessage;
import io.netty.channel.ChannelDuplexHandler;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelPromise;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayDeque;
import java.util.Deque;

/**
 * Stateful ACK tracking for legacy clients.
 *
 * Problem: Legacy protocol uses generic AckEvent (ordinal=4) for all acknowledgments:
 * - BATCH_ACK (acknowledging message batch)
 * - RESET_ACK (acknowledging RESET command)
 * - READY_ACK (acknowledging READY command)
 *
 * Solution: Track outbound messages that expect ACKs (RESET, READY, BATCH_HEADER).
 * When AckEvent arrives, determine its meaning based on the most recent pending message.
 *
 * State machine:
 * 1. Broker sends RESET → expect RESET_ACK
 * 2. Broker sends BATCH_HEADER → expect BATCH_ACK
 * 3. Broker sends READY → expect READY_ACK
 * 4. Client sends ACK → match to pending expectation
 */
public class LegacyConnectionState extends ChannelDuplexHandler {
    private static final Logger log = LoggerFactory.getLogger(LegacyConnectionState.class);

    // Queue of pending ACK expectations (in send order)
    private final Deque<AckExpectation> pendingAcks = new ArrayDeque<>();

    @Override
    public void write(ChannelHandlerContext ctx, Object msg, ChannelPromise promise) throws Exception {
        if (msg instanceof BrokerMessage) {
            BrokerMessage brokerMsg = (BrokerMessage) msg;
            trackOutboundMessage(brokerMsg);
        }

        // Continue with write
        super.write(ctx, msg, promise);
    }

    @Override
    public void channelRead(ChannelHandlerContext ctx, Object msg) throws Exception {
        if (msg instanceof BrokerMessage) {
            BrokerMessage brokerMsg = (BrokerMessage) msg;

            // If this is a generic ACK, resolve it to specific ACK type
            if (brokerMsg.getType() == BrokerMessage.MessageType.ACK) {
                BrokerMessage resolvedMsg = resolveAck(brokerMsg);
                super.channelRead(ctx, resolvedMsg);
                return;
            }
        }

        // Continue with read
        super.channelRead(ctx, msg);
    }

    /**
     * Track outbound messages that expect ACKs
     */
    private void trackOutboundMessage(BrokerMessage msg) {
        BrokerMessage.MessageType type = msg.getType();

        switch (type) {
            case RESET:
                pendingAcks.addLast(new AckExpectation(BrokerMessage.MessageType.RESET_ACK, "RESET"));
                log.debug("Expecting RESET_ACK (pending queue size: {})", pendingAcks.size());
                break;

            case READY:
                pendingAcks.addLast(new AckExpectation(BrokerMessage.MessageType.READY_ACK, "READY"));
                log.debug("Expecting READY_ACK (pending queue size: {})", pendingAcks.size());
                break;

            case BATCH_HEADER:
            case DATA:
                // Both BATCH_HEADER and individual DATA messages expect BATCH_ACK
                pendingAcks.addLast(new AckExpectation(BrokerMessage.MessageType.BATCH_ACK, "BATCH"));
                log.debug("Expecting BATCH_ACK (pending queue size: {})", pendingAcks.size());
                break;

            default:
                // Other message types don't expect ACKs
                break;
        }
    }

    /**
     * Resolve generic ACK to specific ACK type based on pending expectations
     */
    private BrokerMessage resolveAck(BrokerMessage genericAck) {
        if (pendingAcks.isEmpty()) {
            log.warn("Received ACK but no pending expectations - treating as generic ACK");
            return genericAck;
        }

        // Pop the oldest expectation (FIFO)
        AckExpectation expectation = pendingAcks.removeFirst();

        log.debug("Resolved ACK to {} (from generic ACK), remaining pending: {}",
                expectation.expectedType, pendingAcks.size());

        // Create new BrokerMessage with resolved type
        BrokerMessage resolvedMsg = new BrokerMessage();
        resolvedMsg.setType(expectation.expectedType);
        resolvedMsg.setMessageId(genericAck.getMessageId());
        resolvedMsg.setPayload(genericAck.getPayload());

        return resolvedMsg;
    }

    /**
     * Represents an expected ACK
     */
    private static class AckExpectation {
        final BrokerMessage.MessageType expectedType;
        final String description;

        AckExpectation(BrokerMessage.MessageType expectedType, String description) {
            this.expectedType = expectedType;
            this.description = description;
        }

        @Override
        public String toString() {
            return String.format("AckExpectation{type=%s, desc=%s}", expectedType, description);
        }
    }

    @Override
    public void channelInactive(ChannelHandlerContext ctx) throws Exception {
        // Clear pending ACKs on disconnect
        if (!pendingAcks.isEmpty()) {
            log.warn("Client disconnected with {} pending ACKs: {}",
                    pendingAcks.size(), pendingAcks);
            pendingAcks.clear();
        }
        super.channelInactive(ctx);
    }
}
