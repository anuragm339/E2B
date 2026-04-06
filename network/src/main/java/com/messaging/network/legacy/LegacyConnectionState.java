package com.messaging.network.legacy;

import com.messaging.common.model.BrokerMessage;
import io.netty.channel.ChannelDuplexHandler;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelPromise;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
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

            // Legacy client backwards compatibility: Some clients send READY instead of ACK
            // Treat READY from client as acknowledgment of broker's READY message
            if (brokerMsg.getType() == BrokerMessage.MessageType.READY) {
                BrokerMessage resolvedMsg = resolveReadyAsAck(brokerMsg);
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
                String topic = msg.getPayload() != null && msg.getPayload().length > 0
                        ? new String(msg.getPayload(), StandardCharsets.UTF_8) : "";
                pendingAcks.addLast(new AckExpectation(BrokerMessage.MessageType.RESET_ACK, "RESET", topic));
                log.debug("Expecting RESET_ACK for topic={} (pending queue size: {})", topic, pendingAcks.size());
                break;

            case READY:
                // Capture topic from payload so refresh READY_ACK can be routed correctly.
                // Startup READY has empty payload; refresh READY carries the topic name.
                String readyTopic = msg.getPayload() != null && msg.getPayload().length > 0
                        ? new String(msg.getPayload(), StandardCharsets.UTF_8) : null;
                pendingAcks.addLast(new AckExpectation(BrokerMessage.MessageType.READY_ACK, "READY", readyTopic));
                log.debug("Expecting READY_ACK for topic={} (pending queue size: {})", readyTopic, pendingAcks.size());
                break;

            case BATCH_HEADER:
            case DATA:
                // Both BATCH_HEADER and individual DATA messages expect BATCH_ACK
                pendingAcks.addLast(new AckExpectation(BrokerMessage.MessageType.BATCH_ACK, "BATCH", null));
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

        // For RESET_ACK and READY_ACK from legacy clients, build the structured payload that the
        // handler expects: [topicLen:4][topic:var][groupLen:4]  — groupLen=0 signals legacy fallback.
        if ((expectation.expectedType == BrokerMessage.MessageType.RESET_ACK
                || expectation.expectedType == BrokerMessage.MessageType.READY_ACK)
                && expectation.topic != null && !expectation.topic.isEmpty()) {
            byte[] topicBytes = expectation.topic.getBytes(StandardCharsets.UTF_8);
            ByteBuffer buf = ByteBuffer.allocate(4 + topicBytes.length + 4);
            buf.putInt(topicBytes.length);
            buf.put(topicBytes);
            buf.putInt(0); // groupLen=0 → handler will look up the group from consumer registry
            resolvedMsg.setPayload(buf.array());
        } else {
            resolvedMsg.setPayload(genericAck.getPayload());
        }

        return resolvedMsg;
    }

    /**
     * Legacy backwards compatibility: Treat READY from client as READY_ACK
     * Some old clients send READY instead of ACK when acknowledging broker's READY message
     */
    private BrokerMessage resolveReadyAsAck(BrokerMessage readyMsg) {
        if (pendingAcks.isEmpty()) {
            log.debug("Received READY from client but no pending expectations - treating as READY_ACK");
            // Still convert to READY_ACK even if no pending expectations
            BrokerMessage resolvedMsg = new BrokerMessage();
            resolvedMsg.setType(BrokerMessage.MessageType.READY_ACK);
            resolvedMsg.setMessageId(readyMsg.getMessageId());
            resolvedMsg.setPayload(readyMsg.getPayload());
            return resolvedMsg;
        }

        // Pop the oldest expectation (FIFO)
        AckExpectation expectation = pendingAcks.removeFirst();

        log.debug("Legacy client sent READY (expected {}), converting to READY_ACK, remaining pending: {}",
                expectation.expectedType, pendingAcks.size());

        // Always convert READY to READY_ACK for backwards compatibility.
        // If the expectation carried a topic (refresh READY), build the structured payload.
        BrokerMessage resolvedMsg = new BrokerMessage();
        resolvedMsg.setType(BrokerMessage.MessageType.READY_ACK);
        resolvedMsg.setMessageId(readyMsg.getMessageId());

        if (expectation.topic != null && !expectation.topic.isEmpty()) {
            byte[] topicBytes = expectation.topic.getBytes(StandardCharsets.UTF_8);
            ByteBuffer buf = ByteBuffer.allocate(4 + topicBytes.length + 4);
            buf.putInt(topicBytes.length);
            buf.put(topicBytes);
            buf.putInt(0); // groupLen=0 → ReadyAckHandler will look up the group
            resolvedMsg.setPayload(buf.array());
        } else {
            resolvedMsg.setPayload(readyMsg.getPayload());
        }

        return resolvedMsg;
    }

    /**
     * Represents an expected ACK
     */
    private static class AckExpectation {
        final BrokerMessage.MessageType expectedType;
        final String description;
        final String topic; // non-null for RESET expectations; null otherwise

        AckExpectation(BrokerMessage.MessageType expectedType, String description, String topic) {
            this.expectedType = expectedType;
            this.description = description;
            this.topic = topic;
        }

        @Override
        public String toString() {
            return String.format("AckExpectation{type=%s, desc=%s, topic=%s}", expectedType, description, topic);
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
