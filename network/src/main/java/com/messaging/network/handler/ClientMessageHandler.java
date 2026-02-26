package com.messaging.network.handler;

import com.messaging.common.model.BrokerMessage;
import com.messaging.common.model.ConsumerRecord;
import com.messaging.network.tcp.NettyTcpClient;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.SimpleChannelInboundHandler;
import io.netty.handler.codec.DecoderException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

/**
 * Netty handler for client-side message handling
 * Handles both BrokerMessage and List<ConsumerRecord> (from zero-copy batches)
 */
public class ClientMessageHandler extends SimpleChannelInboundHandler<Object> {
    private static final Logger log = LoggerFactory.getLogger(ClientMessageHandler.class);

    private final Object connection;

    public ClientMessageHandler(Object connection) {
        this.connection = connection;
    }

    @Override
    protected void channelRead0(ChannelHandlerContext ctx, Object msg) throws Exception {
        if (msg instanceof BrokerMessage) {
            BrokerMessage brokerMsg = (BrokerMessage) msg;
            log.debug("Received message: type={}, id={}", brokerMsg.getType(), brokerMsg.getMessageId());

            // Use reflection to call handleIncomingMessage (to avoid circular dependency)
            try {
                connection.getClass()
                        .getMethod("handleIncomingMessage", BrokerMessage.class)
                        .invoke(connection, brokerMsg);
            } catch (Exception e) {
                log.error("Failed to handle incoming message", e);
            }
        } else if (msg instanceof List) {
            // Handle zero-copy batch (List<ConsumerRecord>)
            @SuppressWarnings("unchecked")
            List<ConsumerRecord> records = (List<ConsumerRecord>) msg;
            log.info("Received zero-copy batch: {} records", records.size());

            // Create a synthetic DATA message to pass to the connection handler
            // This allows existing consumer code to work without changes
            BrokerMessage dataMsg = new BrokerMessage(
                BrokerMessage.MessageType.DATA,
                System.currentTimeMillis(),
                new byte[0]  // Payload is empty, records are separate
            );

            // Convert records list to JSON bytes for compatibility
            try {
                com.fasterxml.jackson.databind.ObjectMapper mapper = new com.fasterxml.jackson.databind.ObjectMapper();
                mapper.findAndRegisterModules();
                byte[] jsonBytes = mapper.writeValueAsBytes(records);
                dataMsg.setPayload(jsonBytes);

                connection.getClass()
                        .getMethod("handleIncomingMessage", BrokerMessage.class)
                        .invoke(connection, dataMsg);
            } catch (Exception e) {
                log.error("Failed to handle zero-copy batch", e);
            }
        } else {
            log.warn("Unknown message type: {}", msg.getClass().getName());
        }
    }

    @Override
    public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) throws Exception {
        log.error("Exception in client channel", cause);

        // Only close channel on fatal errors, not on decoding errors
        // Decoding errors are usually recoverable (corrupted batch, wrong format, etc.)
        // Let TCP keepalive handle truly dead connections
        if (cause instanceof DecoderException) {
            log.warn("Decoding error detected - keeping connection alive, error: {}", cause.getMessage());
            // Don't close - the decoder already logged the issue and moved on
            return;
        }

        // For other exceptions (IO errors, network errors, etc.), close the connection
        log.warn("Fatal error detected - closing connection");
        ctx.close();
    }
}
