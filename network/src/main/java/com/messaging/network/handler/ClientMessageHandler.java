package com.messaging.network.handler;

import com.messaging.common.model.BrokerMessage;
import com.messaging.network.tcp.NettyTcpClient;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.SimpleChannelInboundHandler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Netty handler for client-side message handling
 */
public class ClientMessageHandler extends SimpleChannelInboundHandler<BrokerMessage> {
    private static final Logger log = LoggerFactory.getLogger(ClientMessageHandler.class);

    private final Object connection;

    public ClientMessageHandler(Object connection) {
        this.connection = connection;
    }

    @Override
    protected void channelRead0(ChannelHandlerContext ctx, BrokerMessage msg) throws Exception {
        log.debug("Received message: type={}, id={}", msg.getType(), msg.getMessageId());

        // Use reflection to call handleIncomingMessage (to avoid circular dependency)
        try {
            connection.getClass()
                    .getMethod("handleIncomingMessage", BrokerMessage.class)
                    .invoke(connection, msg);
        } catch (Exception e) {
            log.error("Failed to handle incoming message", e);
        }
    }

    @Override
    public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) throws Exception {
        log.error("Exception in client channel", cause);
        ctx.close();
    }
}
