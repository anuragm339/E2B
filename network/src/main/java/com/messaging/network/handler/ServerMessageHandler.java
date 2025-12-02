package com.messaging.network.handler;

import com.messaging.common.api.NetworkServer;
import com.messaging.common.model.BrokerMessage;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.SimpleChannelInboundHandler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

/**
 * Netty handler that delegates to registered message handlers
 */
public class ServerMessageHandler extends SimpleChannelInboundHandler<BrokerMessage> {
    private static final Logger log = LoggerFactory.getLogger(ServerMessageHandler.class);

    private final String clientId;
    private final List<NetworkServer.MessageHandler> handlers;

    public ServerMessageHandler(String clientId, List<NetworkServer.MessageHandler> handlers) {
        this.clientId = clientId;
        this.handlers = handlers;
    }

    @Override
    protected void channelRead0(ChannelHandlerContext ctx, BrokerMessage msg) throws Exception {
        log.debug("Received message from {}: type={}, id={}",
                  clientId, msg.getType(), msg.getMessageId());

        // Delegate to all registered handlers
        for (NetworkServer.MessageHandler handler : handlers) {
            try {
                handler.handle(clientId, msg);
            } catch (Exception e) {
                log.error("Handler error for client {}", clientId, e);
            }
        }
    }

    @Override
    public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) throws Exception {
        log.error("Exception in channel for client {}", clientId, cause);
        ctx.close();
    }
}
