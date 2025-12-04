package com.messaging.network.tcp;

import com.messaging.common.api.NetworkClient;
import com.messaging.common.model.BrokerMessage;
import com.messaging.network.codec.BinaryMessageDecoder;
import com.messaging.network.codec.BinaryMessageEncoder;
import com.messaging.network.handler.ClientMessageHandler;
import io.micronaut.context.annotation.Requires;
import io.netty.bootstrap.Bootstrap;
import io.netty.channel.*;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioSocketChannel;
import jakarta.annotation.PreDestroy;
import jakarta.inject.Singleton;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;

/**
 * Netty-based TCP client implementation
 */
@Singleton
@Requires(property = "broker.network.type", value = "tcp")
public class NettyTcpClient implements NetworkClient {
    private static final Logger log = LoggerFactory.getLogger(NettyTcpClient.class);

    private EventLoopGroup workerGroup;

    public NettyTcpClient() {
        this.workerGroup = new NioEventLoopGroup();
        log.info("Initialized NettyTcpClient");
    }

    @Override
    public CompletableFuture<Connection> connect(String host, int port) {
        CompletableFuture<Connection> future = new CompletableFuture<>();

        try {
            Bootstrap bootstrap = new Bootstrap();
            TcpConnection connection = new TcpConnection();

            bootstrap.group(workerGroup)
                    .channel(NioSocketChannel.class)
                    .option(ChannelOption.SO_KEEPALIVE, true)
                    .option(ChannelOption.TCP_NODELAY, true)
                    .handler(new ChannelInitializer<SocketChannel>() {
                        @Override
                        protected void initChannel(SocketChannel ch) throws Exception {
                            ChannelPipeline pipeline = ch.pipeline();

                            // Codecs (Binary format for efficiency)
                            pipeline.addLast("decoder", new BinaryMessageDecoder());
                            pipeline.addLast("encoder", new BinaryMessageEncoder());

                            // Business logic handler
                            pipeline.addLast("handler", new ClientMessageHandler(connection));
                        }
                    });

            // Connect to server
            ChannelFuture channelFuture = bootstrap.connect(host, port);

            channelFuture.addListener((ChannelFutureListener) cf -> {
                if (cf.isSuccess()) {
                    connection.setChannel(cf.channel());
                    future.complete(connection);
                    log.info("Connected to {}:{}", host, port);
                } else {
                    future.completeExceptionally(cf.cause());
                    log.error("Failed to connect to {}:{}", host, port, cf.cause());
                }
            });

        } catch (Exception e) {
            future.completeExceptionally(e);
        }

        return future;
    }

    @PreDestroy
    public void shutdown() {
        if (workerGroup != null) {
            workerGroup.shutdownGracefully();
        }
        log.info("NettyTcpClient shutdown complete");
    }

    /**
     * TCP connection implementation
     */
    public static class TcpConnection implements Connection {
        private Channel channel;
        private Consumer<BrokerMessage> messageHandler;
        private final ConcurrentHashMap<Long, CompletableFuture<Void>> pendingAcks;

        public TcpConnection() {
            this.pendingAcks = new ConcurrentHashMap<>();
        }

        public void setChannel(Channel channel) {
            this.channel = channel;

            // Handle disconnect
            channel.closeFuture().addListener(future -> {
                log.info("Connection closed");
                // Complete all pending ACKs with failure
                pendingAcks.values().forEach(f -> f.completeExceptionally(
                        new IllegalStateException("Connection closed")));
                pendingAcks.clear();
            });
        }

        @Override
        public CompletableFuture<Void> send(BrokerMessage message) {
            if (channel == null || !channel.isActive()) {
                return CompletableFuture.failedFuture(
                        new IllegalStateException("Not connected"));
            }

            CompletableFuture<Void> future = new CompletableFuture<>();

            channel.writeAndFlush(message).addListener((ChannelFutureListener) channelFuture -> {
                if (channelFuture.isSuccess()) {
                    future.complete(null);
                } else {
                    future.completeExceptionally(channelFuture.cause());
                }
            });

            return future;
        }

        @Override
        public void onMessage(Consumer<BrokerMessage> handler) {
            this.messageHandler = handler;
        }

        public void handleIncomingMessage(BrokerMessage message) {
            // Handle ACKs specially
            if (message.getType() == BrokerMessage.MessageType.ACK) {
                CompletableFuture<Void> ackFuture = pendingAcks.remove(message.getMessageId());
                if (ackFuture != null) {
                    ackFuture.complete(null);
                }
            }

            // Call user handler
            if (messageHandler != null) {
                try {
                    messageHandler.accept(message);
                } catch (Exception e) {
                    log.error("Error in message handler", e);
                }
            }
        }

        @Override
        public boolean isAlive() {
            return channel != null && channel.isActive();
        }

        @Override
        public boolean waitForAck(long messageId, long timeoutMs) {
            CompletableFuture<Void> ackFuture = new CompletableFuture<>();
            pendingAcks.put(messageId, ackFuture);

            try {
                ackFuture.get(timeoutMs, TimeUnit.MILLISECONDS);
                return true;
            } catch (Exception e) {
                pendingAcks.remove(messageId);
                return false;
            }
        }

        @Override
        public void disconnect() {
            if (channel != null && channel.isActive()) {
                channel.close();
                log.info("Disconnected");
            }
        }
    }
}
