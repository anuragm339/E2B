package com.messaging.network.tcp;

import com.messaging.common.api.NetworkServer;
import com.messaging.common.model.BrokerMessage;
import com.messaging.network.codec.BinaryMessageDecoder;
import com.messaging.network.codec.BinaryMessageEncoder;
import com.messaging.network.handler.ServerMessageHandler;
import io.micronaut.context.annotation.Requires;
import io.micronaut.context.annotation.Value;
import io.netty.bootstrap.ServerBootstrap;
import io.netty.channel.*;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioServerSocketChannel;
import jakarta.annotation.PreDestroy;
import jakarta.inject.Singleton;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;

/**
 * Netty-based TCP server implementation
 */
@Singleton
@Requires(property = "broker.network.type", value = "tcp")
public class NettyTcpServer implements NetworkServer {
    private static final Logger log = LoggerFactory.getLogger(NettyTcpServer.class);

    private final int bossThreads;
    private final int workerThreads;
    private final ConcurrentHashMap<String, Channel> clientChannels;
    private final List<MessageHandler> handlers;
    private final List<DisconnectHandler> disconnectHandlers;

    private EventLoopGroup bossGroup;
    private EventLoopGroup workerGroup;
    private Channel serverChannel;

    public NettyTcpServer(
            @Value("${broker.network.threads.boss:2}") int bossThreads,
            @Value("${broker.network.threads.worker:16}") int workerThreads) {

        this.bossThreads = bossThreads;
        this.workerThreads = workerThreads;
        this.clientChannels = new ConcurrentHashMap<>();
        this.handlers = new CopyOnWriteArrayList<>();
        this.disconnectHandlers = new CopyOnWriteArrayList<>();

        log.info("Initialized NettyTcpServer: bossThreads={}, workerThreads={}",
                bossThreads, workerThreads);
    }

    @Override
    public void start(int port) {
        try {
            bossGroup = new NioEventLoopGroup(bossThreads);
            workerGroup = new NioEventLoopGroup(workerThreads);

            ServerBootstrap bootstrap = new ServerBootstrap();
            bootstrap.group(bossGroup, workerGroup)
                    .channel(NioServerSocketChannel.class)
                    .childHandler(new ChannelInitializer<SocketChannel>() {
                        @Override
                        protected void initChannel(SocketChannel ch) throws Exception {
                            String clientId = ch.remoteAddress().toString();
                            clientChannels.put(clientId, ch);

                            ChannelPipeline pipeline = ch.pipeline();

                            // Codecs
                            pipeline.addLast("decoder", new BinaryMessageDecoder());
                            pipeline.addLast("encoder", new BinaryMessageEncoder());

                            // Business logic handler
                            pipeline.addLast("handler", new ServerMessageHandler(clientId, handlers));

                            // Handle disconnect
                            ch.closeFuture().addListener(future -> {
                                clientChannels.remove(clientId);
                                log.info("Client disconnected: {}", clientId);

                                // Notify disconnect handlers
                                for (DisconnectHandler handler : disconnectHandlers) {
                                    try {
                                        handler.handle(clientId);
                                    } catch (Exception e) {
                                        log.error("Error in disconnect handler", e);
                                    }
                                }
                            });

                            log.info("Client connected: {}", clientId);
                        }
                    })
                    .option(ChannelOption.SO_BACKLOG, 128)
                    .childOption(ChannelOption.SO_KEEPALIVE, true)
                    .childOption(ChannelOption.TCP_NODELAY, true);

            // Bind and start accepting connections (bind to all interfaces)
            ChannelFuture future = bootstrap.bind("0.0.0.0", port).sync();
            serverChannel = future.channel();

            log.info("NettyTcpServer started on port {} (all interfaces)", port);

        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            log.error("Server startup interrupted", e);
            throw new RuntimeException("Failed to start server", e);
        }
    }

    @Override
    public void registerHandler(MessageHandler handler) {
        handlers.add(handler);
        log.info("Registered message handler: {}", handler.getClass().getSimpleName());
    }

    @Override
    public void registerDisconnectHandler(DisconnectHandler handler) {
        disconnectHandlers.add(handler);
        log.info("Registered disconnect handler: {}", handler.getClass().getSimpleName());
    }

    @Override
    public CompletableFuture<Void> send(String clientId, BrokerMessage message) {
        Channel channel = clientChannels.get(clientId);
        if (channel == null || !channel.isActive()) {
            return CompletableFuture.failedFuture(
                    new IllegalStateException("Client not connected: " + clientId));
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
    public void broadcast(BrokerMessage message) {
        for (Channel channel : clientChannels.values()) {
            if (channel.isActive()) {
                channel.writeAndFlush(message).addListener((ChannelFutureListener) future -> {
                    if (!future.isSuccess()) {
                        log.error("Failed to broadcast to client", future.cause());
                    }
                });
            }
        }
    }

    @Override
    public void closeConnection(String clientId) {
        Channel channel = clientChannels.remove(clientId);
        if (channel != null && channel.isActive()) {
            channel.close();
            log.info("Closed connection to client: {}", clientId);
        }
    }

    @Override
    public List<String> getConnectedClients() {
        return List.copyOf(clientChannels.keySet());
    }

    @Override
    @PreDestroy
    public void shutdown() {
        log.info("Shutting down NettyTcpServer...");

        if (serverChannel != null) {
            serverChannel.close().syncUninterruptibly();
        }

        if (workerGroup != null) {
            workerGroup.shutdownGracefully();
        }

        if (bossGroup != null) {
            bossGroup.shutdownGracefully();
        }

        clientChannels.clear();
        log.info("NettyTcpServer shutdown complete");
    }
}
