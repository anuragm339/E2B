package com.messaging.network.tcp;

import com.messaging.common.api.NetworkServer;
import com.messaging.common.exception.ErrorCode;
import com.messaging.common.exception.NetworkException;
import com.messaging.common.model.DeliveryBatch;
import com.messaging.common.model.BrokerMessage;
import com.messaging.network.codec.BinaryMessageDecoder;
import com.messaging.network.legacy.ProtocolDetectionDecoder;
import com.messaging.network.codec.BinaryMessageEncoder;
import com.messaging.network.codec.JsonMessageDecoder;
import com.messaging.network.codec.JsonMessageEncoder;
import com.messaging.network.handler.ServerMessageHandler;
import io.micronaut.context.annotation.Requires;
import io.micronaut.context.annotation.Value;
import io.netty.bootstrap.ServerBootstrap;
import io.netty.channel.*;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioServerSocketChannel;
import io.netty.util.AbstractReferenceCounted;
import java.nio.ByteBuffer;
import java.nio.channels.WritableByteChannel;
import java.nio.charset.StandardCharsets;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import jakarta.annotation.PreDestroy;
import jakarta.inject.Singleton;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
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
            @Value("${broker.network.threads.worker:8}") int workerThreads) {

        this.bossThreads = bossThreads;
        this.workerThreads = workerThreads;
        this.clientChannels = new ConcurrentHashMap<>();
        this.handlers = new CopyOnWriteArrayList<>();
        this.disconnectHandlers = new CopyOnWriteArrayList<>();

        log.info("Initialized NettyTcpServer: bossThreads={}, workerThreads={}",
                bossThreads, workerThreads);
    }

    @Override
    public void start(int port) throws NetworkException {
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

                            // Protocol detection layer - auto-detects legacy vs modern protocol
                            // Will replace itself with appropriate decoder/encoder once detected
                            pipeline.addLast("decoder", new ProtocolDetectionDecoder());
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
            // Fatal error during server startup - should crash broker startup
            throw new NetworkException(ErrorCode.NETWORK_BIND_FAILED,
                "Failed to start server on port " + port, e);
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

    /**
     * Send a batch to a consumer.
     *
     * This method owns the full protocol detail:
     *   1. Reads routing metadata (topic, group, recordCount, totalBytes) from the DeliveryBatch
     *      and encodes the BATCH_HEADER message — broker passes one object, transport owns encoding
     *   2. Wraps the batch in a Netty FileRegion whose deallocate() closes the batch
     *   3. Sends payload bytes via the internal FileRegion path (sendfile or buffered copy)
     *   4. Closes the connection on failure so the consumer decoder resets to its initial state
     *
     * Ownership: the transport owns the batch from the moment sendBatch() is called.
     * batch.close() is called via BatchFileRegion.deallocate() for all outcomes; if the
     * header send fails before the region is created the exceptionally handler closes it directly.
     */
    @Override
    public CompletableFuture<Void> sendBatch(String clientId, String group, DeliveryBatch batch) {
        // Build BATCH_HEADER bytes — topic/count/size from batch; group from delivery call site
        byte[] topicBytes = batch.getTopic().getBytes(StandardCharsets.UTF_8);
        byte[] groupBytes = group.getBytes(StandardCharsets.UTF_8);
        ByteBuffer headerBuffer = ByteBuffer.allocate(12 + 4 + topicBytes.length + 4 + groupBytes.length);
        headerBuffer.putInt(batch.getRecordCount());
        headerBuffer.putLong(batch.getTotalBytes());
        headerBuffer.putInt(topicBytes.length);
        headerBuffer.put(topicBytes);
        headerBuffer.putInt(groupBytes.length);
        headerBuffer.put(groupBytes);
        headerBuffer.flip();
        byte[] headerBytes = new byte[headerBuffer.remaining()];
        headerBuffer.get(headerBytes);

        BrokerMessage headerMsg = new BrokerMessage(
                BrokerMessage.MessageType.BATCH_HEADER,
                System.currentTimeMillis(),
                headerBytes);

        long timeoutSeconds = 1 + (batch.getTotalBytes() / (1024 * 1024) * 10);

        // Track whether the BatchFileRegion was created so the exceptionally handler
        // knows whether deallocate() will eventually close the batch.
        AtomicReference<BatchPayloadFileRegion> regionRef = new AtomicReference<>();

        return send(clientId, headerMsg)
                .thenCompose(ignored -> {
                    BatchPayloadFileRegion nettyRegion = new BatchPayloadFileRegion(batch, clientId);
                    regionRef.set(nettyRegion);
                    return sendFileRegionInternal(clientId, nettyRegion);
                })
                .exceptionally(e -> {
                    // If the BatchPayloadFileRegion was never created (header send failed),
                    // deallocate() will never be called — close the batch directly.
                    if (regionRef.get() == null) {
                        try {
                            batch.close();
                        } catch (IOException closeEx) {
                            log.warn("Failed to close DeliveryBatch after header send failure for client {}", clientId, closeEx);
                        }
                    }
                    log.error("sendBatch failed after BATCH_HEADER sent for client {}. " +
                              "Closing connection to reset consumer decoder state.", clientId, e);
                    closeConnection(clientId);
                    throw (e instanceof RuntimeException) ? (RuntimeException) e : new RuntimeException(e);
                });
    }

    private CompletableFuture<Void> sendFileRegionInternal(String clientId, FileRegion fileRegion) {
        Channel channel = clientChannels.get(clientId);
        if (channel == null || !channel.isActive()) {
            return CompletableFuture.failedFuture(
                    new IllegalStateException("Client not connected: " + clientId));
        }

        // Check if channel is writable before attempting to send
        // This prevents queueing writes when consumer is slow or disconnecting
        if (!channel.isWritable()) {
            log.warn("Channel not writable for client: {}, backpressure detected", clientId);
            return CompletableFuture.failedFuture(
                    new IllegalStateException("Channel not writable (backpressure): " + clientId));
        }

        CompletableFuture<Void> future = new CompletableFuture<>();

        // Send FileRegion directly to Netty channel for zero-copy transfer
        // Netty will use sendfile() syscall for direct file-to-socket transfer
        channel.writeAndFlush(fileRegion).addListener((ChannelFutureListener) channelFuture -> {
            if (channelFuture.isSuccess()) {
                future.complete(null);
                log.debug("Zero-copy FileRegion sent successfully to client: {}", clientId);
            } else {
                // Only log error if it's not a connection-related issue (already handled elsewhere)
                Throwable cause = channelFuture.cause();
                String causeClassName = cause != null ? cause.getClass().getName() : "null";
                // Check for closed channel exceptions (including Netty's internal StacklessClosedChannelException)
                if (!(cause instanceof java.nio.channels.ClosedChannelException) &&
                    !causeClassName.contains("ClosedChannelException")) {
                    log.error("Failed to send FileRegion to client: {}", clientId, cause);
                } else {
                    log.debug("FileRegion send failed due to closed channel: {}", clientId);
                }
                future.completeExceptionally(cause);
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

    /**
     * Netty FileRegion adapter that wraps a DeliveryBatch for zero-copy transfer.
     *
     * Extends AbstractReferenceCounted so Netty manages the reference count.
     * When the refcount reaches zero, deallocate() is called which closes the DeliveryBatch,
     * releasing any underlying file descriptor (FileChannel) or heap resources.
     */
    private static final class BatchPayloadFileRegion extends AbstractReferenceCounted implements FileRegion {
        private static final Logger rlog = LoggerFactory.getLogger(BatchPayloadFileRegion.class);
        private final DeliveryBatch batch;
        private final String clientId;
        private long transferred = 0;

        BatchPayloadFileRegion(DeliveryBatch batch, String clientId) {
            this.batch = batch;
            this.clientId = clientId;
        }

        @Override
        public long position() {
            return 0;
        }

        @Override
        public long count() {
            return batch.getTotalBytes();
        }

        @Override
        public long transferred() {
            return transferred;
        }

        @Override
        @SuppressWarnings("deprecation")
        public long transfered() {
            return transferred;
        }

        @Override
        public long transferTo(WritableByteChannel target, long position) throws IOException {
            long written = batch.transferTo(target, position);
            if (written > 0) transferred += written;
            return written;
        }

        @Override
        protected void deallocate() {
            try {
                batch.close();
            } catch (IOException e) {
                rlog.warn("Failed to close DeliveryBatch in deallocate() for client {}", clientId, e);
            }
        }

        @Override
        public FileRegion retain() {
            super.retain();
            return this;
        }

        @Override
        public FileRegion retain(int increment) {
            super.retain(increment);
            return this;
        }

        @Override
        public FileRegion touch() {
            return this;
        }

        @Override
        public FileRegion touch(Object hint) {
            return this;
        }
    }
}
