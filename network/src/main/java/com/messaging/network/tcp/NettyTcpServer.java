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
import com.messaging.network.metrics.BrokerNetworkMetrics;
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
    private final BrokerNetworkMetrics networkMetrics;

    private EventLoopGroup bossGroup;
    private EventLoopGroup workerGroup;
    private Channel serverChannel;
    // Last port we bound to, remembered so resumeAccepting() can rebind after a mid-process
    // stopAccepting() (the download-refresh wipe window) without the caller re-supplying it.
    private volatile int boundPort = -1;

    public NettyTcpServer(
            @Value("${broker.network.threads.boss:2}") int bossThreads,
            @Value("${broker.network.threads.worker:8}") int workerThreads,
            BrokerNetworkMetrics networkMetrics) {

        this.bossThreads = bossThreads;
        this.workerThreads = workerThreads;
        this.clientChannels = new ConcurrentHashMap<>();
        this.handlers = new CopyOnWriteArrayList<>();
        this.disconnectHandlers = new CopyOnWriteArrayList<>();
        this.networkMetrics = networkMetrics;
        this.networkMetrics.bindChannelStateGauges(this.clientChannels);

        log.info("Initialized NettyTcpServer: bossThreads={}, workerThreads={}",
                bossThreads, workerThreads);
    }

    public NettyTcpServer(int bossThreads, int workerThreads) {
        this(bossThreads, workerThreads, new BrokerNetworkMetrics(null));
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
                            networkMetrics.recordConnectionOpened();

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
                                networkMetrics.recordConnectionClosed();
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
                    // Allow immediate rebind of the listen port on resumeAccepting() after a
                    // mid-process stopAccepting() — the old listen socket may briefly linger.
                    .option(ChannelOption.SO_REUSEADDR, true)
                    .childOption(ChannelOption.SO_KEEPALIVE, true)
                    .childOption(ChannelOption.TCP_NODELAY, true);

            // Bind and start accepting connections (bind to all interfaces)
            ChannelFuture future = bootstrap.bind("0.0.0.0", port).sync();
            serverChannel = future.channel();
            boundPort = port;

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
            IllegalStateException ex = new IllegalStateException("Client not connected: " + clientId);
            networkMetrics.recordSendFailure(messageType(message), "control", ex);
            return CompletableFuture.failedFuture(ex);
        }

        if (!channel.isWritable()) {
            IllegalStateException ex = new IllegalStateException("Channel not writable (backpressure): " + clientId);
            networkMetrics.recordBackpressure(messageType(message), "control");
            networkMetrics.recordSendFailure(messageType(message), "control", ex);
            return CompletableFuture.failedFuture(ex);
        }

        CompletableFuture<Void> future = new CompletableFuture<>();
        long startNanos = System.nanoTime();
        String messageType = messageType(message);
        int bytes = brokerMessageSizeBytes(message);

        channel.writeAndFlush(message).addListener((ChannelFutureListener) channelFuture -> {
            if (channelFuture.isSuccess()) {
                networkMetrics.recordSendSuccess(messageType, "control", bytes, System.nanoTime() - startNanos);
                future.complete(null);
            } else {
                networkMetrics.recordSendFailure(messageType, "control", channelFuture.cause());
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
     * batch.close() is called via BatchPayloadFileRegion.deallocate() for all outcomes;
     * if the writes are never handed to Netty (channel gone / loop shut down) the batch
     * is closed directly before the failed future is returned.
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

        Channel channel = clientChannels.get(clientId);
        if (channel == null || !channel.isActive()) {
            IllegalStateException ex = new IllegalStateException("Client not connected: " + clientId);
            networkMetrics.recordSendFailure("BATCH_PAYLOAD", "file_region", ex);
            closeBatchQuietly(batch, clientId);
            return CompletableFuture.failedFuture(ex);
        }
        if (!channel.isWritable()) {
            log.warn("Channel not writable for client: {}, backpressure detected", clientId);
            IllegalStateException ex =
                    new IllegalStateException("Channel not writable (backpressure): " + clientId);
            networkMetrics.recordBackpressure("BATCH_PAYLOAD", "file_region");
            networkMetrics.recordSendFailure("BATCH_PAYLOAD", "file_region", ex);
            closeBatchQuietly(batch, clientId);
            return CompletableFuture.failedFuture(ex);
        }

        CompletableFuture<Void> headerFuture = new CompletableFuture<>();
        CompletableFuture<Void> payloadFuture = new CompletableFuture<>();
        long startNanos = System.nanoTime();
        int headerSize = brokerMessageSizeBytes(headerMsg);
        long payloadBytes = batch.getTotalBytes();

        // Enqueue BATCH_HEADER and payload in ONE event-loop task. The consumer-side
        // decoder is stateful (after a header it consumes raw bytes as payload), so a
        // control frame or another topic's batch header written by a different thread
        // must never land between the two writes. Writes submitted from other threads
        // become separate event-loop tasks and therefore cannot interleave inside this one.
        try {
            channel.eventLoop().execute(() -> {
                channel.write(headerMsg).addListener((ChannelFutureListener) f -> {
                    if (f.isSuccess()) {
                        networkMetrics.recordSendSuccess("BATCH_HEADER", "control", headerSize, System.nanoTime() - startNanos);
                        headerFuture.complete(null);
                    } else {
                        networkMetrics.recordSendFailure("BATCH_HEADER", "control", f.cause());
                        headerFuture.completeExceptionally(f.cause());
                    }
                });

                // Netty owns the region from here: deallocate() closes the batch on
                // success, failure, or cancellation — including header-write failure,
                // which fails this queued write too.
                BatchPayloadFileRegion region = new BatchPayloadFileRegion(batch, clientId);
                channel.writeAndFlush(region).addListener((ChannelFutureListener) f -> {
                    if (f.isSuccess()) {
                        networkMetrics.recordSendSuccess("BATCH_PAYLOAD", "file_region", payloadBytes, System.nanoTime() - startNanos);
                        log.debug("Zero-copy FileRegion sent successfully to client: {}", clientId);
                        payloadFuture.complete(null);
                    } else {
                        Throwable cause = f.cause();
                        String causeClassName = cause != null ? cause.getClass().getName() : "null";
                        // Closed-channel failures are routine (consumer disconnect) — keep them at debug
                        if (!(cause instanceof java.nio.channels.ClosedChannelException) &&
                            !causeClassName.contains("ClosedChannelException")) {
                            log.error("Failed to send FileRegion to client: {}", clientId, cause);
                        } else {
                            log.debug("FileRegion send failed due to closed channel: {}", clientId);
                        }
                        networkMetrics.recordSendFailure("BATCH_PAYLOAD", "file_region", cause);
                        payloadFuture.completeExceptionally(cause);
                    }
                });
            });
        } catch (java.util.concurrent.RejectedExecutionException e) {
            // Event loop shut down — the region was never handed to Netty, close directly.
            closeBatchQuietly(batch, clientId);
            return CompletableFuture.failedFuture(e);
        }

        return CompletableFuture.allOf(headerFuture, payloadFuture)
                .exceptionally(e -> {
                    log.error("sendBatch failed for client {}. " +
                              "Closing connection to reset consumer decoder state.", clientId, e);
                    closeConnection(clientId);
                    throw (e instanceof RuntimeException) ? (RuntimeException) e : new RuntimeException(e);
                });
    }

    private static void closeBatchQuietly(DeliveryBatch batch, String clientId) {
        try {
            batch.close();
        } catch (IOException closeEx) {
            log.warn("Failed to close DeliveryBatch for client {}", clientId, closeEx);
        }
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
        closeServer();
        log.info("NettyTcpServer shutdown complete");
    }

    /**
     * Stop accepting and serving consumer connections without tearing down the bean.
     *
     * <p>Closes the listening socket and the worker/boss event-loop groups (which disconnects every
     * connected client), but leaves the registered {@code handlers}/{@code disconnectHandlers} and
     * {@link #boundPort} intact so {@link #resumeAccepting()} can rebind. Used by the download-refresh
     * lifecycle to take the consumer transport down for the wipe + re-source window.
     */
    @Override
    public void stopAccepting() {
        log.info("event=network_server.stop_accepting port={} clients={}", boundPort, clientChannels.size());
        closeServer();
    }

    /**
     * Rebind the same port and resume accepting connections after {@link #stopAccepting()}.
     * Reuses the surviving handler registrations.
     */
    @Override
    public void resumeAccepting() throws NetworkException {
        if (boundPort < 0) {
            throw new NetworkException(ErrorCode.NETWORK_BIND_FAILED,
                    "resumeAccepting() called before the server was ever started");
        }
        log.info("event=network_server.resume_accepting port={}", boundPort);
        start(boundPort);
    }

    /**
     * Close the listening socket and event-loop groups, releasing the port and dropping all client
     * channels. Idempotent — safe to call from both {@link #stopAccepting()} and the {@code @PreDestroy}
     * {@link #shutdown()} (e.g. a refresh-time stop followed by context teardown). Handler
     * registrations are deliberately preserved so a subsequent {@link #start(int)} reuses them.
     */
    private synchronized void closeServer() {
        if (serverChannel != null) {
            serverChannel.close().syncUninterruptibly();
            serverChannel = null;
        }

        if (workerGroup != null) {
            workerGroup.shutdownGracefully().syncUninterruptibly();
            workerGroup = null;
        }

        if (bossGroup != null) {
            bossGroup.shutdownGracefully().syncUninterruptibly();
            bossGroup = null;
        }

        clientChannels.clear();
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

    private static String messageType(BrokerMessage message) {
        return message != null && message.getType() != null ? message.getType().name() : "UNKNOWN";
    }

    private static int brokerMessageSizeBytes(BrokerMessage message) {
        if (message == null) {
            return 0;
        }
        int payloadLength = message.getPayload() != null ? message.getPayload().length : 0;
        return 1 + 8 + 4 + payloadLength;
    }
}
