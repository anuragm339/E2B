package com.messaging.common.api;

import com.messaging.common.exception.NetworkException;
import com.messaging.common.model.DeliveryBatch;
import com.messaging.common.model.BrokerMessage;
import java.util.concurrent.CompletableFuture;

/**
 * Network server abstraction.
 * Implementations: NettyTcpServer, GrpcServer, WebSocketServer
 */
public interface NetworkServer {

    /**
     * Start server on specified port
     * @param port Port number
     * @throws NetworkException if server fails to start
     */
    void start(int port) throws NetworkException;

    /**
     * Register handler for incoming messages
     * @param handler Message handler
     */
    void registerHandler(MessageHandler handler);

    /**
     * Send message to specific client
     * @param clientId Client identifier
     * @param message Message to send
     * @return Future that completes when sent
     */
    CompletableFuture<Void> send(String clientId, BrokerMessage message);

    /**
     * Send a batch to a consumer. The transport is responsible for:
     *   1. Encoding and sending the batch header using group, batch.getTopic(),
     *      batch.getRecordCount(), batch.getTotalBytes() (protocol detail, not broker concern)
     *   2. Streaming the payload bytes via batch.transferTo() (zero-copy, buffered, or chunked)
     *   3. Calling batch.close() in all outcomes: success, failure, cancellation, rejection.
     *
     * {@code group} is passed here (not stored in {@code batch}) because consumer group is a
     * delivery-routing annotation — the same batch bytes can be delivered to multiple groups.
     *
     * Ownership: caller must NOT close the batch after this method is invoked.
     *
     * @param clientId consumer connection identifier
     * @param group    consumer group (encoded in the wire header by the transport)
     * @param batch    delivery unit containing storage metadata + bytes (transport takes ownership)
     * @return Future that completes when the batch has been transferred
     */
    CompletableFuture<Void> sendBatch(String clientId, String group, DeliveryBatch batch);

    /**
     * Broadcast message to all connected clients
     * @param message Message to broadcast
     */
    void broadcast(BrokerMessage message);

    /**
     * Close connection to a specific client
     * @param clientId Client identifier
     */
    void closeConnection(String clientId);

    /**
     * Shutdown server gracefully
     */
    void shutdown();

    /**
     * Stop accepting and serving consumer connections WITHOUT tearing down the bean.
     *
     * <p>Closes the listening socket and disconnects every connected client, but preserves the
     * registered message/disconnect handlers and the configured bind port so the server can be
     * brought back with {@link #resumeAccepting()}. Used by the destructive download-refresh
     * lifecycle to take the consumer transport down for the wipe + re-source window so no ACK can
     * advance consumer offsets while local state is deleted.
     *
     * <p>Default no-op for transports that do not support a mid-process bounce.
     */
    default void stopAccepting() {
        shutdown();
    }

    /**
     * Resume accepting consumer connections after {@link #stopAccepting()}, rebinding the same port.
     *
     * <p>Default is a no-op; transports supporting a mid-process bounce re-bind here.
     *
     * @throws NetworkException if the server fails to rebind
     */
    default void resumeAccepting() throws NetworkException {
        // no-op for transports without mid-process bounce support
    }

    /**
     * Get list of connected client IDs
     * @return List of client IDs
     */
    java.util.List<String> getConnectedClients();

    /**
     * Register handler for client disconnections
     * @param handler Disconnect handler
     */
    void registerDisconnectHandler(DisconnectHandler handler);

    /**
     * Message handler interface
     */
    interface MessageHandler {
        void handle(String clientId, BrokerMessage message);
    }

    /**
     * Disconnect handler interface
     */
    interface DisconnectHandler {
        void handle(String clientId);
    }
}
