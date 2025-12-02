package com.messaging.common.api;

import com.messaging.common.model.BrokerMessage;
import java.util.concurrent.CompletableFuture;
import java.util.function.Consumer;

/**
 * Network server abstraction.
 * Implementations: NettyTcpServer, GrpcServer, WebSocketServer
 */
public interface NetworkServer {

    /**
     * Start server on specified port
     * @param port Port number
     */
    void start(int port);

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
