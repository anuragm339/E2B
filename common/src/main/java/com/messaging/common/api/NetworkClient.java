package com.messaging.common.api;

import com.messaging.common.model.BrokerMessage;
import java.util.concurrent.CompletableFuture;
import java.util.function.Consumer;

/**
 * Network client abstraction.
 * Implementations: NettyTcpClient, GrpcClient, WebSocketClient
 */
public interface NetworkClient {

    /**
     * Connect to server
     * @param host Server host
     * @param port Server port
     * @return Future that completes when connected
     */
    CompletableFuture<Connection> connect(String host, int port);

    /**
     * Connection interface
     */
    interface Connection {
        /**
         * Send message to server
         * @param message Message to send
         * @return Future that completes when sent
         */
        CompletableFuture<Void> send(BrokerMessage message);

        /**
         * Register message handler for incoming messages
         * @param handler Message handler
         */
        void onMessage(Consumer<BrokerMessage> handler);

        /**
         * Check if connection is alive
         * @return true if connected
         */
        boolean isAlive();

        /**
         * Wait for ACK with timeout
         * @param messageId Message ID to wait for
         * @param timeoutMs Timeout in milliseconds
         * @return true if ACK received
         */
        boolean waitForAck(long messageId, long timeoutMs);

        /**
         * Disconnect from server
         */
        void disconnect();
    }
}
