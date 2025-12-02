package com.messaging.common.api;

import com.messaging.common.model.MessageRecord;
import java.util.concurrent.CompletableFuture;
import java.util.function.Consumer;

/**
 * Pipe connector abstraction for connecting to parent broker.
 * Implementations: TcpStreamPipeConnector, Http2StreamPipeConnector, ReactiveStreamPipeConnector
 */
public interface PipeConnector {

    /**
     * Connect to parent broker
     * @param parentUrl Parent broker URL (e.g., "host:port")
     * @return Future with pipe connection
     */
    CompletableFuture<PipeConnection> connectToParent(String parentUrl);

    /**
     * Register handler for incoming data stream
     * @param handler Data handler
     */
    void onDataReceived(Consumer<MessageRecord> handler);

    /**
     * Send ACK upstream to parent
     * @param offset Offset that was processed
     * @return Future that completes when ACK sent
     */
    CompletableFuture<Void> sendAck(long offset);

    /**
     * Get connection health status
     * @return Pipe health
     */
    PipeHealth getHealth();

    /**
     * Reconnect to parent on failure
     */
    void reconnect();

    /**
     * Disconnect from parent
     */
    void disconnect();

    /**
     * Pipe connection interface
     */
    interface PipeConnection {
        boolean isConnected();
        String getParentUrl();
        long getLastReceivedOffset();
    }

    /**
     * Pipe health status
     */
    enum PipeHealth {
        HEALTHY,
        DEGRADED,
        UNHEALTHY
    }
}
