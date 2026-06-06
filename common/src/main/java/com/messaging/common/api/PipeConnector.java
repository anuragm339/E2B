package com.messaging.common.api;

import com.messaging.common.model.MessageRecord;
import java.util.concurrent.CompletableFuture;
import java.util.function.Function;

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
     * @param handler Data handler that returns true on success, false on failure
     */
    void onDataReceived(Function<MessageRecord, Boolean> handler);

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
     * Pause pipe calls (for DataRefresh workflow)
     * Temporarily stops polling from parent broker
     */
    void pausePipeCalls();

    /**
     * Resume pipe calls (after DataRefresh completes)
     * Resumes polling from parent broker
     */
    void resumePipeCalls();

    /**
     * Disconnect from parent
     */
    void disconnect();

    /**
     * The pipe-level cursor: the next offset this connector will request via /pipe/poll.
     * Used by PipeConsistency lineage tracking to record the exact boundary at which a
     * new parent started producing. Implementations that don't track a cursor return -1.
     */
    default long getCurrentOffset() {
        return -1L;
    }

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
