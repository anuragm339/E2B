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
     * The current global pipe ingest cursor (N*) — the upstream offset the poller has reached.
     * Captured into a snapshot manifest so a restoring child can resume the pipe from it instead of
     * re-streaming from 0. Default {@code -1} (unknown) for transports that don't track a cursor.
     */
    default long getCurrentOffset() {
        return -1;
    }

    /**
     * Force the pipe cursor to {@code offset} and persist it, so the next poll resumes from there.
     * Used after a bootstrap: seed {@code N*} from a restored snapshot (resume forward), or {@code 0}
     * after a STREAM/CLOUD wipe (re-stream from the start). Call while the pipe is paused. Default no-op.
     */
    default void resetOffset(long offset) {
        // no-op for transports without a cursor
    }

    /**
     * Whether the upstream is currently "out of data" — the most recent poll returned no records, so
     * the pipe has pulled everything available beyond its cursor. A bootstrap/refresh uses this as the
     * "load finished" signal: while the backlog is still streaming in (not drained) the refresh waits
     * and {@code /health} stays DOWN; once drained, consumers can be confirmed caught up and the node
     * goes green. Default {@code true} (no gating) for transports that don't poll.
     */
    default boolean isUpstreamDrained() {
        return true;
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
