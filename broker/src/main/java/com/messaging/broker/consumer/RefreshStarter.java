package com.messaging.broker.consumer;

import java.util.Set;
import java.util.concurrent.CompletableFuture;

/**
 * Service for initiating refresh workflows.
 */
public interface RefreshStarter {
    /**
     * Start a refresh for a topic.
     *
     * @param topic Topic to refresh
     * @return Future containing refresh result
     */
    CompletableFuture<RefreshResult> startRefresh(String topic);

    /**
     * Get expected consumers for a topic.
     *
     * @param topic Topic name
     * @return Set of consumer identifiers ("group:topic" format)
     */
    Set<String> getExpectedConsumers(String topic);

    /**
     * Generate a new refresh ID for a batch.
     *
     * @return Refresh ID (timestamp-based)
     */
    String generateRefreshId();

    /**
     * Check if a refresh is already in progress for a topic.
     *
     * @param topic Topic name
     * @return true if refresh is active
     */
    boolean isRefreshActive(String topic);

    /**
     * Force cancel an existing refresh for a topic.
     *
     * @param topic Topic name
     * @param reason Cancellation reason
     */
    void cancelExistingRefresh(String topic, String reason);
}
