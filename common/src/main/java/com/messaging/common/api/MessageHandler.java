package com.messaging.common.api;

import com.messaging.common.model.ConsumerRecord;

import java.util.List;

/**
 * Interface for consumer message handlers
 */
public interface MessageHandler {

    /**
     * Handle a batch of consumer records
     *
     * @param records The batch of consumer records to handle
     * @throws Exception if processing fails
     */
    void handleBatch(List<ConsumerRecord> records) throws Exception;

    /**
     * Called when a RESET message is received for a topic, before sending ACK.
     * Override this to perform cleanup operations like clearing caches, resetting state, etc.
     *
     * @param topic The topic being reset
     * @throws Exception if reset operation fails
     */
    default void onReset(String topic) throws Exception {
        // Default: no-op
    }

    /**
     * Called when a READY message is received for a topic, before sending ACK.
     * Override this to perform finalization operations like rebuilding indexes, validating data, etc.
     *
     * @param topic The topic that is ready
     * @throws Exception if ready operation fails
     */
    default void onReady(String topic) throws Exception {
        // Default: no-op
    }
}
