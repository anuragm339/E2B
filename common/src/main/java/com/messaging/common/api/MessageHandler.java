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
}
