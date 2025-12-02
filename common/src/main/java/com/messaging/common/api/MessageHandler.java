package com.messaging.common.api;

import com.messaging.common.model.ConsumerRecord;

/**
 * Interface for consumer message handlers
 */
public interface MessageHandler {

    /**
     * Handle a consumer record
     *
     * @param record The consumer record to handle
     * @throws Exception if processing fails
     */
    void handle(ConsumerRecord record) throws Exception;
}
