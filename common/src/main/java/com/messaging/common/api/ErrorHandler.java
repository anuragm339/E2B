package com.messaging.common.api;

import com.messaging.common.model.ConsumerRecord;

/**
 * Error handler interface for consumer errors
 */
public interface ErrorHandler {

    /**
     * Called when consumer throws an error while processing a message
     *
     * @param record The consumer record that failed
     * @param error The exception that was thrown
     */
    void onError(ConsumerRecord record, Exception error);
}
