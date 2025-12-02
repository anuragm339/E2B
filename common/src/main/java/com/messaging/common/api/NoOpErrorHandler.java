package com.messaging.common.api;

import com.messaging.common.model.ConsumerRecord;
import jakarta.inject.Singleton;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Default no-op error handler that just logs errors
 */
@Singleton
public class NoOpErrorHandler implements ErrorHandler {
    private static final Logger log = LoggerFactory.getLogger(NoOpErrorHandler.class);

    @Override
    public void onError(ConsumerRecord record, Exception error) {
        log.error("Error processing record {}", record.getMsgKey(), error);
    }
}
