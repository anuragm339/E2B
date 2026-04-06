package com.messaging.broker.monitoring;

import com.messaging.broker.monitoring.ConsumerEventLogger;
import com.messaging.broker.monitoring.LogContext;
import jakarta.inject.Singleton;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * SLF4J-backed consumer event logger.
 */
@Singleton
public class DefaultConsumerEventLogger implements ConsumerEventLogger {
    private static final Logger log = LoggerFactory.getLogger(DefaultConsumerEventLogger.class);

    @Override
    public void logConsumerRegistered(LogContext context) {
        log.info("[CONSUMER] Registered: {}", context);
    }

    @Override
    public void logConsumerRegistrationDuplicate(LogContext context) {
        log.warn("[CONSUMER] Registration duplicate (already registered): {}", context);
    }

    @Override
    public void logConsumerUnregistered(LogContext context) {
        log.info("[CONSUMER] Unregistered: {}", context);
    }

    @Override
    public void logBatchDeliveryStarted(LogContext context) {
        log.debug("[DELIVERY] Batch started: {}", context);
    }

    @Override
    public void logBatchDeliverySucceeded(LogContext context) {
        log.debug("[DELIVERY] Batch succeeded: {}", context);
    }

    @Override
    public void logBatchDeliveryFailed(LogContext context) {
        log.error("[DELIVERY] Batch failed: {}", context);
    }

    @Override
    public void logBatchAckReceived(LogContext context) {
        log.debug("[ACK] Batch ACK received: {}", context);
    }

    @Override
    public void logConsumerOffsetUpdated(LogContext context) {
        log.debug("[OFFSET] Updated: {}", context);
    }

    @Override
    public void logConsumerLag(LogContext context) {
        log.debug("[LAG] Calculated: {}", context);
    }

    @Override
    public void logConsumerOffsetClamped(LogContext context) {
        log.warn("[OFFSET] Clamped: {}", context);
    }
}
