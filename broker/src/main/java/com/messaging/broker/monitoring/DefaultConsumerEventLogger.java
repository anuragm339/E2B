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
        try (LogMdc.Scope ignored = LogMdc.withContext(context)) {
            log.info("event=consumer.registered {}", context);
        }
    }

    @Override
    public void logConsumerRegistrationDuplicate(LogContext context) {
        try (LogMdc.Scope ignored = LogMdc.withContext(context)) {
            log.warn("event=consumer.registration_duplicate {}", context);
        }
    }

    @Override
    public void logConsumerUnregistered(LogContext context) {
        try (LogMdc.Scope ignored = LogMdc.withContext(context)) {
            log.info("event=consumer.unregistered {}", context);
        }
    }

    @Override
    public void logBatchDeliveryStarted(LogContext context) {
        try (LogMdc.Scope ignored = LogMdc.withContext(context)) {
            log.debug("event=batch_delivery.started {}", context);
        }
    }

    @Override
    public void logBatchDeliverySucceeded(LogContext context) {
        try (LogMdc.Scope ignored = LogMdc.withContext(context)) {
            log.debug("event=batch_delivery.succeeded {}", context);
        }
    }

    @Override
    public void logBatchDeliveryFailed(LogContext context) {
        try (LogMdc.Scope ignored = LogMdc.withContext(context)) {
            log.error("event=batch_delivery.failed {}", context);
        }
    }

    @Override
    public void logBatchAckReceived(LogContext context) {
        try (LogMdc.Scope ignored = LogMdc.withContext(context)) {
            log.debug("event=batch_ack.received {}", context);
        }
    }

    @Override
    public void logConsumerOffsetUpdated(LogContext context) {
        try (LogMdc.Scope ignored = LogMdc.withContext(context)) {
            log.debug("event=consumer.offset_updated {}", context);
        }
    }

    @Override
    public void logConsumerLag(LogContext context) {
        try (LogMdc.Scope ignored = LogMdc.withContext(context)) {
            log.debug("event=consumer.lag_calculated {}", context);
        }
    }

    @Override
    public void logConsumerOffsetClamped(LogContext context) {
        try (LogMdc.Scope ignored = LogMdc.withContext(context)) {
            log.warn("event=consumer.offset_clamped {}", context);
        }
    }
}
