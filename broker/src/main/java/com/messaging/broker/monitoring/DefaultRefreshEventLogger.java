package com.messaging.broker.monitoring;

import com.messaging.broker.monitoring.LogContext;
import com.messaging.broker.monitoring.RefreshEventLogger;
import jakarta.inject.Singleton;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * SLF4J-backed refresh event logger.
 */
@Singleton
public class DefaultRefreshEventLogger implements RefreshEventLogger {
    private static final Logger log = LoggerFactory.getLogger(DefaultRefreshEventLogger.class);

    @Override
    public void logRefreshStarted(LogContext context) {
        try (LogMdc.Scope ignored = LogMdc.withContext(context)) {
            log.info("event=refresh.started {}", context);
        }
    }

    @Override
    public void logResetSent(LogContext context) {
        try (LogMdc.Scope ignored = LogMdc.withContext(context)) {
            log.info("event=refresh.reset_sent {}", context);
        }
    }

    @Override
    public void logResetAckReceived(LogContext context) {
        try (LogMdc.Scope ignored = LogMdc.withContext(context)) {
            log.debug("event=refresh.reset_ack_received {}", context);
        }
    }

    @Override
    public void logStateTransition(LogContext context) {
        try (LogMdc.Scope ignored = LogMdc.withContext(context)) {
            log.info("event=refresh.state_transition {}", context);
        }
    }

    @Override
    public void logReplayProgress(LogContext context) {
        try (LogMdc.Scope ignored = LogMdc.withContext(context)) {
            log.debug("event=refresh.replay_progress {}", context);
        }
    }

    @Override
    public void logReadySent(LogContext context) {
        try (LogMdc.Scope ignored = LogMdc.withContext(context)) {
            log.info("event=refresh.ready_sent {}", context);
        }
    }

    @Override
    public void logReadyAckReceived(LogContext context) {
        try (LogMdc.Scope ignored = LogMdc.withContext(context)) {
            log.debug("event=refresh.ready_ack_received {}", context);
        }
    }

    @Override
    public void logRefreshCompleted(LogContext context) {
        try (LogMdc.Scope ignored = LogMdc.withContext(context)) {
            log.info("event=refresh.completed {}", context);
        }
    }

    @Override
    public void logRefreshAborted(LogContext context) {
        try (LogMdc.Scope ignored = LogMdc.withContext(context)) {
            log.error("event=refresh.aborted {}", context);
        }
    }

    @Override
    public void logPipePaused(LogContext context) {
        try (LogMdc.Scope ignored = LogMdc.withContext(context)) {
            log.info("event=refresh.pipe_paused {}", context);
        }
    }

    @Override
    public void logPipeResumed(LogContext context) {
        try (LogMdc.Scope ignored = LogMdc.withContext(context)) {
            log.info("event=refresh.pipe_resumed {}", context);
        }
    }
}
