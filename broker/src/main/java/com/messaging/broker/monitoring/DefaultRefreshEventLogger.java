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
        log.info("[REFRESH] Started: {}", context);
    }

    @Override
    public void logResetSent(LogContext context) {
        log.info("[REFRESH] RESET sent: {}", context);
    }

    @Override
    public void logResetAckReceived(LogContext context) {
        log.debug("[REFRESH] RESET ACK received: {}", context);
    }

    @Override
    public void logStateTransition(LogContext context) {
        log.info("[REFRESH] State transition: {}", context);
    }

    @Override
    public void logReplayProgress(LogContext context) {
        log.debug("[REFRESH] Replay progress: {}", context);
    }

    @Override
    public void logReadySent(LogContext context) {
        log.info("[REFRESH] READY sent: {}", context);
    }

    @Override
    public void logReadyAckReceived(LogContext context) {
        log.debug("[REFRESH] READY ACK received: {}", context);
    }

    @Override
    public void logRefreshCompleted(LogContext context) {
        log.info("[REFRESH] Completed: {}", context);
    }

    @Override
    public void logRefreshAborted(LogContext context) {
        log.error("[REFRESH] Aborted: {}", context);
    }

    @Override
    public void logPipePaused(LogContext context) {
        log.info("[REFRESH] Pipe paused: {}", context);
    }

    @Override
    public void logPipeResumed(LogContext context) {
        log.info("[REFRESH] Pipe resumed: {}", context);
    }
}
