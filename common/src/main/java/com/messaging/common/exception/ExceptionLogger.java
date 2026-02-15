package com.messaging.common.exception;

import org.slf4j.Logger;

/**
 * Utility for structured exception logging with rich context.
 * Provides consistent exception logging across all components.
 */
public class ExceptionLogger {

    /**
     * Log a MessagingException with full context at ERROR level
     */
    public static void logError(Logger log, MessagingException ex) {
        log.error(ex.getStructuredMessage(), ex.getCause() != null ? ex.getCause() : ex);
    }

    /**
     * Log a MessagingException with full context at WARN level
     */
    public static void logWarn(Logger log, MessagingException ex) {
        log.warn(ex.getStructuredMessage(), ex.getCause() != null ? ex.getCause() : ex);
    }

    /**
     * Log a MessagingException with conditional level based on retriability
     * Retriable: WARN, Non-retriable: ERROR
     */
    public static void logConditional(Logger log, MessagingException ex) {
        if (ex.isRetriable()) {
            logWarn(log, ex);
        } else {
            logError(log, ex);
        }
    }

    /**
     * Log exception and rethrow it
     */
    public static <T extends MessagingException> T logAndThrow(Logger log, T ex) throws T {
        logError(log, ex);
        throw ex;
    }

    /**
     * Log exception at specified level and rethrow it
     */
    public static <T extends MessagingException> T logAndThrow(Logger log, T ex, LogLevel level) throws T {
        switch (level) {
            case ERROR:
                logError(log, ex);
                break;
            case WARN:
                logWarn(log, ex);
                break;
            case INFO:
                log.info(ex.getStructuredMessage(), ex.getCause() != null ? ex.getCause() : ex);
                break;
            case DEBUG:
                log.debug(ex.getStructuredMessage(), ex.getCause() != null ? ex.getCause() : ex);
                break;
        }
        throw ex;
    }

    /**
     * Wrap generic exception into MessagingException and log it
     */
    public static MessagingException wrapAndLog(Logger log, Exception ex, ErrorCode errorCode, String message) {
        MessagingException wrapped = new MessagingException(errorCode, message, ex);
        logError(log, wrapped);
        return wrapped;
    }

    /**
     * Create summary for monitoring/alerting (single line, parseable)
     */
    public static String getMonitoringSummary(MessagingException ex) {
        return String.format("error_code=%s category=%s retriable=%s timestamp=%s",
                ex.getErrorCode().name(),
                ex.getCategory(),
                ex.isRetriable(),
                ex.getTimestamp());
    }

    public enum LogLevel {
        ERROR, WARN, INFO, DEBUG
    }
}
