package com.messaging.common.exception;

import java.time.Instant;
import java.util.HashMap;
import java.util.Map;

/**
 * Base exception for all messaging system errors.
 * Provides rich context for debugging, monitoring, and alerting.
 */
public class MessagingException extends Exception {

    private final ErrorCode errorCode;
    private final Instant timestamp;
    private final Map<String, Object> context;
    private final boolean retriable;

    public MessagingException(ErrorCode errorCode, String message) {
        super(message);
        this.errorCode = errorCode;
        this.timestamp = Instant.now();
        this.context = new HashMap<>();
        this.retriable = errorCode.isRetriable();
    }

    public MessagingException(ErrorCode errorCode, String message, Throwable cause) {
        super(message, cause);
        this.errorCode = errorCode;
        this.timestamp = Instant.now();
        this.context = new HashMap<>();
        this.retriable = errorCode.isRetriable();
    }

    public MessagingException(ErrorCode errorCode, String message, Throwable cause, boolean retriable) {
        super(message, cause);
        this.errorCode = errorCode;
        this.timestamp = Instant.now();
        this.context = new HashMap<>();
        this.retriable = retriable;
    }

    /**
     * Add context information for debugging
     */
    public MessagingException withContext(String key, Object value) {
        this.context.put(key, value);
        return this;
    }

    /**
     * Add multiple context values
     */
    public MessagingException withContext(Map<String, Object> additionalContext) {
        this.context.putAll(additionalContext);
        return this;
    }

    public ErrorCode getErrorCode() {
        return errorCode;
    }

    public Instant getTimestamp() {
        return timestamp;
    }

    public Map<String, Object> getContext() {
        return new HashMap<>(context);
    }

    public boolean isRetriable() {
        return retriable;
    }

    public String getCategory() {
        return errorCode.getCategory();
    }

    public int getCode() {
        return errorCode.getCode();
    }

    /**
     * Get structured error information for logging and monitoring
     */
    public String getStructuredMessage() {
        StringBuilder sb = new StringBuilder();
        sb.append("[").append(errorCode.name()).append("]");
        sb.append(" category=").append(errorCode.getCategory());
        sb.append(", code=").append(errorCode.getCode());
        sb.append(", retriable=").append(retriable);
        sb.append(", timestamp=").append(timestamp);
        sb.append(", message=").append(getMessage());

        if (!context.isEmpty()) {
            sb.append(", context={");
            context.forEach((k, v) -> sb.append(k).append("=").append(v).append(", "));
            sb.delete(sb.length() - 2, sb.length()); // Remove trailing ", "
            sb.append("}");
        }

        return sb.toString();
    }

    @Override
    public String toString() {
        return getStructuredMessage();
    }
}
