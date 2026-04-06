package com.messaging.broker.monitoring;

import java.util.HashMap;
import java.util.Map;

/**
 * Structured log context for consistent logging.
 *
 * Provides key-value pairs for contextual information.
 */
public class LogContext {
    private final Map<String, Object> context;

    private LogContext(Map<String, Object> context) {
        this.context = context;
    }

    public static Builder builder() {
        return new Builder();
    }

    public Map<String, Object> getContext() {
        return new HashMap<>(context);
    }

    public String get(String key) {
        Object value = context.get(key);
        return value != null ? value.toString() : null;
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        context.forEach((key, value) -> {
            if (sb.length() > 0) sb.append(", ");
            sb.append(key).append("=").append(value);
        });
        return sb.toString();
    }

    public static class Builder {
        private final Map<String, Object> context = new HashMap<>();

        public Builder topic(String topic) {
            context.put("topic", topic);
            return this;
        }

        public Builder clientId(String clientId) {
            context.put("clientId", clientId);
            return this;
        }

        public Builder traceId(String traceId) {
            context.put("traceId", traceId);
            return this;
        }

        public Builder consumerGroup(String group) {
            context.put("consumerGroup", group);
            return this;
        }

        public Builder refreshId(String refreshId) {
            context.put("refreshId", refreshId);
            return this;
        }

        public Builder offset(long offset) {
            context.put("offset", offset);
            return this;
        }

        public Builder messageType(String messageType) {
            context.put("messageType", messageType);
            return this;
        }

        public Builder state(String state) {
            context.put("state", state);
            return this;
        }

        public Builder custom(String key, Object value) {
            context.put(key, value);
            return this;
        }

        public LogContext build() {
            return new LogContext(context);
        }
    }
}
