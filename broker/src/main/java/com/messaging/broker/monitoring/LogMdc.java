package com.messaging.broker.monitoring;

import org.slf4j.MDC;

import java.util.HashMap;
import java.util.Map;

/**
 * Small MDC helper used to correlate concise INFO logs with richer DEBUG logs.
 */
public final class LogMdc {

    private LogMdc() {
    }

    public static Scope withTrace(String traceId) {
        Map<String, String> values = new HashMap<>();
        putIfPresent(values, "traceId", traceId);
        return with(values);
    }

    public static Scope withContext(LogContext context) {
        Map<String, String> values = new HashMap<>();
        if (context == null) {
            return with(values);
        }
        putIfPresent(values, "traceId", context.get("traceId"));
        putIfPresent(values, "topic", context.get("topic"));
        putIfPresent(values, "group", context.get("consumerGroup"));
        putIfPresent(values, "clientId", context.get("clientId"));
        putIfPresent(values, "refreshId", context.get("refreshId"));
        return with(values);
    }

    public static Scope with(String traceId, String topic, String group, String clientId) {
        Map<String, String> values = new HashMap<>();
        putIfPresent(values, "traceId", traceId);
        putIfPresent(values, "topic", topic);
        putIfPresent(values, "group", group);
        putIfPresent(values, "clientId", clientId);
        return with(values);
    }

    public static Scope with(Map<String, String> values) {
        Map<String, String> previous = new HashMap<>();
        values.forEach((key, value) -> {
            previous.put(key, MDC.get(key));
            if (value != null && !value.isBlank()) {
                MDC.put(key, value);
            }
        });
        return new Scope(previous, values.keySet());
    }

    private static void putIfPresent(Map<String, String> values, String key, String value) {
        if (value != null && !value.isBlank()) {
            values.put(key, value);
        }
    }

    public static final class Scope implements AutoCloseable {
        private final Map<String, String> previous;
        private final Iterable<String> keys;

        private Scope(Map<String, String> previous, Iterable<String> keys) {
            this.previous = previous;
            this.keys = keys;
        }

        @Override
        public void close() {
            for (String key : keys) {
                String oldValue = previous.get(key);
                if (oldValue == null) {
                    MDC.remove(key);
                } else {
                    MDC.put(key, oldValue);
                }
            }
        }
    }
}
