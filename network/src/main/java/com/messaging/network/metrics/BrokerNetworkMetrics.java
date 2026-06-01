package com.messaging.network.metrics;

import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.Gauge;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Timer;
import jakarta.inject.Singleton;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.ToDoubleFunction;

/**
 * Low-cardinality broker-side network metrics.
 *
 * Tracks send latency, bytes/messages sent, failures, backpressure, and current channel state.
 * Labels are intentionally limited to message type, transport path, and failure reason.
 */
@Singleton
public class BrokerNetworkMetrics {
    private final MeterRegistry registry;
    private final ConcurrentMap<String, Counter> messagesSent = new ConcurrentHashMap<>();
    private final ConcurrentMap<String, Counter> bytesSent = new ConcurrentHashMap<>();
    private final ConcurrentMap<String, Counter> sendFailures = new ConcurrentHashMap<>();
    private final ConcurrentMap<String, Counter> backpressure = new ConcurrentHashMap<>();
    private final ConcurrentMap<String, Timer> sendDurations = new ConcurrentHashMap<>();
    private final AtomicLong activeConnections = new AtomicLong(0);

    public BrokerNetworkMetrics(MeterRegistry registry) {
        this.registry = registry;

        if (registry != null) {
            Gauge.builder("broker_network_connections_active", activeConnections, AtomicLong::get)
                    .description("Current number of active broker-side network connections")
                    .register(registry);
        }
    }

    public void bindChannelStateGauges(Map<String, ? extends io.netty.channel.Channel> clientChannels) {
        if (registry == null) {
            return;
        }
        Gauge.builder("broker_network_channels_writable", clientChannels, countChannels(io.netty.channel.Channel::isWritable))
                .description("Current number of writable broker-side network channels")
                .register(registry);
        Gauge.builder("broker_network_channels_backpressured", clientChannels,
                        countChannels(channel -> channel.isActive() && !channel.isWritable()))
                .description("Current number of active broker-side network channels under backpressure")
                .register(registry);
    }

    public void recordConnectionOpened() {
        if (registry == null) {
            return;
        }
        activeConnections.incrementAndGet();
    }

    public void recordConnectionClosed() {
        if (registry == null) {
            return;
        }
        activeConnections.updateAndGet(current -> current > 0 ? current - 1 : 0);
    }

    public void recordSendSuccess(String messageType, String path, long bytes, long durationNanos) {
        if (registry == null) {
            return;
        }
        String safeType = safe(messageType);
        String safePath = safe(path);
        String key = safeType + ":" + safePath;

        messagesSent.computeIfAbsent(key, ignored ->
                Counter.builder("broker_network_messages_sent_total")
                        .description("Total broker-side network sends by message type and path")
                        .tag("message_type", safeType)
                        .tag("path", safePath)
                        .register(registry)
        ).increment();

        bytesSent.computeIfAbsent(key, ignored ->
                Counter.builder("broker_network_bytes_sent_total")
                        .description("Total broker-side network bytes sent by message type and path")
                        .baseUnit("bytes")
                        .tag("message_type", safeType)
                        .tag("path", safePath)
                        .register(registry)
        ).increment(Math.max(0, bytes));

        sendDurations.computeIfAbsent(key, ignored ->
                Timer.builder("broker_network_send_duration_seconds")
                        .description("Broker-side network send duration by message type and path")
                        .tag("message_type", safeType)
                        .tag("path", safePath)
                        .publishPercentiles(0.5, 0.95, 0.99)
                        .register(registry)
        ).record(Math.max(0, durationNanos), TimeUnit.NANOSECONDS);
    }

    public void recordSendFailure(String messageType, String path, Throwable cause) {
        if (registry == null) {
            return;
        }
        String safeType = safe(messageType);
        String safePath = safe(path);
        String reason = classify(cause);
        String key = safeType + ":" + safePath + ":" + reason;

        sendFailures.computeIfAbsent(key, ignored ->
                Counter.builder("broker_network_send_failures_total")
                        .description("Broker-side network send failures by message type, path, and reason")
                        .tag("message_type", safeType)
                        .tag("path", safePath)
                        .tag("reason", reason)
                        .register(registry)
        ).increment();
    }

    public void recordBackpressure(String messageType, String path) {
        if (registry == null) {
            return;
        }
        String safeType = safe(messageType);
        String safePath = safe(path);
        String key = safeType + ":" + safePath;

        backpressure.computeIfAbsent(key, ignored ->
                Counter.builder("broker_network_backpressure_total")
                        .description("Broker-side network backpressure occurrences by message type and path")
                        .tag("message_type", safeType)
                        .tag("path", safePath)
                        .register(registry)
        ).increment();
    }

    private static String safe(String value) {
        return (value == null || value.isBlank()) ? "unknown" : value;
    }

    private static String classify(Throwable cause) {
        if (cause == null) {
            return "unknown";
        }
        String message = cause.getMessage() == null ? "" : cause.getMessage().toLowerCase();
        String className = cause.getClass().getName();
        if (message.contains("backpressure") || message.contains("not writable")) {
            return "backpressure";
        }
        if (cause instanceof java.nio.channels.ClosedChannelException || className.contains("ClosedChannelException")) {
            return "closed_channel";
        }
        if (message.contains("broken pipe")) {
            return "broken_pipe";
        }
        if (message.contains("connection reset")) {
            return "connection_reset";
        }
        if (message.contains("timeout")) {
            return "timeout";
        }
        return "other";
    }

    private static ToDoubleFunction<Map<String, ? extends io.netty.channel.Channel>> countChannels(
            java.util.function.Predicate<io.netty.channel.Channel> predicate) {
        return channels -> channels.values().stream().filter(predicate).count();
    }
}
