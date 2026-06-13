package com.messaging.broker.monitoring;

import ch.qos.logback.classic.Level;
import ch.qos.logback.classic.Logger;
import ch.qos.logback.classic.LoggerContext;
import com.messaging.common.exception.ErrorCode;
import com.messaging.common.exception.MessagingException;
import jakarta.inject.Singleton;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;

@Singleton
public class RuntimeLogLevelService {

    private final LoggerContext loggerContext;
    private final Map<String, List<String>> featureLoggers;

    public RuntimeLogLevelService() {
        this.loggerContext = (LoggerContext) LoggerFactory.getILoggerFactory();
        this.featureLoggers = createFeatureMap();
    }

    public Map<String, Object> describeAll() {
        Map<String, Object> response = new LinkedHashMap<>();
        featureLoggers.forEach((feature, loggers) -> response.put(feature, describeFeature(feature, loggers)));
        return response;
    }

    public Map<String, Object> describeFeature(String feature) {
        String normalized = normalizeFeature(feature);
        List<String> loggers = featureLoggers.get(normalized);
        if (loggers == null) {
            throw new MessagingException(ErrorCode.VALIDATION_INVALID_ARGUMENT,
                    "Unknown logging feature: " + feature);
        }
        return describeFeature(normalized, loggers);
    }

    public Map<String, Object> setFeatureLevel(String feature, String levelName) {
        String normalizedFeature = normalizeFeature(feature);
        List<String> loggers = featureLoggers.get(normalizedFeature);
        if (loggers == null) {
            throw new MessagingException(ErrorCode.VALIDATION_INVALID_ARGUMENT,
                    "Unknown logging feature: " + feature);
        }

        Level level = parseLevel(levelName);
        for (String loggerName : loggers) {
            loggerContext.getLogger(loggerName).setLevel(level);
        }
        return describeFeature(normalizedFeature, loggers);
    }

    private Map<String, Object> describeFeature(String feature, List<String> loggers) {
        Map<String, Object> response = new LinkedHashMap<>();
        response.put("feature", feature);

        List<Map<String, Object>> loggerStates = new ArrayList<>();
        for (String loggerName : loggers) {
            Logger logger = loggerContext.getLogger(loggerName);
            Map<String, Object> state = new LinkedHashMap<>();
            state.put("logger", loggerName);
            state.put("configuredLevel", logger.getLevel() == null ? "INHERIT" : logger.getLevel().levelStr);
            state.put("effectiveLevel", logger.getEffectiveLevel().levelStr);
            loggerStates.add(state);
        }
        response.put("loggers", loggerStates);
        return response;
    }

    private String normalizeFeature(String feature) {
        return feature == null ? "" : feature.trim().toLowerCase(Locale.ROOT);
    }

    private Level parseLevel(String value) {
        if (value == null || value.isBlank()) {
            throw new MessagingException(ErrorCode.VALIDATION_INVALID_ARGUMENT, "Missing log level");
        }
        String normalized = value.trim().toUpperCase(Locale.ROOT);
        return switch (normalized) {
            case "TRACE" -> Level.TRACE;
            case "DEBUG" -> Level.DEBUG;
            case "INFO" -> Level.INFO;
            case "WARN" -> Level.WARN;
            case "ERROR" -> Level.ERROR;
            case "OFF" -> Level.OFF;
            case "INHERIT", "DEFAULT", "RESET" -> null;
            default -> throw new MessagingException(ErrorCode.VALIDATION_INVALID_ARGUMENT,
                    "Unsupported log level: " + value);
        };
    }

    private Map<String, List<String>> createFeatureMap() {
        Map<String, List<String>> features = new LinkedHashMap<>();
        features.put("all", List.of(
                "com.messaging.broker",
                "com.messaging.pipe",
                "com.messaging.storage.segment",
                "com.messaging.storage.filechannel",
                "com.messaging.network.tcp",
                "com.messaging.network.codec"
        ));
        features.put("refresh", List.of(
                "com.messaging.broker.consumer.RefreshCoordinator",
                "com.messaging.broker.consumer.RefreshReplayService",
                "com.messaging.broker.consumer.RefreshInitiator",
                "com.messaging.broker.consumer.RefreshResetService",
                "com.messaging.broker.http.RefreshController"
        ));
        features.put("compaction", List.of(
                "com.messaging.broker.compaction",
                "com.messaging.broker.http.CompactionController"
        ));
        features.put("delivery", List.of(
                "com.messaging.broker.consumer.BatchDeliveryService",
                "com.messaging.broker.consumer.DeliveryScheduler",
                "com.messaging.broker.consumer.AdaptiveBatchDeliveryManager"
        ));
        features.put("ack", List.of(
                "com.messaging.broker.consumer.BatchAckService",
                "com.messaging.broker.handler.BatchAckHandler",
                "com.messaging.broker.handler.ReadyAckHandler",
                "com.messaging.broker.handler.ResetAckHandler"
        ));
        features.put("pipe", List.of(
                "com.messaging.pipe"
        ));
        features.put("storage", List.of(
                "com.messaging.storage.segment",
                "com.messaging.storage.filechannel"
        ));
        features.put("network", List.of(
                "com.messaging.network.tcp",
                "com.messaging.network.codec"
        ));
        features.put("consumer", List.of(
                "com.messaging.broker.consumer",
                "com.messaging.broker.handler.SubscribeHandler"
        ));
        features.put("legacy", List.of(
                "com.messaging.broker.legacy",
                "com.messaging.broker.consumer.ConsumerRegistry"
        ));
        features.put("monitoring", List.of(
                "com.messaging.broker.monitoring"
        ));
        return features;
    }
}
