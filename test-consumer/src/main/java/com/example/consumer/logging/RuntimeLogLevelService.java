package com.example.consumer.logging;

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
                "com.example.consumer",
                "com.messaging.client",
                "com.messaging.network.tcp",
                "com.messaging.network.codec"
        ));
        features.put("consumer", List.of(
                "com.example.consumer.GenericConsumerHandler",
                "com.example.consumer.ConsumerApplication"
        ));
        features.put("legacy", List.of(
                "com.example.consumer.service.LegacyConsumerService",
                "com.example.consumer.service.LegacyBrokerConnection",
                "com.example.consumer.legacy"
        ));
        features.put("client", List.of(
                "com.messaging.client"
        ));
        features.put("network", List.of(
                "com.messaging.network.tcp",
                "com.messaging.network.codec"
        ));
        features.put("app", List.of(
                "com.example.consumer"
        ));
        return features;
    }
}
