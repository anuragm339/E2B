package com.messaging.broker.refresh;

import io.micronaut.context.annotation.Value;
import jakarta.inject.Singleton;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardCopyOption;
import java.time.Instant;
import java.util.*;

@Singleton
public class DataRefreshStateStore {
    private static final Logger log = LoggerFactory.getLogger(DataRefreshStateStore.class);
    private static final String STATE_FILE_NAME = "data-refresh-state.properties";

    private final Path stateFilePath;

    public DataRefreshStateStore(@Value("${broker.storage.data-dir:./data}") String dataDir) {
        Path dataDirPath = Paths.get(dataDir);
        this.stateFilePath = dataDirPath.resolve(STATE_FILE_NAME);

        try {
            Files.createDirectories(dataDirPath);
        } catch (IOException e) {
            log.error("Failed to create data directory", e);
        }
    }

    /**
     * Save refresh context for a specific topic
     * Supports multiple concurrent topic refreshes
     */
    public synchronized void saveState(DataRefreshContext context) {
        if (context == null) return;

        // Load existing state to preserve other topics
        Properties props = loadAllStates();

        String topicPrefix = "topic." + context.getTopic();

        // Save state for this topic
        props.setProperty(topicPrefix + ".state", context.getState().toString());
        props.setProperty(topicPrefix + ".start.time", context.getStartTime().toString());

        if (context.getResetSentTime() != null) {
            props.setProperty(topicPrefix + ".reset.sent.time", context.getResetSentTime().toString());
        }

        if (context.getReadySentTime() != null) {
            props.setProperty(topicPrefix + ".ready.sent.time", context.getReadySentTime().toString());
        }

        // Save refresh ID (for per-batch metrics tracking)
        if (context.getRefreshId() != null) {
            props.setProperty(topicPrefix + ".refresh.id", context.getRefreshId());
        }

        // Save downtime periods
        List<DataRefreshContext.DowntimePeriod> downtimePeriods = context.getDowntimePeriods();
        props.setProperty(topicPrefix + ".downtime.count", String.valueOf(downtimePeriods.size()));

        for (int i = 0; i < downtimePeriods.size(); i++) {
            DataRefreshContext.DowntimePeriod period = downtimePeriods.get(i);
            props.setProperty(topicPrefix + ".downtime." + i + ".shutdown",
                             period.getShutdownTime().toString());
            props.setProperty(topicPrefix + ".downtime." + i + ".startup",
                             period.getStartupTime().toString());
        }

        // Save last shutdown time if exists (for ongoing outage)
        Instant lastShutdown = context.getLastShutdownTime();
        if (lastShutdown != null) {
            props.setProperty(topicPrefix + ".last.shutdown.time", lastShutdown.toString());
        }

        // Expected consumers for this topic
        props.setProperty(topicPrefix + ".expected.consumers",
                         String.join(",", context.getExpectedConsumers()));

        // Per-consumer state for this topic
        for (String consumerId : context.getExpectedConsumers()) {
            String consumerPrefix = topicPrefix + ".consumer." + consumerId;

            props.setProperty(consumerPrefix + ".reset.ack.received",
                            String.valueOf(context.getReceivedResetAcks().contains(consumerId)));

            props.setProperty(consumerPrefix + ".ready.ack.received",
                            String.valueOf(context.getReceivedReadyAcks().contains(consumerId)));

            Long offset = context.getConsumerOffsets().get(consumerId);
            if (offset != null) {
                props.setProperty(consumerPrefix + ".current.offset", String.valueOf(offset));
            }

            props.setProperty(consumerPrefix + ".replaying",
                            String.valueOf(context.isConsumerReplaying(consumerId)));
        }

        // Update list of active topics
        Set<String> activeTopics = getActiveTopics(props);
        activeTopics.add(context.getTopic());
        props.setProperty("active.refresh.topics", String.join(",", activeTopics));

        // Atomic write: temp file + rename
        try {
            Path tempFile = stateFilePath.getParent().resolve(STATE_FILE_NAME + ".tmp");

            try (OutputStream out = Files.newOutputStream(tempFile)) {
                props.store(out, "DataRefresh State - Updated: " + Instant.now());
            }

            Files.move(tempFile, stateFilePath, StandardCopyOption.ATOMIC_MOVE, StandardCopyOption.REPLACE_EXISTING);
            log.debug("Saved refresh state for topic: {} (total active topics: {})",
                     context.getTopic(), activeTopics.size());

        } catch (IOException e) {
            log.error("Failed to save refresh state", e);
        }
    }

    /**
     * Load all states (for internal use)
     */
    private Properties loadAllStates() {
        Properties props = new Properties();
        if (!Files.exists(stateFilePath)) {
            return props;
        }

        try (InputStream in = Files.newInputStream(stateFilePath)) {
            props.load(in);
        } catch (IOException e) {
            log.error("Failed to load states", e);
        }
        return props;
    }

    /**
     * Get set of active topics from properties
     */
    private Set<String> getActiveTopics(Properties props) {
        String topicsStr = props.getProperty("active.refresh.topics", "");
        if (topicsStr.isEmpty()) {
            return new HashSet<>();
        }
        return new HashSet<>(Arrays.asList(topicsStr.split(",")));
    }

    /**
     * Load all refresh contexts from properties file (new multi-topic format)
     * Returns map of topic -> context for all active refreshes
     */
    public synchronized Map<String, DataRefreshContext> loadAllRefreshes() {
        Map<String, DataRefreshContext> contexts = new HashMap<>();

        if (!Files.exists(stateFilePath)) {
            return contexts;
        }

        try (InputStream in = Files.newInputStream(stateFilePath)) {
            Properties props = new Properties();
            props.load(in);

            // Get list of active topics
            Set<String> activeTopics = getActiveTopics(props);
            if (activeTopics.isEmpty()) {
                return contexts;
            }

            // Load context for each topic
            for (String topic : activeTopics) {
                try {
                    DataRefreshContext context = loadTopicState(props, topic);
                    if (context != null) {
                        contexts.put(topic, context);
                        log.info("Loaded refresh state for topic: {} (state={})",
                                topic, context.getState());
                    }
                } catch (Exception e) {
                    log.error("Failed to load state for topic: {}", topic, e);
                }
            }

            return contexts;

        } catch (Exception e) {
            log.error("Failed to load refresh states", e);
            return contexts;
        }
    }

    /**
     * Load state for a specific topic from properties
     */
    private DataRefreshContext loadTopicState(Properties props, String topic) {
        String topicPrefix = "topic." + topic;

        String stateStr = props.getProperty(topicPrefix + ".state");
        if (stateStr == null) {
            return null;
        }

        DataRefreshState state = DataRefreshState.valueOf(stateStr);

        String consumersStr = props.getProperty(topicPrefix + ".expected.consumers");
        if (consumersStr == null || consumersStr.isEmpty()) {
            return null;
        }

        Set<String> expectedConsumers = new HashSet<>(Arrays.asList(consumersStr.split(",")));

        DataRefreshContext context = new DataRefreshContext(topic, expectedConsumers);
        context.setState(state);

        // Parse timestamps
        String startTimeStr = props.getProperty(topicPrefix + ".start.time");
        if (startTimeStr != null) {
            // Note: context.startTime is set in constructor, we can't override it
            // This is fine as the start time is mostly informational
        }

        String resetSentTimeStr = props.getProperty(topicPrefix + ".reset.sent.time");
        if (resetSentTimeStr != null) {
            context.setResetSentTime(Instant.parse(resetSentTimeStr));
        }

        String readySentTimeStr = props.getProperty(topicPrefix + ".ready.sent.time");
        if (readySentTimeStr != null) {
            context.setReadySentTime(Instant.parse(readySentTimeStr));
        }

        // Load refresh ID (for per-batch metrics tracking)
        String refreshIdStr = props.getProperty(topicPrefix + ".refresh.id");
        if (refreshIdStr != null) {
            context.setRefreshId(refreshIdStr);
        }

        // Load downtime periods
        int downtimeCount = Integer.parseInt(
            props.getProperty(topicPrefix + ".downtime.count", "0"));

        for (int i = 0; i < downtimeCount; i++) {
            String shutdownStr = props.getProperty(topicPrefix + ".downtime." + i + ".shutdown");
            String startupStr = props.getProperty(topicPrefix + ".downtime." + i + ".startup");

            if (shutdownStr != null && startupStr != null) {
                context.addDowntimePeriod(
                    Instant.parse(shutdownStr),
                    Instant.parse(startupStr)
                );
            }
        }

        // Load last shutdown time (for ongoing outage)
        String lastShutdownStr = props.getProperty(topicPrefix + ".last.shutdown.time");
        if (lastShutdownStr != null) {
            context.setLastShutdownTime(Instant.parse(lastShutdownStr));
        }

        log.info("Loaded {} downtime period(s) for topic: {}", downtimeCount, topic);

        // Restore per-consumer state
        for (String consumerId : expectedConsumers) {
            String consumerPrefix = topicPrefix + ".consumer." + consumerId;

            boolean resetAck = Boolean.parseBoolean(
                    props.getProperty(consumerPrefix + ".reset.ack.received", "false"));
            if (resetAck) {
                context.getReceivedResetAcks().add(consumerId);
            }

            boolean readyAck = Boolean.parseBoolean(
                    props.getProperty(consumerPrefix + ".ready.ack.received", "false"));
            if (readyAck) {
                context.getReceivedReadyAcks().add(consumerId);
            }

            String offsetStr = props.getProperty(consumerPrefix + ".current.offset");
            if (offsetStr != null) {
                context.updateConsumerOffset(consumerId, Long.parseLong(offsetStr));
            }

            boolean replaying = Boolean.parseBoolean(
                    props.getProperty(consumerPrefix + ".replaying", "false"));
            if (replaying) {
                context.markConsumerReplaying(consumerId);
            }
        }

        return context;
    }

    /**
     * Load refresh context from properties file (backward compatibility)
     * @deprecated Use loadAllRefreshes() for new format
     */
    @Deprecated
    public synchronized DataRefreshContext loadState() {
        if (!Files.exists(stateFilePath)) {
            return null;
        }

        try (InputStream in = Files.newInputStream(stateFilePath)) {
            Properties props = new Properties();
            props.load(in);

            // Try new format first
            Set<String> activeTopics = getActiveTopics(props);
            if (!activeTopics.isEmpty()) {
                // New format - return first topic (for backward compatibility)
                String firstTopic = activeTopics.iterator().next();
                log.warn("Using deprecated loadState() with new multi-topic format, loading only first topic: {}", firstTopic);
                return loadTopicState(props, firstTopic);
            }

            // Try old format
            String topic = props.getProperty("active.refresh.topic");
            if (topic == null) return null;

            String stateStr = props.getProperty("active.refresh.state");
            DataRefreshState state = DataRefreshState.valueOf(stateStr);

            String consumersStr = props.getProperty("active.refresh.expected.consumers");
            Set<String> expectedConsumers = new HashSet<>(Arrays.asList(consumersStr.split(",")));

            DataRefreshContext context = new DataRefreshContext(topic, expectedConsumers);
            context.setState(state);

            // Restore per-consumer state (old format)
            for (String consumerId : expectedConsumers) {
                String prefix = "consumer." + consumerId;

                boolean resetAck = Boolean.parseBoolean(props.getProperty(prefix + ".reset.ack.received", "false"));
                if (resetAck) {
                    context.getReceivedResetAcks().add(consumerId);
                }

                boolean readyAck = Boolean.parseBoolean(props.getProperty(prefix + ".ready.ack.received", "false"));
                if (readyAck) {
                    context.getReceivedReadyAcks().add(consumerId);
                }

                String offsetStr = props.getProperty(prefix + ".current.offset");
                if (offsetStr != null) {
                    context.updateConsumerOffset(consumerId, Long.parseLong(offsetStr));
                }
            }

            log.info("Loaded refresh state (old format) for topic: {} (state={})", topic, state);
            return context;

        } catch (Exception e) {
            log.error("Failed to load refresh state", e);
            return null;
        }
    }

    /**
     * Clear state for a specific topic after refresh completes
     */
    public synchronized void clearState(String topic) {
        Properties props = loadAllStates();

        String topicPrefix = "topic." + topic;

        // Remove all properties for this topic
        Set<String> keysToRemove = new HashSet<>();
        for (String key : props.stringPropertyNames()) {
            if (key.startsWith(topicPrefix)) {
                keysToRemove.add(key);
            }
        }

        for (String key : keysToRemove) {
            props.remove(key);
        }

        // Update active topics list
        Set<String> activeTopics = getActiveTopics(props);
        activeTopics.remove(topic);

        if (activeTopics.isEmpty()) {
            props.remove("active.refresh.topics");
        } else {
            props.setProperty("active.refresh.topics", String.join(",", activeTopics));
        }

        // Save updated state (or delete file if empty)
        try {
            if (props.isEmpty()) {
                Files.deleteIfExists(stateFilePath);
                log.info("Cleared refresh state file (no active topics)");
            } else {
                Path tempFile = stateFilePath.getParent().resolve(STATE_FILE_NAME + ".tmp");

                try (OutputStream out = Files.newOutputStream(tempFile)) {
                    props.store(out, "DataRefresh State - Updated: " + Instant.now());
                }

                Files.move(tempFile, stateFilePath, StandardCopyOption.ATOMIC_MOVE, StandardCopyOption.REPLACE_EXISTING);
                log.info("Cleared refresh state for topic: {} (remaining active topics: {})",
                        topic, activeTopics.size());
            }
        } catch (IOException e) {
            log.error("Failed to clear refresh state for topic: {}", topic, e);
        }
    }

    /**
     * Clear all state (for backward compatibility)
     */
    public synchronized void clearState() {
        try {
            Files.deleteIfExists(stateFilePath);
            log.info("Cleared all refresh state");
        } catch (IOException e) {
            log.error("Failed to clear refresh state file", e);
        }
    }
}
