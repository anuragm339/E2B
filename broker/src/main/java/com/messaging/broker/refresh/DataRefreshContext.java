package com.messaging.broker.refresh;

import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Tracks state of a data refresh operation for a single topic
 */
public class DataRefreshContext {

    /**
     * Represents a single downtime period when broker was offline
     */
    public static class DowntimePeriod {
        private final Instant shutdownTime;
        private final Instant startupTime;

        public DowntimePeriod(Instant shutdownTime, Instant startupTime) {
            this.shutdownTime = shutdownTime;
            this.startupTime = startupTime;
        }

        public long getDurationSeconds() {
            return Duration.between(shutdownTime, startupTime).getSeconds();
        }

        public Instant getShutdownTime() {
            return shutdownTime;
        }

        public Instant getStartupTime() {
            return startupTime;
        }
    }
    private final String topic;
    private volatile DataRefreshState state;
    private final Set<String> expectedConsumers;  // From YAML config
    private final Set<String> receivedResetAcks;
    private final Set<String> receivedReadyAcks;
    private final Map<String, Long> consumerOffsets;  // Track replay progress per consumer
    private final Map<String, Boolean> consumerReplaying;  // Which consumers are actively replaying
    private final Map<String, Instant> resetAckTimes;  // Track when each consumer sent RESET ACK
    private final Map<String, Instant> readyAckTimes;  // Track when each consumer sent READY ACK
    private final Instant startTime;
    private volatile Instant resetSentTime;
    private volatile Instant readySentTime;

    // Downtime tracking
    private final List<DowntimePeriod> downtimePeriods;  // All completed downtime periods
    private volatile Instant lastShutdownTime;  // Current/ongoing shutdown time

    // Refresh batch tracking
    private volatile String refreshId;  // Unique identifier for the refresh batch (survives broker restarts)

    // For extensibility (future service/global refresh)
    private final String refreshScope;  // "TOPIC", "SERVICE", "GLOBAL"
    private final String refreshType;   // "NO_DELETE", "WITH_DELETE"

    public DataRefreshContext(String topic, Set<String> expectedConsumers) {
        this(topic, expectedConsumers, "TOPIC", "NO_DELETE");
    }

    public DataRefreshContext(String topic, Set<String> expectedConsumers, String refreshScope, String refreshType) {
        this.topic = topic;
        this.state = DataRefreshState.IDLE;
        this.expectedConsumers = ConcurrentHashMap.newKeySet();
        this.expectedConsumers.addAll(expectedConsumers);
        this.receivedResetAcks = ConcurrentHashMap.newKeySet();
        this.receivedReadyAcks = ConcurrentHashMap.newKeySet();
        this.consumerOffsets = new ConcurrentHashMap<>();
        this.consumerReplaying = new ConcurrentHashMap<>();
        this.resetAckTimes = new ConcurrentHashMap<>();
        this.readyAckTimes = new ConcurrentHashMap<>();
        this.startTime = Instant.now();
        this.downtimePeriods = new ArrayList<>();
        this.lastShutdownTime = null;
        this.refreshScope = refreshScope;
        this.refreshType = refreshType;
    }

    public boolean allResetAcksReceived() {
        return receivedResetAcks.containsAll(expectedConsumers);
    }

    public boolean allReadyAcksReceived() {
        return receivedReadyAcks.containsAll(expectedConsumers);
    }

    public void recordResetAck(String consumerId) {
        receivedResetAcks.add(consumerId);
        resetAckTimes.put(consumerId, Instant.now());  // Record timestamp
        consumerReplaying.put(consumerId, true);  // Mark as replaying
        consumerOffsets.put(consumerId, 0L);      // Start from offset 0
    }

    public void recordReadyAck(String consumerId) {
        receivedReadyAcks.add(consumerId);
        readyAckTimes.put(consumerId, Instant.now());  // Record timestamp
        consumerReplaying.put(consumerId, false); // No longer replaying
    }

    public void updateConsumerOffset(String consumerId, long offset) {
        consumerOffsets.put(consumerId, offset);
    }

    public boolean isConsumerReplaying(String consumerId) {
        return consumerReplaying.getOrDefault(consumerId, false);
    }

    public void markConsumerReplaying(String consumerId) {
        consumerReplaying.put(consumerId, true);
    }

    public void markConsumerNotReplaying(String consumerId) {
        consumerReplaying.put(consumerId, false);
    }

    // Downtime tracking methods
    public void recordShutdown(Instant time) {
        this.lastShutdownTime = time;
    }

    public void recordStartup(Instant time) {
        if (lastShutdownTime != null) {
            downtimePeriods.add(new DowntimePeriod(lastShutdownTime, time));
            lastShutdownTime = null;
        }
    }

    public long getTotalDowntimeSeconds() {
        return downtimePeriods.stream()
                .mapToLong(DowntimePeriod::getDurationSeconds)
                .sum();
    }

    public List<DowntimePeriod> getDowntimePeriods() {
        return new ArrayList<>(downtimePeriods);
    }

    public Instant getLastShutdownTime() {
        return lastShutdownTime;
    }

    public void setLastShutdownTime(Instant time) {
        this.lastShutdownTime = time;
    }

    public void addDowntimePeriod(Instant shutdown, Instant startup) {
        downtimePeriods.add(new DowntimePeriod(shutdown, startup));
    }

    // Getters and setters
    public String getTopic() { return topic; }
    public DataRefreshState getState() { return state; }
    public void setState(DataRefreshState state) { this.state = state; }
    public Set<String> getExpectedConsumers() { return expectedConsumers; }
    public Set<String> getReceivedResetAcks() { return receivedResetAcks; }
    public Set<String> getReceivedReadyAcks() { return receivedReadyAcks; }
    public Map<String, Long> getConsumerOffsets() { return consumerOffsets; }
    public Map<String, Instant> getResetAckTimes() { return resetAckTimes; }
    public Map<String, Instant> getReadyAckTimes() { return readyAckTimes; }
    public Instant getStartTime() { return startTime; }
    public Instant getResetSentTime() { return resetSentTime; }
    public void setResetSentTime(Instant time) { this.resetSentTime = time; }
    public Instant getReadySentTime() { return readySentTime; }
    public void setReadySentTime(Instant time) { this.readySentTime = time; }
    public String getRefreshId() { return refreshId; }
    public void setRefreshId(String refreshId) { this.refreshId = refreshId; }
    public String getRefreshScope() { return refreshScope; }
    public String getRefreshType() { return refreshType; }
}
