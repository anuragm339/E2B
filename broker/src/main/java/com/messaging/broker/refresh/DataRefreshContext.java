package com.messaging.broker.refresh;

import java.time.Instant;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Tracks state of a data refresh operation for a single topic
 */
public class DataRefreshContext {
    private final String topic;
    private volatile DataRefreshState state;
    private final Set<String> expectedConsumers;  // From YAML config
    private final Set<String> receivedResetAcks;
    private final Set<String> receivedReadyAcks;
    private final Map<String, Long> consumerOffsets;  // Track replay progress per consumer
    private final Map<String, Boolean> consumerReplaying;  // Which consumers are actively replaying
    private final Instant startTime;
    private volatile Instant resetSentTime;
    private volatile Instant readySentTime;

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
        this.startTime = Instant.now();
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
        consumerReplaying.put(consumerId, true);  // Mark as replaying
        consumerOffsets.put(consumerId, 0L);      // Start from offset 0
    }

    public void recordReadyAck(String consumerId) {
        receivedReadyAcks.add(consumerId);
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

    // Getters and setters
    public String getTopic() { return topic; }
    public DataRefreshState getState() { return state; }
    public void setState(DataRefreshState state) { this.state = state; }
    public Set<String> getExpectedConsumers() { return expectedConsumers; }
    public Set<String> getReceivedResetAcks() { return receivedResetAcks; }
    public Set<String> getReceivedReadyAcks() { return receivedReadyAcks; }
    public Map<String, Long> getConsumerOffsets() { return consumerOffsets; }
    public Instant getStartTime() { return startTime; }
    public Instant getResetSentTime() { return resetSentTime; }
    public void setResetSentTime(Instant time) { this.resetSentTime = time; }
    public Instant getReadySentTime() { return readySentTime; }
    public void setReadySentTime(Instant time) { this.readySentTime = time; }
    public String getRefreshScope() { return refreshScope; }
    public String getRefreshType() { return refreshType; }
}
