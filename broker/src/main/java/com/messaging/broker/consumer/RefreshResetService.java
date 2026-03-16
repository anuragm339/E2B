package com.messaging.broker.consumer;

import com.messaging.broker.monitoring.LogContext;
import com.messaging.broker.monitoring.RefreshEventLogger;
import com.messaging.broker.consumer.ConsumerRegistry;
import com.messaging.broker.monitoring.DataRefreshMetrics;
import jakarta.inject.Singleton;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashSet;
import java.util.Set;

/**
 * Sends RESET messages and tracks RESET acknowledgments.
 */
@Singleton
public class RefreshResetService implements ResetPhase {
    private static final Logger log = LoggerFactory.getLogger(RefreshResetService.class);

    private final ConsumerRegistry remoteConsumers;
    private final DataRefreshMetrics metrics;
    private final RefreshStateStore stateStore;
    private final RefreshEventLogger refreshLogger;

    public RefreshResetService(
            ConsumerRegistry remoteConsumers,
            DataRefreshMetrics metrics,
            RefreshStateStore stateStore,
            RefreshEventLogger refreshLogger) {
        this.remoteConsumers = remoteConsumers;
        this.metrics = metrics;
        this.stateStore = stateStore;
        this.refreshLogger = refreshLogger;
    }

    @Override
    public void sendReset(String topic, RefreshContext context) {
        // Broadcast RESET to all consumers
        remoteConsumers.broadcastResetToTopic(topic);

        LogContext resetContext = LogContext.builder()
                .topic(topic)
                .custom("refreshId", context.getRefreshId())
                .custom("consumerCount", context.getExpectedConsumers().size())
                .build();
        refreshLogger.logResetSent(resetContext);

        // Record metrics: RESET sent to each expected consumer
        for (String consumer : context.getExpectedConsumers()) {
            metrics.recordResetSent(topic, consumer, context.getRefreshId());
        }
    }

    @Override
    public boolean handleResetAck(String consumerGroupTopic, String clientId, String topic, RefreshContext context, String traceId) {
        if (!context.getExpectedConsumers().contains(consumerGroupTopic)) {
            log.warn("Received RESET ACK from unexpected consumer: {} (expected: {}), traceId={}",
                    consumerGroupTopic, context.getExpectedConsumers(), traceId);
            return false;
        }

        if (context.getReceivedResetAcks().contains(consumerGroupTopic)) {
            log.debug("Duplicate RESET ACK from {} for topic {}, ignoring, traceId={}",
                    consumerGroupTopic, topic, traceId);
            return false;
        }

        context.recordResetAck(consumerGroupTopic);

        LogContext ackContext = LogContext.builder()
                .traceId(traceId)
                .topic(topic)
                .custom("refreshId", context.getRefreshId())
                .custom("consumer", consumerGroupTopic)
                .custom("clientId", clientId)
                .custom("ackedCount", context.getReceivedResetAcks().size())
                .custom("expectedCount", context.getExpectedConsumers().size())
                .build();
        refreshLogger.logResetAckReceived(ackContext);

        // Record metrics: RESET ACK received
        metrics.recordResetAckReceived(topic, consumerGroupTopic, context.getRefreshId());

        // Reset offset to 0 for THIS consumer
        // Extract the group from the subscription identifier ("group:topic").
        String group = consumerGroupTopic.split(":")[0];
        remoteConsumers.resetConsumerOffset(clientId, topic, group, 0);
        log.debug("Reset offset to 0 for consumer: {} (group:topic={}) on topic: {}, traceId={}",
                 clientId, consumerGroupTopic, topic, traceId);

        // Initialize transfer metrics to 0 now that replay will begin for this consumer.
        // This ensures the gauge exists in Prometheus immediately — even when the topic
        // has no data to replay (empty folder). Without this, the gauge is created lazily
        // on the first batch, so an empty-topic refresh never creates it at all and
        // Grafana shows stale values from a prior refresh instead of 0.
        metrics.initializeTransferMetrics(topic, group, context.getRefreshType(), context.getRefreshId());

        // Persist state after each ACK
        stateStore.saveState(context);

        // Return true if this was the first RESET ACK (should transition to REPLAYING)
        return context.getReceivedResetAcks().size() == 1;
    }

    @Override
    public Set<String> retryResetBroadcast(String topic, RefreshContext context) {
        if (context.getState() != RefreshState.RESET_SENT) {
            return new HashSet<>();
        }

        Set<String> missingAcks = getMissingResetAcks(context);
        if (missingAcks.isEmpty()) {
            log.info("All RESET ACKs received for topic {}, stopping retry", topic);
            return new HashSet<>();
        }

        // Re-broadcast RESET to all consumers (safe to send multiple times)
        log.debug("Retrying RESET broadcast for topic {} - still waiting for {} consumer(s): {}",
                topic, missingAcks.size(), missingAcks);
        remoteConsumers.broadcastResetToTopic(topic);

        return missingAcks;
    }

    @Override
    public Set<String> getMissingResetAcks(RefreshContext context) {
        Set<String> missing = new HashSet<>(context.getExpectedConsumers());
        missing.removeAll(context.getReceivedResetAcks());
        return missing;
    }

    @Override
    public boolean allResetAcksReceived(RefreshContext context) {
        return context.allResetAcksReceived();
    }
}
