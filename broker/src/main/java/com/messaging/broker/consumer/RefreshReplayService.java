package com.messaging.broker.consumer;

import com.messaging.broker.monitoring.LogContext;
import com.messaging.broker.monitoring.RefreshEventLogger;
import com.messaging.broker.consumer.ConsumerRegistry;
import com.messaging.broker.monitoring.DataRefreshMetrics;
import com.messaging.common.exception.DataRefreshException;
import com.messaging.common.exception.ErrorCode;
import com.messaging.common.exception.ExceptionLogger;
import jakarta.inject.Singleton;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Set;

/**
 * Monitors replay progress and triggers replay delivery.
 */
@Singleton
public class RefreshReplayService implements ReplayPhase {
    private static final Logger log = LoggerFactory.getLogger(RefreshReplayService.class);

    private final ConsumerRegistry remoteConsumers;
    private final DataRefreshMetrics metrics;
    private final RefreshEventLogger refreshLogger;

    public RefreshReplayService(
            ConsumerRegistry remoteConsumers,
            DataRefreshMetrics metrics,
            RefreshEventLogger refreshLogger) {
        this.remoteConsumers = remoteConsumers;
        this.metrics = metrics;
        this.refreshLogger = refreshLogger;
    }

    @Override
    public boolean checkReplayProgress(String topic, RefreshContext context) {
        if (context.getState() != RefreshState.REPLAYING) {
            return false;
        }

        // Only check consumers that have ACKed RESET
        Set<String> ackedConsumers = context.getReceivedResetAcks();
        if (ackedConsumers.isEmpty()) {
            return false;
        }

        boolean allCaughtUp = allConsumersCaughtUp(topic, ackedConsumers);
        boolean allResetAcksReceived = context.allResetAcksReceived();

        if (allCaughtUp && allResetAcksReceived) {
            LogContext progressContext = LogContext.builder()
                    .topic(topic)
                    .custom("refreshId", context.getRefreshId())
                    .custom("caughtUpCount", ackedConsumers.size())
                    .custom("totalCount", context.getExpectedConsumers().size())
                    .custom("status", "ready for READY phase")
                    .build();
            refreshLogger.logReplayProgress(progressContext);
            return true;
        }

        // Trigger replay for each consumer
        // Use group-topic pairs so replay stays correct across multiple groups on one topic.
        List<ConsumerRegistry.ConsumerGroupTopicPair> consumerPairs =
            remoteConsumers.getConsumerGroupTopicPairs(topic);

        if (consumerPairs == null || consumerPairs.isEmpty()) {
            log.debug("No remote consumers found for topic {}, cannot trigger replay", topic);
        } else {
            log.debug("Checking replay progress for {} consumer registrations on topic {}",
                     consumerPairs.size(), topic);
            for (ConsumerRegistry.ConsumerGroupTopicPair pair : consumerPairs) {
                // Only trigger replay for consumers that ACKed RESET
                if (!ackedConsumers.contains(pair.groupTopic)) {
                    log.debug("Skipping replay trigger for clientId={} consumerGroupTopic={} (not in ackedConsumers)",
                            pair.clientId, pair.groupTopic);
                    continue;
                }
                startReplayForConsumer(pair.clientId, topic, context);
            }
        }

        return false;
    }

    @Override
    public void startReplayForConsumer(String clientId, String topic, RefreshContext context) {
        log.debug("Starting IMMEDIATE replay for consumer: {} on topic: {}", clientId, topic);

        try {
            // Record metrics: replay started
            metrics.recordReplayStarted(topic, topic, context.getRefreshId());

            // Note: Adaptive delivery manager will automatically discover and deliver messages
            // No explicit trigger needed - watermark-based polling handles replay

            log.debug("Replay ready for consumer: {} starting from offset 0 (adaptive delivery will poll)", clientId);

        } catch (Exception e) {
            DataRefreshException ex = new DataRefreshException(ErrorCode.DATA_REFRESH_REPLAY_FAILED,
                "Failed to start replay for consumer", e);
            ex.withContext("clientId", clientId);
            ex.withContext("topic", topic);
            ExceptionLogger.logError(log, ex);
            // Don't rethrow - replay will be retried automatically
        }
    }

    @Override
    public boolean allConsumersCaughtUp(String topic, Set<String> ackedConsumers) {
        return remoteConsumers.allConsumersCaughtUp(topic, ackedConsumers);
    }
}
