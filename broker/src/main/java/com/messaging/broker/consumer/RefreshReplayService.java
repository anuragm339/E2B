package com.messaging.broker.consumer;

import com.messaging.broker.monitoring.LogContext;
import com.messaging.broker.monitoring.RefreshEventLogger;
import com.messaging.broker.consumer.ConsumerRegistry;
import com.messaging.broker.monitoring.DataRefreshMetrics;
import com.messaging.broker.monitoring.LogMdc;
import com.messaging.common.api.StorageEngine;
import com.messaging.common.exception.DataRefreshException;
import com.messaging.common.exception.ErrorCode;
import com.messaging.common.exception.ExceptionLogger;
import jakarta.inject.Singleton;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Instant;
import java.util.HashMap;
import java.util.List;
import java.util.Set;

/**
 * Monitors replay progress and triggers replay delivery.
 */
@Singleton
public class RefreshReplayService implements ReplayPhase {
    private static final Logger log = LoggerFactory.getLogger(RefreshReplayService.class);

    private final ConsumerRegistry remoteConsumers;
    private final StorageEngine storage;
    private final DataRefreshMetrics metrics;
    private final RefreshEventLogger refreshLogger;

    public RefreshReplayService(
            ConsumerRegistry remoteConsumers,
            StorageEngine storage,
            DataRefreshMetrics metrics,
            RefreshEventLogger refreshLogger) {
        this.remoteConsumers = remoteConsumers;
        this.storage = storage;
        this.metrics = metrics;
        this.refreshLogger = refreshLogger;
    }

    /**
     * Backward-compatible constructor for tests that only verify replay gating
     * and metric calls, not storage-head-based gap updates.
     */
    @Deprecated
    public RefreshReplayService(
            ConsumerRegistry remoteConsumers,
            DataRefreshMetrics metrics,
            RefreshEventLogger refreshLogger) {
        this(remoteConsumers, null, metrics, refreshLogger);
    }

    @Override
    public boolean checkReplayProgress(String topic, RefreshContext context) {
        java.util.Map<String, String> mdc = new HashMap<>();
        mdc.put("topic", topic);
        if (context != null && context.getRefreshId() != null) {
            mdc.put("refreshId", context.getRefreshId());
        }
        try (LogMdc.Scope ignored = LogMdc.with(mdc)) {
        if (context.getState() != RefreshState.REPLAYING) {
            return false;
        }

        // Only check consumers that have ACKed RESET
        Set<String> ackedConsumers = context.getReceivedResetAcks();
        if (ackedConsumers.isEmpty()) {
            return false;
        }

        int progressedConsumers = 0;
        for (String consumerGroupTopic : ackedConsumers) {
            long committedOffset = remoteConsumers.getCommittedOffset(consumerGroupTopic);
            if (context.recordReplayProgress(consumerGroupTopic, committedOffset)) {
                progressedConsumers++;
            }
            try {
                long storageHead = storage.getCurrentOffset(topic, 0);
                long gap = Math.max(0, storageHead - committedOffset);
                metrics.updateReplayGapOffset(topic, consumerGroupTopic, gap);
            } catch (Exception e) {
                log.debug("Could not update replay gap metric for topic {} consumer {}: {}",
                        topic, consumerGroupTopic, e.getMessage());
            }
        }

        if (progressedConsumers > 0) {
            log.debug("Replay progress advanced for topic {} on {} consumer(s); lastProgress={}",
                    topic, progressedConsumers, Instant.now());
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
                startReplayForConsumer(pair.clientId, topic, pair.groupTopic, context);
            }
        }

        return false;
        }
    }

    @Override
    public void startReplayForConsumer(String clientId, String topic, String consumerGroupTopic, RefreshContext context) {
        java.util.Map<String, String> mdc = new HashMap<>();
        mdc.put("topic", topic);
        mdc.put("clientId", clientId);
        mdc.put("consumer", consumerGroupTopic);
        if (context != null && context.getRefreshId() != null) {
            mdc.put("refreshId", context.getRefreshId());
        }
        try (LogMdc.Scope ignored = LogMdc.with(mdc)) {
        log.debug("Starting IMMEDIATE replay for consumer");

        try {
            // Record metrics: replay started
            metrics.recordReplayStarted(topic, consumerGroupTopic, context.getRefreshId());

            // Note: Adaptive delivery manager will automatically discover and deliver messages
            // No explicit trigger needed - watermark-based polling handles replay

            log.debug("Replay ready for consumer starting from offset 0 (adaptive delivery will poll)");

        } catch (Exception e) {
            DataRefreshException ex = new DataRefreshException(ErrorCode.DATA_REFRESH_REPLAY_FAILED,
                "Failed to start replay for consumer", e);
            ex.withContext("clientId", clientId);
            ex.withContext("topic", topic);
            ExceptionLogger.logError(log, ex);
            // Don't rethrow - replay will be retried automatically
        }
        }
    }

    @Override
    public boolean allConsumersCaughtUp(String topic, Set<String> ackedConsumers) {
        return remoteConsumers.allConsumersCaughtUp(topic, ackedConsumers);
    }
}
