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
import io.micronaut.context.annotation.Value;
import jakarta.inject.Inject;
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
    private final RefreshReplayWindowResolver replayWindowResolver;
    private final com.messaging.common.api.PipeConnector pipeConnector;

    @Inject
    public RefreshReplayService(
            ConsumerRegistry remoteConsumers,
            StorageEngine storage,
            DataRefreshMetrics metrics,
            RefreshEventLogger refreshLogger,
            RefreshReplayWindowResolver replayWindowResolver,
            com.messaging.common.api.PipeConnector pipeConnector) {
        this.remoteConsumers = remoteConsumers;
        this.storage = storage;
        this.metrics = metrics;
        this.refreshLogger = refreshLogger;
        this.replayWindowResolver = replayWindowResolver;
        this.pipeConnector = pipeConnector;
    }

    /**
     * Backward-compatible constructor for tests that only verify replay gating
     * and metric calls, not storage-head-based gap updates. Passes a null storage engine, an
     * unbounded resolver, and a null pipe (so the dynamic "load finished" gate is skipped).
     */
    @Deprecated
    public RefreshReplayService(
            ConsumerRegistry remoteConsumers,
            DataRefreshMetrics metrics,
            RefreshEventLogger refreshLogger) {
        this(remoteConsumers, null, metrics, refreshLogger, RefreshReplayWindowResolver.unbounded(), null);
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
                long replayTarget = context.hasReplayTargetOffset()
                        ? context.getReplayTargetOffset()
                        : storage.getCurrentOffset(topic, 0);
                long gap = Math.max(0, replayTarget - committedOffset);
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

        boolean allCaughtUp = allConsumersCaughtUp(topic, context, ackedConsumers);
        boolean allResetAcksReceived = context.allResetAcksReceived();

        if (allCaughtUp && allResetAcksReceived) {
            // READY fires once consumers have caught up to the replay target. The target is the
            // "settled" history horizon (last record with created_time older than the configured
            // settle window — see RefreshReplayWindowResolver); records inside the window are still
            // settling and continue to arrive via the normal delivery flow after READY.
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

            log.debug("Replay ready for consumer starting from offset {} (adaptive delivery will poll)",
                    context.getReplayStartOffset());

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

    private boolean allConsumersCaughtUp(String topic, RefreshContext context, Set<String> ackedConsumers) {
        if (!context.hasReplayTargetOffset()) {
            return allConsumersCaughtUp(topic, ackedConsumers);
        }
        long target = context.getReplayTargetOffset();
        if (context.isDynamicReplayTarget()) {
            // Async (pipe-fed) load: the "load finished" signal is the pipe being OUT OF DATA (its last
            // poll returned nothing → the whole backlog is in storage). Until then we are still loading,
            // so hold READY and keep /health DOWN — this is what stops a fresh boot from completing
            // instantly against empty storage. Once drained, re-evaluate the settled target against the
            // now-complete live head and check consumers against it.
            if (pipeConnector != null && !pipeConnector.isUpstreamDrained()) {
                return false; // pipe still streaming the backlog — load not finished
            }
            target = replayWindowResolver.settledTarget(topic);
            context.setReplayTargetOffset(target);
        }
        if (target < 0) {
            return true;
        }
        for (String consumerGroupTopic : ackedConsumers) {
            long committedOffset = remoteConsumers.getCommittedOffset(consumerGroupTopic);
            long requiredOffset = remoteConsumers.isLegacyGroupTopic(topic, consumerGroupTopic)
                    ? target
                    : target + 1;
            if (committedOffset < requiredOffset) {
                log.debug("Consumer {} not caught up for refresh window: offset={}, required={}, target={}",
                        consumerGroupTopic, committedOffset, requiredOffset, target);
                return false;
            }
        }
        return true;
    }
}
