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
    private final DeliveryFreshnessTracker deliveryFreshness;
    // READY is held until a real delivery happened within this window (or the topic is empty).
    // 0 disables the gate (legacy/test constructor). Default 6h.
    private final long freshnessWindowMs;

    @Inject
    public RefreshReplayService(
            ConsumerRegistry remoteConsumers,
            StorageEngine storage,
            DataRefreshMetrics metrics,
            RefreshEventLogger refreshLogger,
            DeliveryFreshnessTracker deliveryFreshness,
            @Value("${broker.refresh.delivery-freshness-window-ms:21600000}") long freshnessWindowMs) {
        this.remoteConsumers = remoteConsumers;
        this.storage = storage;
        this.metrics = metrics;
        this.refreshLogger = refreshLogger;
        this.deliveryFreshness = deliveryFreshness;
        this.freshnessWindowMs = freshnessWindowMs;
    }

    /**
     * Backward-compatible constructor for tests that only verify replay gating
     * and metric calls, not storage-head-based gap updates. Passes a null freshness
     * tracker and window 0, which disables the delivery-freshness gate.
     */
    @Deprecated
    public RefreshReplayService(
            ConsumerRegistry remoteConsumers,
            DataRefreshMetrics metrics,
            RefreshEventLogger refreshLogger) {
        this(remoteConsumers, null, metrics, refreshLogger, null, 0L);
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
            // Delivery-freshness gate: caught-up alone is not enough to go READY/green. Require that
            // fresh data actually reached a consumer recently, UNLESS the topic legitimately has
            // nothing to deliver (empty head) — the healthy-idle exception. Guards against a refresh
            // that "completes" without any real delivery (e.g. silently broken delivery path).
            if (!deliveryFreshGateSatisfied(topic)) {
                log.info("event=refresh.ready_gated topic={} refreshId={} reason=no_recent_delivery "
                                + "windowMs={} lastDeliveryMs={} — holding READY",
                        topic, context.getRefreshId(), freshnessWindowMs,
                        deliveryFreshness != null ? deliveryFreshness.getLastSuccessfulDeliveryMs() : -1);
                return false;
            }
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

    /**
     * The READY delivery-freshness gate. Returns true (READY allowed) when either a real delivery
     * happened within the configured window, or the topic has nothing to deliver.
     *
     * <p>Disabled (always true) when the tracker is absent or the window is non-positive — the
     * legacy/test constructor and an explicit opt-out. {@code getCurrentOffset} returns the last
     * stored offset and {@code -1} for an empty topic, so {@code head < 0} is the healthy-idle case.
     *
     * <p>NOTE: this is the steady-state/LOCAL-refresh gate. Download-refresh will layer a per-topic
     * bootstrap watermark on top so a mid-bootstrap empty topic cannot satisfy the head&lt;0 branch.
     */
    private boolean deliveryFreshGateSatisfied(String topic) {
        if (deliveryFreshness == null || freshnessWindowMs <= 0) {
            return true; // gate disabled (legacy/test ctor)
        }
        long head = -1;
        try {
            if (storage != null) {
                head = storage.getCurrentOffset(topic, 0);
            }
        } catch (Exception e) {
            log.debug("deliveryFreshGate: could not read head for topic {}: {}", topic, e.getMessage());
            head = -1;
        }
        if (head < 0) {
            return true; // healthy-idle: nothing to deliver
        }
        return deliveryFreshness.deliveredWithin(freshnessWindowMs, System.currentTimeMillis());
    }
}
