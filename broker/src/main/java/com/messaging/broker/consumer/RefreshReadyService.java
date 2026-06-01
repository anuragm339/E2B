package com.messaging.broker.consumer;

import com.messaging.broker.ack.AckReconciliationScheduler;
import com.messaging.broker.monitoring.LogContext;
import com.messaging.broker.monitoring.RefreshEventLogger;
import com.messaging.broker.consumer.ConsumerRegistry;
import com.messaging.broker.monitoring.DataRefreshMetrics;
import com.messaging.common.api.PipeConnector;
import jakarta.inject.Singleton;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Instant;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;

/**
 * Sends READY messages and tracks READY acknowledgments.
 */
@Singleton
public class RefreshReadyService implements ReadyPhase {
    private static final Logger log = LoggerFactory.getLogger(RefreshReadyService.class);

    private final ConsumerRegistry remoteConsumers;
    private final PipeConnector pipeConnector;
    private final DataRefreshMetrics metrics;
    private final RefreshStateStore stateStore;
    private final RefreshEventLogger refreshLogger;
    private final AckReconciliationScheduler reconciliationScheduler;

    // Shared state - injected by coordinator
    private Map<String, RefreshContext> activeRefreshes;

    public RefreshReadyService(
            ConsumerRegistry remoteConsumers,
            PipeConnector pipeConnector,
            DataRefreshMetrics metrics,
            RefreshStateStore stateStore,
            RefreshEventLogger refreshLogger,
            AckReconciliationScheduler reconciliationScheduler) {
        this.remoteConsumers = remoteConsumers;
        this.pipeConnector = pipeConnector;
        this.metrics = metrics;
        this.stateStore = stateStore;
        this.refreshLogger = refreshLogger;
        this.reconciliationScheduler = reconciliationScheduler;
    }

    /**
     * Inject shared state from coordinator.
     */
    public void setSharedState(Map<String, RefreshContext> activeRefreshes) {
        this.activeRefreshes = activeRefreshes;
    }

    @Override
    public void sendReady(String topic, RefreshContext context) {
        // Snapshot receivedResetAcks BEFORE making READY_SENT state visible to other threads.
        // registerLateJoiningConsumer (Netty thread) does a volatile re-read of state after
        // recordResetAck: any entry that observes READY_SENT is guaranteed to have missed this
        // snapshot, so it returns false and the caller sends READY directly — no duplicate.
        Set<String> readySnapshot = new HashSet<>(context.getReceivedResetAcks());  // T1
        context.setState(RefreshState.READY_SENT);                                  // T3
        context.setReadySentTime(Instant.now());
        metrics.updateRefreshState(topic, RefreshState.READY_SENT);

        // Record READY sent metrics for each expected consumer
        for (String consumer : context.getExpectedConsumers()) {
            metrics.recordReadySent(topic, consumer, context.getRefreshId());
        }

        // P1-3: Primary broadcast first — consumers that were in the set at T1.
        remoteConsumers.sendReadyToAckedConsumers(topic, readySnapshot);

        // T1-T3 supplemental send: consumers whose recordResetAck landed between T1 and T3 are
        // in the live set but not in readySnapshot. They called registerLateJoiningConsumer,
        // whose C2 re-read still saw REPLAYING (T3 not yet visible), so it returned true — but
        // they missed the primary broadcast. Diff the live set against the snapshot and send READY
        // so they don't wait READY_ACK_TIMEOUT_MS for the first checkReadyAckTimeout retry.
        // Sent after the primary broadcast so primary consumers are not delayed by this path.
        Set<String> liveAcks = new HashSet<>(context.getReceivedResetAcks());
        liveAcks.removeAll(readySnapshot);
        if (!liveAcks.isEmpty()) {
            log.debug("Sending supplemental READY to {} T1-T3 window consumer(s) for topic {}",
                    liveAcks.size(), topic);
            remoteConsumers.sendReadyToAckedConsumers(topic, liveAcks);
        }

        LogContext readyContext = LogContext.builder()
                .topic(topic)
                .custom("refreshId", context.getRefreshId())
                .custom("consumerCount", readySnapshot.size())
                .build();
        refreshLogger.logReadySent(readyContext);

        // Persist state
        stateStore.saveState(context);
    }

    @Override
    public boolean handleReadyAck(String consumerGroupTopic, String topic, RefreshContext context, String traceId) {
        if (context.getState() != RefreshState.READY_SENT) {
            log.warn("Received unexpected READY ACK from {} for topic {} (state: {}), traceId={}",
                    consumerGroupTopic, topic, context.getState(), traceId);
            return false;
        }

        if (!context.getExpectedConsumers().contains(consumerGroupTopic)) {
            log.warn("Received READY ACK from unexpected consumer: {} (expected: {}), traceId={}",
                    consumerGroupTopic, context.getExpectedConsumers(), traceId);
            return false;
        }

        if (context.getReceivedReadyAcks().contains(consumerGroupTopic)) {
            log.debug("Duplicate READY ACK from {} for topic {}, ignoring, traceId={}",
                    consumerGroupTopic, topic, traceId);
            return false;
        }

        context.recordReadyAck(consumerGroupTopic);

        LogContext ackContext = LogContext.builder()
                .traceId(traceId)
                .topic(topic)
                .custom("refreshId", context.getRefreshId())
                .custom("consumer", consumerGroupTopic)
                .custom("ackedCount", context.getReceivedReadyAcks().size())
                .custom("expectedCount", context.getExpectedConsumers().size())
                .build();
        refreshLogger.logReadyAckReceived(ackContext);

        // Record metrics: READY ACK received
        metrics.recordReadyAckReceived(topic, consumerGroupTopic, context.getRefreshId());

        // Persist state after each ACK
        stateStore.saveState(context);

        // Return true if all READY ACKs received (should complete refresh)
        return allReadyAcksReceived(context);
    }

    @Override
    public void checkReadyAckTimeout(String topic, RefreshContext context) {
        if (context == null) return;

        if (context.getState() == RefreshState.READY_SENT &&
            !context.allReadyAcksReceived()) {
            Set<String> missing = getMissingReadyAcks(context);
            log.warn("READY ACK timeout for topic {} - missing ACKs from: {}", topic, missing);
            // Retry: use the live set (not a snapshot) so late joiners registered after the
            // initial sendReady broadcast are included in this re-broadcast.
            remoteConsumers.sendReadyToAckedConsumers(topic, context.getReceivedResetAcks());
            stateStore.saveState(context);
        }
    }

    @Override
    public Set<String> getMissingReadyAcks(RefreshContext context) {
        Set<String> missing = new HashSet<>(context.getExpectedConsumers());
        missing.removeAll(context.getReceivedReadyAcks());
        return missing;
    }

    @Override
    public boolean allReadyAcksReceived(RefreshContext context) {
        return context.allReadyAcksReceived();
    }

    @Override
    public void completeRefresh(String topic, RefreshContext context) {
        context.setState(RefreshState.COMPLETED);
        metrics.updateRefreshState(topic, RefreshState.COMPLETED);
        stateStore.saveState(context);

        // Use context.getRefreshId() instead of currentRefreshId field
        // (currentRefreshId may be null if another topic completed first)
        metrics.recordRefreshCompleted(topic, "LOCAL", "SUCCESS", context.getRefreshId(), context);

        long durationMs = context.getResetSentTime() != null ?
                java.time.Duration.between(context.getResetSentTime(), Instant.now()).toMillis() : 0;

        LogContext completeContext = LogContext.builder()
                .topic(topic)
                .custom("refreshId", context.getRefreshId())
                .custom("durationMs", durationMs)
                .custom("consumerCount", context.getExpectedConsumers().size())
                .build();
        refreshLogger.logRefreshCompleted(completeContext);

        // Resume pipe calls only if NO other refreshes IN THE SAME BATCH are in progress.
        // Exclude both COMPLETED and ABORTED: an aborted sibling topic is terminal and should
        // not prevent the pipe from resuming when the remaining topics have completed.
        String batchId = context.getRefreshId();
        boolean otherRefreshesInBatchActive = activeRefreshes.values().stream()
                .anyMatch(ctx -> !ctx.getTopic().equals(topic) &&
                                 ctx.getRefreshId().equals(batchId) &&
                                 ctx.getState() != RefreshState.COMPLETED &&
                                 ctx.getState() != RefreshState.ABORTED);

        if (!otherRefreshesInBatchActive) {
            pipeConnector.resumePipeCalls();

            LogContext pipeContext = LogContext.builder()
                    .topic(topic)
                    .custom("refreshId", batchId)
                    .build();
            refreshLogger.logPipeResumed(pipeContext);
        } else {
            long activeTopicsInBatch = activeRefreshes.values().stream()
                    .filter(ctx -> ctx.getRefreshId().equals(batchId) &&
                                   ctx.getState() != RefreshState.COMPLETED &&
                                   ctx.getState() != RefreshState.ABORTED)
                    .count();
            log.info("Pipe calls remain PAUSED ({} other topic(s) in batch {} still in progress)",
                    activeTopicsInBatch, batchId);
        }

        // Clear state for this topic only
        stateStore.clearState(topic);
        log.info("Refresh state cleared for topic: {}", topic);

        // Resume reconciliation for this topic now that RocksDB is fully re-populated.
        // The checkpoint was cleared when the refresh started (pauseForTopic), so the
        // first post-refresh run will do a full scan to verify all replayed ACK entries.
        reconciliationScheduler.resumeForTopic(topic);
    }
}
