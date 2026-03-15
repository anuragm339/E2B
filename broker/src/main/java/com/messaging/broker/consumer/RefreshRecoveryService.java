package com.messaging.broker.consumer;

import com.messaging.broker.monitoring.LogContext;
import com.messaging.broker.monitoring.RefreshEventLogger;
import com.messaging.broker.consumer.ConsumerRegistry;
import com.messaging.broker.monitoring.DataRefreshMetrics;
import com.messaging.common.api.PipeConnector;
import jakarta.inject.Singleton;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.time.Instant;
import java.util.*;
import java.util.concurrent.ScheduledFuture;
import java.util.stream.Collectors;

/**
 * Restores and resumes refresh workflows after restart.
 */
@Singleton
public class RefreshRecoveryService implements RefreshRecovery {
    private static final Logger log = LoggerFactory.getLogger(RefreshRecoveryService.class);

    private final ConsumerRegistry remoteConsumers;
    private final PipeConnector pipeConnector;
    private final DataRefreshMetrics metrics;
    private final RefreshStateStore stateStore;
    private final ResetPhase resetService;
    private final RefreshEventLogger refreshLogger;

    // Shared state - injected by coordinator
    private Map<String, RefreshContext> activeRefreshes;
    private Map<String, ScheduledFuture<?>> resetRetryTasks;
    private Map<String, ScheduledFuture<?>> replayCheckTasks;

    // Callbacks for scheduling tasks (provided by coordinator)
    private ScheduleResetRetryCallback scheduleResetRetryCallback;
    private ScheduleReplayCheckCallback scheduleReplayCheckCallback;
    private ScheduleReadyTimeoutCallback scheduleReadyTimeoutCallback;

    public RefreshRecoveryService(
            ConsumerRegistry remoteConsumers,
            PipeConnector pipeConnector,
            DataRefreshMetrics metrics,
            RefreshStateStore stateStore,
            ResetPhase resetService,
            RefreshEventLogger refreshLogger) {
        this.remoteConsumers = remoteConsumers;
        this.pipeConnector = pipeConnector;
        this.metrics = metrics;
        this.stateStore = stateStore;
        this.resetService = resetService;
        this.refreshLogger = refreshLogger;
    }

    /**
     * Inject shared state from coordinator.
     */
    public void setSharedState(
            Map<String, RefreshContext> activeRefreshes,
            Map<String, ScheduledFuture<?>> resetRetryTasks,
            Map<String, ScheduledFuture<?>> replayCheckTasks) {
        this.activeRefreshes = activeRefreshes;
        this.resetRetryTasks = resetRetryTasks;
        this.replayCheckTasks = replayCheckTasks;
    }

    /**
     * Inject scheduling callbacks from coordinator.
     */
    public void setSchedulingCallbacks(
            ScheduleResetRetryCallback resetRetry,
            ScheduleReplayCheckCallback replayCheck,
            ScheduleReadyTimeoutCallback readyTimeout) {
        this.scheduleResetRetryCallback = resetRetry;
        this.scheduleReplayCheckCallback = replayCheck;
        this.scheduleReadyTimeoutCallback = readyTimeout;
    }

    @Override
    public Map<String, RefreshContext> recoverAndResumeRefreshes() {
        Map<String, RefreshContext> savedRefreshes = stateStore.loadAllRefreshes();

        if (savedRefreshes.isEmpty()) {
            log.info("No saved refresh state found, starting fresh");
            return new HashMap<>();
        }

        log.info("Resuming {} refresh(es) from saved state", savedRefreshes.size());

        // Pause pipes BEFORE resuming any refresh
        pipeConnector.pausePipeCalls();

        LogContext pipeContext = LogContext.builder()
                .custom("reason", "recovery")
                .custom("refreshCount", savedRefreshes.size())
                .build();
        refreshLogger.logPipePaused(pipeContext);

        // Record startup time for all resumed refreshes
        Instant startupTime = Instant.now();

        // Group topics by refresh_id to handle batches properly
        Map<String, List<RefreshContext>> batchGroups = new HashMap<>();
        for (Map.Entry<String, RefreshContext> entry : savedRefreshes.entrySet()) {
            String topic = entry.getKey();
            RefreshContext context = entry.getValue();

            String refreshId = context.getRefreshId();
            if (refreshId == null) {
                // Backward compatibility: generate refresh_id if missing
                refreshId = String.valueOf(System.currentTimeMillis());
                context.setRefreshId(refreshId);
                log.warn("No refresh_id found for topic {}, generating new one: {}", topic, refreshId);
            }

            batchGroups.computeIfAbsent(refreshId, k -> new ArrayList<>()).add(context);
        }

        log.info("Found {} batch(es) to resume", batchGroups.size());

        // Resume all topics, grouped by batch
        for (Map.Entry<String, List<RefreshContext>> batchEntry : batchGroups.entrySet()) {
            String batchId = batchEntry.getKey();
            List<RefreshContext> topicsInBatch = batchEntry.getValue();

            log.info("Resuming batch {} with {} topic(s): {}",
                    batchId,
                    topicsInBatch.size(),
                    topicsInBatch.stream().map(RefreshContext::getTopic).collect(Collectors.toList()));

            for (RefreshContext context : topicsInBatch) {
                String topic = context.getTopic();
                log.info("Resuming topic: {} (batch: {}, state: {})", topic, batchId, context.getState());

                // Record startup to close any open downtime period
                context.recordStartup(startupTime);
                long totalDowntime = context.getTotalDowntimeSeconds();
                log.info("Recorded startup for topic: {} at {} (total downtime so far: {}s)",
                         topic, startupTime, totalDowntime);

                activeRefreshes.put(topic, context);
                stateStore.saveState(context);
                resumeRefresh(context);
            }
        }

        log.info("Successfully resumed {} active refresh(es) across {} batch(es)",
                savedRefreshes.size(), batchGroups.size());

        return savedRefreshes;
    }

    @Override
    public void resumeRefresh(RefreshContext context) {
        String topic = context.getTopic();
        RefreshState state = context.getState();

        LogContext stateContext = LogContext.builder()
                .topic(topic)
                .custom("refreshId", context.getRefreshId())
                .custom("state", state.name())
                .custom("action", "resuming")
                .build();
        refreshLogger.logStateTransition(stateContext);

        switch (state) {
            case RESET_SENT:
                resumeResetSent(topic, context);
                break;

            case REPLAYING:
                resumeReplaying(topic, context);
                break;

            case READY_SENT:
                resumeReadySent(topic, context);
                break;

            case COMPLETED:
                resumeCompleted(topic, context);
                break;

            case ABORTED:
                log.warn("Resuming from ABORTED state - manual intervention needed");
                break;

            default:
                log.warn("Unknown state during resume: {}", state);
        }
    }

    @Override
    public void repopulateMetricTimings(String topic, RefreshContext context) {
        String refreshId = context.getRefreshId();
        Instant resetSentTime = context.getResetSentTime();
        Instant readySentTime = context.getReadySentTime();

        for (String consumer : context.getExpectedConsumers()) {
            // RESET timing
            if (context.getReceivedResetAcks().contains(consumer)) {
                Instant resetAckTime = context.getResetAckTimes().get(consumer);
                if (resetSentTime != null && resetAckTime != null) {
                    long durationMs = Duration.between(resetSentTime, resetAckTime).toMillis();
                    metrics.recordResetAckDuration(topic, consumer, refreshId, durationMs);
                    log.debug("Recorded historical RESET ACK duration for {}: {}ms", consumer, durationMs);
                }
            } else {
                // Still waiting for RESET ACK
                long sentTimeMs = resetSentTime != null ? resetSentTime.toEpochMilli() : System.currentTimeMillis();
                metrics.recordResetSentAt(topic, consumer, refreshId, sentTimeMs);
            }

            // READY timing
            if (context.getReceivedReadyAcks().contains(consumer)) {
                Instant readyAckTime = context.getReadyAckTimes().get(consumer);
                if (readySentTime != null && readyAckTime != null) {
                    long durationMs = Duration.between(readySentTime, readyAckTime).toMillis();
                    metrics.recordReadyAckDuration(topic, consumer, refreshId, durationMs);
                    log.debug("Recorded historical READY ACK duration for {}: {}ms", consumer, durationMs);
                }
            } else if (readySentTime != null) {
                // Still waiting for READY ACK
                long sentTimeMs = readySentTime.toEpochMilli();
                metrics.recordReadySentAt(topic, consumer, refreshId, sentTimeMs);
            }
        }

        log.info("Re-populated timing metrics for topic {} (refresh_id: {})", topic, refreshId);
    }

    private void resumeResetSent(String topic, RefreshContext context) {
        Set<String> missingResetAcks = resetService.getMissingResetAcks(context);
        log.info("Resuming from RESET_SENT - missing {} ACK(s): {}",
                missingResetAcks.size(), missingResetAcks);

        repopulateMetricTimings(topic, context);

        // Immediately send RESET once
        remoteConsumers.broadcastResetToTopic(topic);

        LogContext resetContext = LogContext.builder()
                .topic(topic)
                .custom("refreshId", context.getRefreshId())
                .custom("consumerCount", context.getExpectedConsumers().size())
                .custom("action", "recovery")
                .build();
        refreshLogger.logResetSent(resetContext);

        // Schedule periodic retry
        if (scheduleResetRetryCallback != null) {
            scheduleResetRetryCallback.schedule(topic);
        }
    }

    private void resumeReplaying(String topic, RefreshContext context) {
        log.info("Resuming from REPLAYING - {} consumers ACKed",
                context.getReceivedResetAcks().size());

        repopulateMetricTimings(topic, context);

        // Start replay progress monitoring
        if (scheduleReplayCheckCallback != null) {
            scheduleReplayCheckCallback.schedule(topic);
        }
    }

    private void resumeReadySent(String topic, RefreshContext context) {
        log.info("Resuming from READY_SENT - waiting for ACKs from: {}",
                context.getExpectedConsumers().size() - context.getReceivedReadyAcks().size());

        repopulateMetricTimings(topic, context);

        // Schedule timeout check
        if (scheduleReadyTimeoutCallback != null) {
            scheduleReadyTimeoutCallback.schedule(topic);
        }
    }

    private void resumeCompleted(String topic, RefreshContext context) {
        log.info("Resuming from COMPLETED - cleaning up");
        activeRefreshes.remove(topic);
        stateStore.clearState(topic);
    }

    // Callback interfaces for coordinator to inject scheduling logic
    @FunctionalInterface
    public interface ScheduleResetRetryCallback {
        void schedule(String topic);
    }

    @FunctionalInterface
    public interface ScheduleReplayCheckCallback {
        void schedule(String topic);
    }

    @FunctionalInterface
    public interface ScheduleReadyTimeoutCallback {
        void schedule(String topic);
    }
}
