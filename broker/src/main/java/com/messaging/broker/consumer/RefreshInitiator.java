package com.messaging.broker.consumer;

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
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ScheduledFuture;

/**
 * Starts refresh workflows and initializes shared refresh state.
 */
@Singleton
public class RefreshInitiator implements RefreshStarter {
    private static final Logger log = LoggerFactory.getLogger(RefreshInitiator.class);

    private final ConsumerRegistry remoteConsumers;
    private final PipeConnector pipeConnector;
    private final DataRefreshMetrics metrics;
    private final RefreshWorkflow stateMachine;
    private final RefreshStateStore stateStore;
    private final RefreshEventLogger refreshLogger;

    // Shared state - injected by coordinator
    private Map<String, RefreshContext> activeRefreshes;
    private Map<String, ScheduledFuture<?>> resetRetryTasks;
    private Map<String, ScheduledFuture<?>> replayCheckTasks;
    private volatile String currentRefreshId;

    public RefreshInitiator(
            ConsumerRegistry remoteConsumers,
            PipeConnector pipeConnector,
            DataRefreshMetrics metrics,
            RefreshWorkflow stateMachine,
            RefreshStateStore stateStore,
            RefreshEventLogger refreshLogger) {
        this.remoteConsumers = remoteConsumers;
        this.pipeConnector = pipeConnector;
        this.metrics = metrics;
        this.stateMachine = stateMachine;
        this.stateStore = stateStore;
        this.refreshLogger = refreshLogger;
    }

    /**
     * Inject shared state maps from coordinator.
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
     * Set current refresh ID (managed by coordinator).
     */
    public void setCurrentRefreshId(String refreshId) {
        this.currentRefreshId = refreshId;
    }

    @Override
    public CompletableFuture<RefreshResult> startRefresh(String topic) {
        // Check for existing refresh and force cancel if needed
        RefreshContext existingRefresh = activeRefreshes.get(topic);
        if (existingRefresh != null) {
            cancelExistingRefresh(topic, "forcing new refresh");
        }

        // Build expected consumers
        Set<String> expectedConsumers = getExpectedConsumers(topic);

        if (expectedConsumers.isEmpty()) {
            log.warn("No consumers registered for topic: {} — skipping refresh", topic);
            return CompletableFuture.completedFuture(
                RefreshResult.success(topic, RefreshState.COMPLETED, 0)
            );
        }

        // Thread-safe initialization of currentRefreshId and metrics reset
        synchronized (this) {
            if (activeRefreshes.isEmpty()) {
                // Generate new refresh_id for this batch
                currentRefreshId = generateRefreshId();
                metrics.resetMetricsForNewRefresh();
            }

            // Defensive: ensure currentRefreshId is never null
            if (currentRefreshId == null) {
                currentRefreshId = generateRefreshId();
                log.warn("currentRefreshId was null, initializing to: {}", currentRefreshId);
            }
        }

        // Log refresh started with structured context
        LogContext startContext = LogContext.builder()
                .topic(topic)
                .custom("refreshId", currentRefreshId)
                .custom("consumerCount", expectedConsumers.size())
                .custom("expectedConsumers", expectedConsumers.toString())
                .build();
        refreshLogger.logRefreshStarted(startContext);

        // Create refresh context
        RefreshContext context = new RefreshContext(topic, expectedConsumers);
        activeRefreshes.put(topic, context);

        context.setState(RefreshState.RESET_SENT);
        context.setResetSentTime(Instant.now());
        context.setRefreshId(currentRefreshId);

        // Record metrics
        metrics.recordRefreshStarted(topic, "LOCAL", currentRefreshId);

        // Pause pipe calls before starting refresh
        pipeConnector.pausePipeCalls();

        LogContext pipeContext = LogContext.builder()
                .topic(topic)
                .custom("refreshId", currentRefreshId)
                .build();
        refreshLogger.logPipePaused(pipeContext);

        // Persist state immediately
        stateStore.saveState(context);

        return CompletableFuture.completedFuture(
            RefreshResult.success(topic, context.getState(), expectedConsumers.size())
        );
    }

    @Override
    public Set<String> getExpectedConsumers(String topic) {
        return new HashSet<>(remoteConsumers.getGroupTopicIdentifiers(topic));
    }

    @Override
    public String generateRefreshId() {
        return String.valueOf(System.currentTimeMillis());
    }

    @Override
    public boolean isRefreshActive(String topic) {
        return activeRefreshes.containsKey(topic);
    }

    @Override
    public void cancelExistingRefresh(String topic, String reason) {
        RefreshContext existingRefresh = activeRefreshes.get(topic);
        if (existingRefresh == null) {
            return;
        }

        log.warn("Refresh already in progress for topic: {} (state: {}), {}",
                 topic, existingRefresh.getState(), reason);

        // Cancel existing RESET retry task
        ScheduledFuture<?> oldResetTask = resetRetryTasks.remove(topic);
        if (oldResetTask != null) {
            oldResetTask.cancel(false);
            log.info("Cancelled orphaned RESET retry task for topic: {}", topic);
        }

        // Cancel existing replay check task
        ScheduledFuture<?> oldReplayTask = replayCheckTasks.remove(topic);
        if (oldReplayTask != null) {
            oldReplayTask.cancel(false);
            log.info("Cancelled orphaned replay check task for topic: {}", topic);
        }

        // Remove old context
        activeRefreshes.remove(topic);
        log.info("Cleaned up old refresh context for topic: {}", topic);
    }

    public String getCurrentRefreshId() {
        return currentRefreshId;
    }
}
