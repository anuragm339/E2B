package com.messaging.broker.consumer;

import com.messaging.broker.monitoring.LogContext;
import com.messaging.broker.monitoring.RefreshEventLogger;
import com.messaging.broker.consumer.ConsumerRegistry;
import com.messaging.broker.legacy.LegacyClientConfig;
import com.messaging.broker.monitoring.DataRefreshMetrics;
import com.messaging.common.api.PipeConnector;
import jakarta.inject.Inject;
import jakarta.inject.Singleton;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Instant;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
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
    private final RefreshReplayWindowResolver replayWindowResolver;
    private final LegacyClientConfig legacyClientConfig;

    // Shared state - injected by coordinator
    private Map<String, RefreshContext> activeRefreshes;
    private Map<String, ScheduledFuture<?>> resetRetryTasks;
    private Map<String, ScheduledFuture<?>> replayCheckTasks;
    private Map<String, ScheduledFuture<?>> abortWatchdogTasks;
    private Map<String, ScheduledFuture<?>> readyTimeoutTasks;
    private volatile String currentRefreshId;

    public RefreshInitiator(
            ConsumerRegistry remoteConsumers,
            PipeConnector pipeConnector,
            DataRefreshMetrics metrics,
            RefreshWorkflow stateMachine,
            RefreshStateStore stateStore,
            RefreshEventLogger refreshLogger) {
        this(remoteConsumers, pipeConnector, metrics, stateMachine, stateStore, refreshLogger,
                RefreshReplayWindowResolver.unbounded(), null);
    }

    @Inject
    public RefreshInitiator(
            ConsumerRegistry remoteConsumers,
            PipeConnector pipeConnector,
            DataRefreshMetrics metrics,
            RefreshWorkflow stateMachine,
            RefreshStateStore stateStore,
            RefreshEventLogger refreshLogger,
            RefreshReplayWindowResolver replayWindowResolver,
            LegacyClientConfig legacyClientConfig) {
        this.remoteConsumers = remoteConsumers;
        this.pipeConnector = pipeConnector;
        this.metrics = metrics;
        this.stateMachine = stateMachine;
        this.stateStore = stateStore;
        this.refreshLogger = refreshLogger;
        this.replayWindowResolver = replayWindowResolver;
        this.legacyClientConfig = legacyClientConfig;
    }

    /**
     * Inject shared state maps from coordinator.
     */
    public void setSharedState(
            Map<String, RefreshContext> activeRefreshes,
            Map<String, ScheduledFuture<?>> resetRetryTasks,
            Map<String, ScheduledFuture<?>> replayCheckTasks,
            Map<String, ScheduledFuture<?>> abortWatchdogTasks,
            Map<String, ScheduledFuture<?>> readyTimeoutTasks) {
        this.activeRefreshes = activeRefreshes;
        this.resetRetryTasks = resetRetryTasks;
        this.replayCheckTasks = replayCheckTasks;
        this.abortWatchdogTasks = abortWatchdogTasks;
        this.readyTimeoutTasks = readyTimeoutTasks;
    }

    /**
     * Set current refresh ID (managed by coordinator).
     */
    public void setCurrentRefreshId(String refreshId) {
        this.currentRefreshId = refreshId;
    }

    @Override
    public CompletableFuture<RefreshResult> startRefresh(String topic) {
        return startRefresh(topic, "LOCAL");
    }

    @Override
    public CompletableFuture<RefreshResult> startRefresh(String topic, String refreshType) {
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

        // Build the context object before acquiring the lock — construction is safe without it.
        RefreshContext context = new RefreshContext(topic, expectedConsumers, "TOPIC", refreshType);
        context.setState(RefreshState.RESET_SENT);
        context.setResetSentTime(Instant.now());
        RefreshReplayWindowResolver.RefreshReplayWindow replayWindow = replayWindowResolver.resolve(topic, refreshType);
        context.setReplayStartOffset(replayWindow.startOffset());
        context.setReplayTargetOffset(replayWindow.targetOffset());
        context.setReplayCutoffTime(replayWindow.cutoff());
        // Non-LOCAL refreshes load data asynchronously over the pipe (no bulkFetch), so the settled
        // READY target must track the live head as storage fills — not the snapshot taken now (which
        // may see empty/partial storage). LOCAL replays already-present local segments → static target.
        context.setDynamicReplayTarget(!"LOCAL".equals(refreshType));

        // Synchronized block covers both refreshId assignment AND activeRefreshes.put().
        // Previously put() was outside the block, leaving a window where two concurrent
        // startRefresh() calls for different topics both entered the isEmpty() branch,
        // each generated a new refreshId, and the second write overwrote the first —
        // causing both topics to share the same refreshId.
        synchronized (this) {
            if (activeRefreshes.isEmpty()) {
                currentRefreshId = generateRefreshId();
                metrics.resetMetricsForNewRefresh();
            }

            if (currentRefreshId == null) {
                currentRefreshId = generateRefreshId();
                log.warn("currentRefreshId was null, initializing to: {}", currentRefreshId);
            }

            context.setRefreshId(currentRefreshId);
            activeRefreshes.put(topic, context); // inside lock — visible as a unit with refreshId
        }

        // Log refresh started with structured context
        LogContext startContext = LogContext.builder()
                .topic(topic)
                .custom("refreshId", currentRefreshId)
                .custom("consumerCount", expectedConsumers.size())
                .custom("expectedConsumers", expectedConsumers.toString())
                .build();
        refreshLogger.logRefreshStarted(startContext);

        // Record metrics outside the lock. Consumer refresh does not mutate pipe-offset.properties
        // or topic folders, so it must not own pipe pause/resume.
        metrics.recordRefreshStarted(topic, refreshType, currentRefreshId);
        metrics.updateRefreshState(topic, RefreshState.RESET_SENT);

        // Persist state immediately
        stateStore.saveState(context);

        return CompletableFuture.completedFuture(
            RefreshResult.success(topic, context.getState(), expectedConsumers.size())
        );
    }

    @Override
    public Set<String> getExpectedConsumers(String topic) {
        // Prefer the consumers REGISTERED right now. Only when none are registered — the fresh/cold-boot
        // case, where the refresh fires before consumers reconnect — fall back to the CONFIGURED groups
        // (legacy service-topics) so the refresh WAITS for them via RESET-retry + late-join instead of
        // skipping the topic; the abort watchdog bounds genuinely-absent consumers. Format is
        // "group:topic", and a legacy consumer's group is its serviceName (the service-topics key), so
        // the configured identifier matches what a consumer reports on its RESET/READY ack.
        Set<String> runtime = new HashSet<>(remoteConsumers.getGroupTopicIdentifiers(topic));
        if (!runtime.isEmpty()) {
            return runtime; // consumers are connected — use them (don't wait on configured-but-absent ones)
        }
        Set<String> configured = new HashSet<>();
        if (legacyClientConfig != null && legacyClientConfig.getServiceTopics() != null) {
            for (Map.Entry<String, java.util.List<String>> e : legacyClientConfig.getServiceTopics().entrySet()) {
                if (e.getValue() != null && e.getValue().contains(topic)) {
                    configured.add(e.getKey() + ":" + topic); // serviceName == group
                }
            }
        }
        return configured;
    }

    @Override
    public String generateRefreshId() {
        // UUID guarantees uniqueness across rapid successive refreshes; millisecond timestamps
        // can collide under high load, which would invalidate the refreshId guard in abortRefreshIfStuck.
        return UUID.randomUUID().toString();
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

        // Cancel existing abort watchdog
        ScheduledFuture<?> oldWatchdog = abortWatchdogTasks.remove(topic);
        if (oldWatchdog != null) {
            oldWatchdog.cancel(false);
            log.info("Cancelled orphaned abort watchdog for topic: {}", topic);
        }

        // Cancel existing READY timeout — the old refresh may have reached READY_SENT and
        // scheduled a timeout before being force-cancelled. Cancel it so the stale task
        // doesn't fire against the new refresh context.
        ScheduledFuture<?> oldReadyTimeout = readyTimeoutTasks.remove(topic);
        if (oldReadyTimeout != null) {
            oldReadyTimeout.cancel(false);
            log.info("Cancelled orphaned READY timeout task for topic: {}", topic);
        }

        // Remove old context
        activeRefreshes.remove(topic);
        log.info("Cleaned up old refresh context for topic: {}", topic);
    }

    public String getCurrentRefreshId() {
        return currentRefreshId;
    }
}
