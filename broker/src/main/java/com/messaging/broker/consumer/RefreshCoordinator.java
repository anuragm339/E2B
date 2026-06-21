package com.messaging.broker.consumer;
import com.messaging.broker.consumer.BatchDeliveryService;
import com.messaging.broker.consumer.RefreshGatePolicy;
import com.messaging.broker.monitoring.DataRefreshMetrics;
import com.messaging.broker.monitoring.RefreshHistoryRecorder;
import jakarta.annotation.PostConstruct;
import jakarta.annotation.PreDestroy;
import jakarta.inject.Singleton;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.time.Instant;
import java.util.Map;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Central coordinator for the refresh workflow.
 *
 * Orchestrates startup, reset, replay, ready, and recovery phases.
 */
@Singleton
public class RefreshCoordinator {
    private static final Logger log = LoggerFactory.getLogger(RefreshCoordinator.class);
    private static final long READY_ACK_TIMEOUT_MS = 10000;
    private static final long REPLAY_CHECK_INTERVAL_MS = 1000;
    private static final long RESET_RETRY_INTERVAL_MS = 5000;
    private static final long REFRESH_ABORT_TIMEOUT_MS = 600000; // 10 minutes
    private static final AtomicInteger THREAD_COUNTER = new AtomicInteger(0);

    // Services
    private final RefreshStarter initiationService;
    private final ResetPhase resetService;
    private final ReplayPhase replayService;
    private final ReadyPhase readyService;
    private final RefreshRecovery recoveryService;
    private final RefreshWorkflow stateMachine;
    private final RefreshGatePolicy dataRefreshGatePolicy;
    private final BatchDeliveryService batchDeliveryService;
    private final ConsumerRegistry remoteConsumers;
    private final DataRefreshMetrics dataRefreshMetrics;
    private final RefreshReadinessPolicy readinessPolicy;

    // Shared state
    private final Map<String, RefreshContext> activeRefreshes;
    private final Map<String, ScheduledFuture<?>> replayCheckTasks;
    private final Map<String, ScheduledFuture<?>> resetRetryTasks;
    private final Map<String, ScheduledFuture<?>> abortWatchdogTasks;
    private final Map<String, ScheduledFuture<?>> readyTimeoutTasks;
    private final ScheduledExecutorService scheduler;

    public RefreshCoordinator(
            RefreshStarter initiationService,
            ResetPhase resetService,
            ReplayPhase replayService,
            ReadyPhase readyService,
            RefreshRecovery recoveryService,
            RefreshWorkflow stateMachine,
            RefreshGatePolicy dataRefreshGatePolicy,
            BatchDeliveryService batchDeliveryService,
            ConsumerRegistry remoteConsumers,
            DataRefreshMetrics dataRefreshMetrics,
            RefreshReadinessPolicy readinessPolicy) {
        this.initiationService = initiationService;
        this.resetService = resetService;
        this.replayService = replayService;
        this.readyService = readyService;
        this.recoveryService = recoveryService;
        this.stateMachine = stateMachine;
        this.dataRefreshGatePolicy = dataRefreshGatePolicy;
        this.batchDeliveryService = batchDeliveryService;
        this.remoteConsumers = remoteConsumers;
        this.dataRefreshMetrics = dataRefreshMetrics;
        this.readinessPolicy = readinessPolicy;

        this.scheduler = Executors.newScheduledThreadPool(2, r -> {
            Thread t = new Thread(r);
            t.setName("RefreshCoordinator-" + THREAD_COUNTER.incrementAndGet());
            t.setDaemon(true);
            return t;
        });
        this.activeRefreshes = new ConcurrentHashMap<>();
        this.replayCheckTasks = new ConcurrentHashMap<>();
        this.resetRetryTasks = new ConcurrentHashMap<>();
        this.abortWatchdogTasks = new ConcurrentHashMap<>();
        this.readyTimeoutTasks = new ConcurrentHashMap<>();

        // Inject shared state into services
        wireServices();
    }

    /**
     * Backward-compatible constructor used by older tests that do not care about
     * refresh-state metric emission.
     */
    @Deprecated
    public RefreshCoordinator(
            RefreshStarter initiationService,
            ResetPhase resetService,
            ReplayPhase replayService,
            ReadyPhase readyService,
            RefreshRecovery recoveryService,
            RefreshWorkflow stateMachine,
            RefreshGatePolicy dataRefreshGatePolicy,
            BatchDeliveryService batchDeliveryService,
            ConsumerRegistry remoteConsumers) {
        this(
                initiationService,
                resetService,
                replayService,
                readyService,
                recoveryService,
                stateMachine,
                dataRefreshGatePolicy,
                batchDeliveryService,
                remoteConsumers,
                null,
                new GlobalBarrierPolicy()
        );
    }

    /**
     * Inject shared state into all services.
     */
    private void wireServices() {
        // Initiation service needs task maps for cancellation
        if (initiationService instanceof RefreshInitiator) {
            ((RefreshInitiator) initiationService).setSharedState(
                    activeRefreshes, resetRetryTasks, replayCheckTasks, abortWatchdogTasks, readyTimeoutTasks);
        }

        // Ready service needs activeRefreshes for batch completion check
        if (readyService instanceof RefreshReadyService) {
            ((RefreshReadyService) readyService).setSharedState(activeRefreshes);
        }

        // Recovery service needs task maps and scheduling callbacks
        if (recoveryService instanceof RefreshRecoveryService) {
            RefreshRecoveryService impl = (RefreshRecoveryService) recoveryService;
            impl.setSharedState(activeRefreshes, resetRetryTasks, replayCheckTasks);
            impl.setSchedulingCallbacks(
                    this::scheduleResetRetry,
                    this::scheduleReplayCheck,
                    this::scheduleReadyTimeout,
                    this::scheduleAbortWatchdog
            );
        }
    }

    @PostConstruct
    public void init() {
        dataRefreshGatePolicy.setDataRefreshCoordinator(this);
        batchDeliveryService.setDataRefreshCoordinator(this);
        remoteConsumers.setRefreshCoordinator(this);
        log.info("RefreshCoordinator initialized");

        // Recover and resume in-progress refreshes
        recoveryService.recoverAndResumeRefreshes();
    }

    /**
     * Start a refresh for a topic.
     */
    public CompletableFuture<RefreshResult> startRefresh(String topic) {
        return startRefresh(topic, "LOCAL");
    }

    public CompletableFuture<RefreshResult> startRefresh(String topic, String refreshType) {
        CompletableFuture<RefreshResult> result = initiationService.startRefresh(topic, refreshType);

        // Send RESET after initiation
        RefreshContext context = activeRefreshes.get(topic);
        if (context != null && context.getState() == RefreshState.RESET_SENT) {
            resetService.sendReset(topic, context);

            // Schedule periodic RESET retry
            scheduleResetRetry(topic);
        }

        // Schedule abort watchdog: if the refresh is still in a non-terminal state after 10 minutes, abort it.
        // Capture refreshId at scheduling time so an orphaned watchdog from a previous refresh cannot
        // fire against a new refresh context for the same topic.
        if (context != null) {
            String refreshId = context.getRefreshId();
            ScheduledFuture<?> watchdog = scheduler.schedule(
                    () -> runAbortWatchdog(topic, refreshId, false),
                    REFRESH_ABORT_TIMEOUT_MS, TimeUnit.MILLISECONDS);
            // compute() atomically replaces any previous watchdog entry, cancelling the old one.
            // This prevents a stale watchdog from a rapid force-cancel + restart sequence leaking
            // into the new refresh — consistent with the putIfAbsent pattern in scheduleReplayCheck.
            abortWatchdogTasks.compute(topic, (k, old) -> {
                if (old != null) old.cancel(false);
                return watchdog;
            });
        }

        return result;
    }

    /**
     * F1: run the abort watchdog so an unchecked throw in the abort logic can never silently lose
     * the safety net. The watchdog is a one-shot {@code scheduler.schedule}; if its task throws,
     * ScheduledThreadPoolExecutor stores the throwable in a Future nobody reads and the watchdog is
     * simply gone — a stuck refresh would then never be aborted. On any Exception we log and re-arm
     * for another window instead (re-arm itself is refreshId-guarded, so a completed/replaced
     * refresh does not loop). Errors (OOM etc.) stay fatal.
     */
    private void runAbortWatchdog(String topic, String refreshId, boolean allowAbortFromReadySent) {
        try {
            abortRefreshIfStuck(topic, refreshId, allowAbortFromReadySent);
        } catch (Exception e) {
            log.error("Abort watchdog failed for topic={} refreshId={} — re-arming for another window",
                    topic, refreshId, e);
            try {
                rearmAbortWatchdog(topic, refreshId, allowAbortFromReadySent);
            } catch (Exception rearmEx) {
                log.error("Failed to re-arm abort watchdog for topic={} refreshId={}", topic, refreshId, rearmEx);
            }
        }
    }

    /**
     * Abort a refresh that has not completed within the timeout window.
     *
     * @param expectedRefreshId guards against firing against a different refresh that started
     *                          for the same topic after the one that scheduled this watchdog.
     */
    private void abortRefreshIfStuck(String topic, String expectedRefreshId) {
        abortRefreshIfStuck(topic, expectedRefreshId, false);
    }

    private void abortRefreshIfStuck(String topic, String expectedRefreshId, boolean allowAbortFromReadySent) {
        RefreshContext context = activeRefreshes.get(topic);
        if (context == null) return;

        if (!expectedRefreshId.equals(context.getRefreshId())) {
            log.debug("Abort watchdog for refreshId={} fired but current refreshId={} — skipping",
                    expectedRefreshId, context.getRefreshId());
            return;
        }

        RefreshState state = context.getState();
        if (stateMachine.isTerminalState(state)) return;

        // H2-2: Do not abort on the first watchdog fire once READY messages are already in flight.
        // Aborting immediately would leave consumers in a ready state while the broker considers
        // the refresh ABORTED, causing the pipe to never resume.
        // H1-NEW-1: Re-arm the watchdog for one more window so that a permanently stuck READY_SENT
        // (all consumers disconnected, READY ACK retry path unresponsive) is eventually aborted.
        // The re-armed call passes allowAbortFromReadySent=true and will not re-arm again.
        if (state == RefreshState.READY_SENT && !allowAbortFromReadySent) {
            log.warn("Abort watchdog fired for topic={} in READY_SENT — re-arming for one more window; "
                    + "READY ACK retry path has {} more ms to resolve",
                    topic, REFRESH_ABORT_TIMEOUT_MS);
            rearmAbortWatchdog(topic, context.getRefreshId(), true);
            return;
        }

        if (state == RefreshState.REPLAYING) {
            Instant lastProgress = context.getLastReplayProgressTime();
            long idleMs = lastProgress == null
                    ? Long.MAX_VALUE
                    : Duration.between(lastProgress, Instant.now()).toMillis();
            if (idleMs < REFRESH_ABORT_TIMEOUT_MS) {
                log.warn("Abort watchdog fired for topic={} in REPLAYING but replay is still progressing; "
                                + "lastProgress={} idleMs={} — re-arming watchdog",
                        topic, lastProgress, idleMs);
                rearmAbortWatchdog(topic, context.getRefreshId(), false);
                return;
            }
        }

        log.error("Refresh timeout after {}ms for topic={}, state={}, refreshId={} — aborting",
                REFRESH_ABORT_TIMEOUT_MS, topic, state, context.getRefreshId());

        RefreshWorkflow.StateTransitionResult result = stateMachine.transition(state, RefreshState.ABORTED);
        if (result.isSuccess()) {
            context.setState(RefreshState.ABORTED);
            if (dataRefreshMetrics != null) {
                dataRefreshMetrics.updateRefreshState(topic, RefreshState.ABORTED);
            }

            ScheduledFuture<?> resetTask = resetRetryTasks.remove(topic);
            if (resetTask != null) resetTask.cancel(false);

            ScheduledFuture<?> replayTask = replayCheckTasks.remove(topic);
            if (replayTask != null) replayTask.cancel(false);

            // Remove our own map entry — prevents a stale ScheduledFuture reference
            // from accumulating indefinitely for topics that abort and are never refreshed again.
            abortWatchdogTasks.remove(topic);

            // Cancel any pending READY timeout — state is now ABORTED, the task would exit
            // harmlessly but leaving it in the map is inconsistent with all other task maps.
            ScheduledFuture<?> readyTask = readyTimeoutTasks.remove(topic);
            if (readyTask != null) readyTask.cancel(false);

            activeRefreshes.remove(topic);
            log.info("Refresh aborted and cleaned up for topic: {}", topic);
            RefreshHistoryRecorder abortHist = RefreshHistoryRecorder.instance();
            if (abortHist != null) {
                abortHist.record(context, "ABORTED");
            }
        }
    }

    /**
     * Handle RESET ACK from consumer.
     */
    public void handleResetAck(String consumerGroupTopic, String clientId, String topic, String traceId) {
        RefreshContext context = activeRefreshes.get(topic);
        if (context == null) {
            log.warn("Received RESET ACK from {} for topic {} but no active refresh, traceId={}",
                    consumerGroupTopic, topic, traceId);
            return;
        }

        boolean isFirstAck = resetService.handleResetAck(consumerGroupTopic, clientId, topic, context, traceId);

        if (isFirstAck) {
            // Transition to REPLAYING state
            RefreshWorkflow.StateTransitionResult transition =
                    stateMachine.transition(context.getState(), RefreshState.REPLAYING);

            if (transition.isSuccess()) {
                context.setState(RefreshState.REPLAYING);
                if (dataRefreshMetrics != null) {
                    dataRefreshMetrics.updateRefreshState(topic, RefreshState.REPLAYING);
                }
                if (resetService instanceof RefreshResetService) {
                    ((RefreshResetService) resetService).persistState(context);
                }

                // Cancel RESET retry task
                ScheduledFuture<?> resetTask = resetRetryTasks.remove(topic);
                if (resetTask != null) {
                    resetTask.cancel(false);
                    log.info("Cancelled RESET retry task for topic {}", topic);
                }

                // Start replay progress monitoring
                scheduleReplayCheck(topic);
            }
        }
    }

    /**
     * Handle READY ACK from consumer.
     */
    public void handleReadyAck(String consumerGroupTopic, String topic, String traceId) {
        RefreshContext context = activeRefreshes.get(topic);
        if (context == null) {
            log.warn("Received READY ACK from {} for topic {} but no active refresh, traceId={}",
                    consumerGroupTopic, topic, traceId);
            return;
        }

        boolean allReceived = readyService.handleReadyAck(consumerGroupTopic, topic, context, traceId);

        if (allReceived) {
            // This topic is settled (all READY acks in). Cancel its ready-timeout + abort watchdog now —
            // it is not stuck, just possibly waiting on the cross-topic barrier, and we must not let its
            // own watchdog abort it while it waits.
            cancelTopicTimers(topic);

            // Readiness barrier: a settled topic does NOT go live until the policy allows it. For the
            // global barrier that means EVERY in-flight refresh is settled — so no topic serves fresh
            // data while another is still catching up. Held topics' new records wait in storage.
            if (!readinessPolicy.canGoLive(topic, activeRefreshes)) {
                log.info("event=refresh.settled_waiting topic={} — held by readiness barrier until all topics settle",
                        topic);
                return;
            }

            // Barrier open → complete ALL settled topics together (not just this one).
            completeAllSettledRefreshes();
        }
    }

    /** Cancel the ready-timeout and abort watchdog for a topic (it has settled; nothing left to retry/abort). */
    private void cancelTopicTimers(String topic) {
        ScheduledFuture<?> readyTask = readyTimeoutTasks.remove(topic);
        if (readyTask != null) readyTask.cancel(false);
        ScheduledFuture<?> watchdogTask = abortWatchdogTasks.remove(topic);
        if (watchdogTask != null) watchdogTask.cancel(false);
    }

    /**
     * Complete every active refresh that has reached its settled point (all READY acks in) — invoked
     * once the readiness barrier opens. Each is claimed via {@code markFirstReadyComplete()} so a
     * concurrent caller cannot double-complete, and uses a pre-CAS state snapshot so a racing abort
     * watchdog cannot wedge the READY_SENT → COMPLETED transition.
     */
    private void completeAllSettledRefreshes() {
        for (Map.Entry<String, RefreshContext> entry : activeRefreshes.entrySet()) {
            String topic = entry.getKey();
            RefreshContext context = entry.getValue();
            if (!context.allReadyAcksReceived()) {
                continue; // not settled (shouldn't happen once the barrier is open) — leave it running
            }
            RefreshState stateAtCompletion = context.getState();
            if (!context.markFirstReadyComplete()) {
                continue; // already claimed/completed by another thread
            }
            RefreshWorkflow.StateTransitionResult transition =
                    stateMachine.transition(stateAtCompletion, RefreshState.COMPLETED);
            if (!transition.isSuccess()) {
                continue;
            }
            readyService.completeRefresh(topic, context);
            RefreshHistoryRecorder doneHist = RefreshHistoryRecorder.instance();
            if (doneHist != null) {
                doneHist.record(context, "COMPLETED");
            }
            cancelTopicTimers(topic);

            final String completedRefreshId = context.getRefreshId();
            scheduler.schedule(() -> {
                RefreshContext currentContext = activeRefreshes.get(topic);
                if (currentContext != null && completedRefreshId.equals(currentContext.getRefreshId())) {
                    activeRefreshes.remove(topic);
                    log.info("Refresh context removed for topic: {}", topic);
                }
            }, 60, TimeUnit.SECONDS);
        }
    }

    /**
     * Schedule periodic RESET retry task.
     * Uses putIfAbsent to prevent duplicate tasks if called concurrently during recovery,
     * consistent with the pattern in scheduleReplayCheck.
     */
    private void scheduleResetRetry(String topic) {
        ScheduledFuture<?> task = scheduler.scheduleWithFixedDelay(
                () -> retryResetBroadcast(topic),
                RESET_RETRY_INTERVAL_MS,
                RESET_RETRY_INTERVAL_MS,
                TimeUnit.MILLISECONDS
        );
        ScheduledFuture<?> existing = resetRetryTasks.putIfAbsent(topic, task);
        if (existing != null) {
            task.cancel(false);
            log.debug("RESET retry already scheduled for topic {}, cancelled duplicate", topic);
        } else {
            log.info("Scheduled RESET retry task for topic {}", topic);
        }
    }

    /**
     * Schedule periodic replay check task.
     * Uses putIfAbsent to prevent duplicate tasks from concurrent callers
     * (e.g., handleResetAck and registerLateJoiningConsumer racing).
     */
    private void scheduleReplayCheck(String topic) {
        ScheduledFuture<?> task = scheduler.scheduleWithFixedDelay(
                () -> {
                    try {
                        checkReplayProgress(topic);
                    } catch (Exception e) {
                        log.error("Replay check failed for topic {}", topic, e);
                    }
                },
                REPLAY_CHECK_INTERVAL_MS,
                REPLAY_CHECK_INTERVAL_MS,
                TimeUnit.MILLISECONDS
        );
        ScheduledFuture<?> existing = replayCheckTasks.putIfAbsent(topic, task);
        if (existing != null) {
            // Another task already registered — cancel ours to avoid duplicate progress checks
            task.cancel(false);
            log.debug("Replay check already scheduled for topic {}, cancelled duplicate", topic);
        } else {
            log.info("Scheduled replay check task for topic {}", topic);
        }
    }

    private void rearmAbortWatchdog(String topic, String refreshId, boolean allowAbortFromReadySent) {
        ScheduledFuture<?> rearm = scheduler.schedule(
                () -> runAbortWatchdog(topic, refreshId, allowAbortFromReadySent),
                REFRESH_ABORT_TIMEOUT_MS, TimeUnit.MILLISECONDS);
        RefreshContext current = activeRefreshes.get(topic);
        if (current != null && refreshId.equals(current.getRefreshId())) {
            abortWatchdogTasks.compute(topic, (k, old) -> {
                if (old != null) old.cancel(false);
                return rearm;
            });
        } else {
            rearm.cancel(false);
        }
    }

    /**
     * Schedule READY ACK timeout check.
     * Stores the future so @PreDestroy can cancel pending timeouts before teardown.
     * Uses cancel-on-replace so a previously completed entry is explicitly released,
     * making the map lifecycle consistent with all other task maps.
     */
    private void scheduleReadyTimeout(String topic) {
        // P2-1: Guard against inserting an orphaned task after handleReadyAck already completed
        // the refresh. Without this check, the self-reschedule in checkReadyAckTimeout can race
        // with the readyTimeoutTasks.remove() in handleReadyAck and insert a new entry that has
        // no owner to cancel it until @PreDestroy.
        RefreshContext guard = activeRefreshes.get(topic);
        if (guard == null || stateMachine.isTerminalState(guard.getState())) return;

        ScheduledFuture<?> task = scheduler.schedule(
                () -> checkReadyAckTimeout(topic),
                READY_ACK_TIMEOUT_MS,
                TimeUnit.MILLISECONDS
        );
        ScheduledFuture<?> previous = readyTimeoutTasks.put(topic, task);
        if (previous != null && !previous.isDone()) {
            previous.cancel(false);
        }
        log.debug("Scheduled READY timeout check for topic {}", topic);
    }

    /**
     * Schedule an abort watchdog for a topic. Used by the recovery service when resuming
     * an in-progress refresh after restart — ensures the abort safety net is always armed
     * regardless of whether the refresh was started or recovered.
     */
    private void scheduleAbortWatchdog(String topic) {
        RefreshContext context = activeRefreshes.get(topic);
        if (context == null) return;
        String refreshId = context.getRefreshId();
        ScheduledFuture<?> watchdog = scheduler.schedule(
                () -> abortRefreshIfStuck(topic, refreshId),
                REFRESH_ABORT_TIMEOUT_MS, TimeUnit.MILLISECONDS);
        abortWatchdogTasks.compute(topic, (k, old) -> {
            if (old != null) old.cancel(false);
            return watchdog;
        });
        log.info("Scheduled abort watchdog for topic {} (recovered refresh, refreshId={})", topic, refreshId);
    }

    /**
     * Retry RESET broadcast (called by scheduler).
     */
    private void retryResetBroadcast(String topic) {
        RefreshContext context = activeRefreshes.get(topic);
        if (context == null) return;

        // F1: this runs on a scheduleWithFixedDelay task; an unchecked throw would cancel the whole
        // periodic schedule, so RESET would never be retried and the refresh could stall in
        // RESET_SENT. Swallow Exception so the periodic schedule is retained.
        try {
            resetService.retryResetBroadcast(topic, context);
        } catch (Exception e) {
            log.error("RESET retry failed for topic {}; periodic schedule retained", topic, e);
        }
    }

    /**
     * Check replay progress (called by scheduler).
     */
    private void checkReplayProgress(String topic) {
        RefreshContext context = activeRefreshes.get(topic);
        if (context == null) return;

        boolean readyToSend = replayService.checkReplayProgress(topic, context);

        if (readyToSend) {
            // Transition to READY_SENT state
            RefreshWorkflow.StateTransitionResult transition =
                    stateMachine.transition(context.getState(), RefreshState.READY_SENT);

            if (transition.isSuccess()) {
                readyService.sendReady(topic, context);

                // Cancel replay check task
                ScheduledFuture<?> task = replayCheckTasks.remove(topic);
                if (task != null) {
                    task.cancel(false);
                    log.info("Cancelled replay check task for topic {}", topic);
                }

                // Schedule READY timeout check
                scheduleReadyTimeout(topic);
            }
        }
    }

    /**
     * Check READY ACK timeout (called by scheduler).
     * Re-schedules itself if ACKs are still missing, so late-connecting consumers get READY.
     */
    private void checkReadyAckTimeout(String topic) {
        RefreshContext context = activeRefreshes.get(topic);
        if (context == null) return;

        // F1: guard the timeout check so a throw cannot skip the self-reschedule below. This is a
        // one-shot task that re-arms itself; if readyService.checkReadyAckTimeout threw, the chain
        // would die and a stuck READY_SENT refresh could remain active until restart.
        try {
            readyService.checkReadyAckTimeout(topic, context);
        } catch (Exception e) {
            log.error("READY-ack timeout check failed for topic {}; rescheduling next check", topic, e);
        }

        // If still waiting for ACKs, schedule another check so newly connected consumers get READY
        if (context.getState() == RefreshState.READY_SENT && !context.allReadyAcksReceived()) {
            scheduleReadyTimeout(topic);
        }
    }

    /**
     * Get refresh status for a topic.
     */
    public RefreshContext getRefreshStatus(String topic) {
        return activeRefreshes.get(topic);
    }

    public Map<String, RefreshContext> getActiveRefreshesSnapshot() {
        return new ConcurrentHashMap<>(activeRefreshes);
    }

    /**
     * Check if any refresh is in progress.
     */
    public boolean isRefreshInProgress() {
        return !activeRefreshes.isEmpty();
    }

    /**
     * Check if a refresh is active for a specific topic.
     */
    public boolean isRefreshActive(String topic) {
        return activeRefreshes.containsKey(topic);
    }

    /**
     * Get current refresh ID.
     */
    public String getCurrentRefreshId() {
        if (initiationService instanceof RefreshInitiator) {
            return ((RefreshInitiator) initiationService).getCurrentRefreshId();
        }
        return null;
    }

    /**
     * Get refresh ID for a specific topic.
     */
    public String getRefreshIdForTopic(String topic) {
        RefreshContext context = activeRefreshes.get(topic);
        return context != null ? context.getRefreshId() : null;
    }

    /**
     * Get refresh type for a topic.
     */
    public String getRefreshTypeForTopic(String topic) {
        RefreshContext context = activeRefreshes.get(topic);
        return context != null ? context.getRefreshType() : null;
    }

    /**
     * Get current refresh context (for health check).
     */
    public RefreshContext getCurrentRefreshContext() {
        return activeRefreshes.values().stream().findFirst().orElse(null);
    }

    /**
     * Get current refresh topic.
     */
    public String getCurrentRefreshTopic() {
        RefreshContext context = getCurrentRefreshContext();
        return context != null ? context.getTopic() : null;
    }

    /**
     * Register a consumer that connected after the refresh started (late joiner).
     *
     * <p>When a consumer subscribes during RESET_SENT or REPLAYING, it missed the RESET broadcast.
     * Recording it in {@code receivedResetAcks} ensures it is included in {@code sendReady()}'s
     * broadcast at the end of replay AND in {@code checkReadyAckTimeout} re-broadcasts.
     *
     * <p>When state is READY_SENT the consumer also needs to be in {@code receivedResetAcks} so
     * that any future {@code checkReadyAckTimeout} re-broadcast reaches it if the direct READY send
     * is lost. We record the ack and return {@code false} so the caller sends READY immediately.
     *
     * <p>Returns {@code true} if the consumer was registered and the READY will arrive via the
     * normal broadcast path (caller should NOT send READY directly).
     * Returns {@code false} in all other cases — state has advanced past REPLAYING, so the caller
     * must send READY directly (or fall back to startup READY for terminal states).
     */
    public boolean registerLateJoiningConsumer(String topic, String groupTopic) {
        RefreshContext context = activeRefreshes.get(topic);
        if (context == null) return false;

        RefreshState state = context.getState();
        if (state == RefreshState.RESET_SENT || state == RefreshState.REPLAYING) {
            context.recordResetAck(groupTopic);
            log.info("Late-joining consumer {} registered for topic {} in state {} - will receive refresh READY",
                    groupTopic, topic, state);

            // C2: Re-read state after recording the ack. If checkReplayProgress() ran concurrently
            // and advanced state to READY_SENT (calling sendReady() before our recordResetAck),
            // this consumer was not in the broadcast snapshot. Return false so the caller sends
            // READY directly rather than waiting for a re-broadcast that may not arrive.
            RefreshState stateAfter = context.getState();
            if (stateAfter != RefreshState.RESET_SENT && stateAfter != RefreshState.REPLAYING) {
                log.debug("State advanced to {} after recordResetAck for {}/{} — returning false for caller to send READY directly",
                        stateAfter, topic, groupTopic);
                return false;
            }

            // P1-2: If this ACK completes the set and we are still in RESET_SENT, drive the
            // transition to REPLAYING ourselves — handleResetAck() was never called for this consumer
            // so the normal transition path was bypassed. Use stateAfter (post-recordResetAck re-read)
            // rather than the stale pre-ack state variable. Also claim the markFirstResetAck() CAS so
            // a concurrent handleResetAck cannot drive the same transition simultaneously — without
            // this guard, both threads could independently attempt setState(REPLAYING) and
            // scheduleReplayCheck, producing a duplicate replay-check task.
            if (stateAfter == RefreshState.RESET_SENT && context.allResetAcksReceived()
                    && context.markFirstResetAck()) {
                // NEW-P1: Use stateAfter (not context.getState()) — consistent with the guard above
                // and avoids a spurious REPLAYING→REPLAYING idempotent transition if the volatile
                // field was updated between the guard read and this call.
                RefreshWorkflow.StateTransitionResult transition =
                        stateMachine.transition(stateAfter, RefreshState.REPLAYING);
                if (transition.isSuccess()) {
                    context.setState(RefreshState.REPLAYING);

                    ScheduledFuture<?> resetTask = resetRetryTasks.remove(topic);
                    if (resetTask != null) {
                        resetTask.cancel(false);
                        log.info("Cancelled RESET retry task for topic {} (all ACKs via late-joiner)", topic);
                    }

                    scheduleReplayCheck(topic);
                    log.info("All RESET ACKs received via late-joining consumer — transitioning {} RESET_SENT → REPLAYING",
                            topic);
                }
            }
            return true;
        }

        if (state == RefreshState.READY_SENT && !context.allReadyAcksReceived()) {
            // C1: Consumer joined during the READY broadcast phase. Add to receivedResetAcks so
            // checkReadyAckTimeout re-broadcasts include this consumer if the direct READY send is lost.
            // P2-2: Use direct set addition instead of recordResetAck() to avoid the replay-tracking
            // side effects (consumerReplaying=true, consumerOffsets=0) that are incorrect for a
            // consumer that never participated in the replay phase.
            context.getReceivedResetAcks().add(groupTopic);
            log.info("Late-joining consumer {} registered for topic {} in READY_SENT — caller will send READY directly",
                    groupTopic, topic);
            // Return false: caller must send READY immediately; it cannot wait for the next retry.
        }
        return false;
    }

    @PreDestroy
    public void shutdown() {
        log.info("Shutting down RefreshCoordinator");

        // Cancel all scheduled tasks
        resetRetryTasks.values().forEach(task -> task.cancel(false));
        resetRetryTasks.clear();
        replayCheckTasks.values().forEach(task -> task.cancel(false));
        replayCheckTasks.clear();
        abortWatchdogTasks.values().forEach(task -> task.cancel(false));
        abortWatchdogTasks.clear();
        readyTimeoutTasks.values().forEach(task -> task.cancel(false));
        readyTimeoutTasks.clear();

        // Record shutdown time for all active refreshes
        if (!activeRefreshes.isEmpty()) {
            Instant shutdownTime = Instant.now();
            log.info("Recording shutdown time for {} active refresh(es)", activeRefreshes.size());

            for (RefreshContext context : activeRefreshes.values()) {
                context.recordShutdown(shutdownTime);
                log.info("Recorded shutdown for topic: {}", context.getTopic());
            }
        }

        scheduler.shutdown();
        try {
            if (!scheduler.awaitTermination(5, TimeUnit.SECONDS)) {
                scheduler.shutdownNow();
            }
        } catch (InterruptedException e) {
            scheduler.shutdownNow();
            Thread.currentThread().interrupt();
        }
    }
}
