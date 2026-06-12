package com.messaging.broker.consistency;

import com.messaging.broker.core.TopologyManager;
import com.messaging.broker.monitoring.MemoryMonitor;
import io.micronaut.context.annotation.Requires;
import io.micronaut.context.annotation.Value;
import io.micronaut.scheduling.annotation.Scheduled;
import jakarta.inject.Inject;
import jakarta.inject.Named;
import jakarta.inject.Singleton;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.concurrent.ExecutorService;

/**
 * Automatic pipe-consistency detection — runs {@link PipeConsistencyService#runCheck} on a
 * fixed cadence so divergence is detected without anyone calling the admin API.
 *
 * <p>Follows the {@code CompactionScheduler} conventions: skip-don't-queue, every skip is
 * logged with a reason, and the actual work runs on the shared {@code compactionExecutor}.
 * Skips when:
 * <ul>
 *   <li>{@code pipe.consistency.schedule.enabled=false} (runtime kill-switch; the admin API
 *       keeps working)</li>
 *   <li>a check is already running (single-flight)</li>
 *   <li>the target is the parent and none is assigned — typical offline POS; no point
 *       burning index scans or producing UNREACHABLE noise every tick, the next tick after
 *       reconnect covers it</li>
 *   <li>heap pressure is already high ({@link MemoryMonitor}) — the audit is diagnostics,
 *       never competition for a struggling store device</li>
 * </ul>
 *
 * <p>The bean only exists when {@code pipe.consistency.enabled=true} (fail-closed, same
 * posture as the served endpoints). Cost per tick when it does run: one streaming index
 * scan per topic per side + one ~1 KB HTTP exchange per topic when consistent.
 */
@Singleton
@Requires(property = "pipe.consistency.enabled", value = "true")
public class PipeConsistencyScheduler {

    private static final Logger log = LoggerFactory.getLogger(PipeConsistencyScheduler.class);

    private final PipeConsistencyService service;
    private final TopologyManager topologyManager;
    private final MemoryMonitor memoryMonitor;
    private final ExecutorService compactionExecutor;
    private final boolean scheduleEnabled;
    private final String target;

    @Inject
    public PipeConsistencyScheduler(
            PipeConsistencyService service,
            TopologyManager topologyManager,
            MemoryMonitor memoryMonitor,
            @Named("compactionExecutor") ExecutorService compactionExecutor,
            @Value("${pipe.consistency.schedule.enabled:true}") boolean scheduleEnabled,
            @Value("${pipe.consistency.schedule.target:parent}") String target) {
        this.service = service;
        this.topologyManager = topologyManager;
        this.memoryMonitor = memoryMonitor;
        this.compactionExecutor = compactionExecutor;
        this.scheduleEnabled = scheduleEnabled;
        this.target = PipeConsistencyService.TARGET_CLOUD.equalsIgnoreCase(target)
                ? PipeConsistencyService.TARGET_CLOUD
                : PipeConsistencyService.TARGET_PARENT;
        log.info("event=pipe_consistency.scheduler_initialized scheduleEnabled={} target={}",
                scheduleEnabled, this.target);
    }

    @Scheduled(
            fixedDelay = "${pipe.consistency.schedule.interval:6h}",
            initialDelay = "${pipe.consistency.schedule.initial-delay:10m}")
    public void runScheduledCheck() {
        String skipReason = skipReason();
        if (skipReason != null) {
            log.debug("event=pipe_consistency.scheduled_run_skipped reason={}", skipReason);
            return;
        }

        compactionExecutor.execute(() -> {
            try {
                List<PipeConsistencyReport> reports = service.runCheck("all", target);
                if (reports == null) {
                    log.info("event=pipe_consistency.scheduled_run_skipped reason=already_running");
                    return;
                }
                long inconsistent = reports.stream().filter(PipeConsistencyReport::isInconsistent).count();
                log.info("event=pipe_consistency.scheduled_run_complete topics={} inconsistent={} target={}",
                        reports.size(), inconsistent, target);
            } catch (Throwable t) {
                // execute() swallows uncaught throwables on this pool — log here.
                log.error("event=pipe_consistency.scheduled_run_failed target={}", target, t);
            }
        });
    }

    /** Null when the run should proceed; otherwise the reason to skip this tick. */
    String skipReason() {
        if (!scheduleEnabled) {
            return "schedule_disabled";
        }
        if (service.isRunning()) {
            return "already_running";
        }
        if (PipeConsistencyService.TARGET_PARENT.equals(target)
                && topologyManager.getCurrentParentUrl() == null) {
            return "no_parent_offline";
        }
        if (memoryMonitor.isMemoryPressureHigh()) {
            return "memory_pressure";
        }
        return null;
    }
}
