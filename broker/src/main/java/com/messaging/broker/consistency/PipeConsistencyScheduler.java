package com.messaging.broker.consistency;

import io.micronaut.context.annotation.Value;
import io.micronaut.scheduling.annotation.Scheduled;
import jakarta.inject.Singleton;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Set;
import java.util.concurrent.ThreadLocalRandom;

/**
 * Drives HOP and DEEP audits on independent schedules.
 *
 * <ul>
 *   <li>HOP — every {@code pipe.consistency.schedule.hop-interval} (default 6h)</li>
 *   <li>DEEP — every {@code pipe.consistency.schedule.deep-interval} (default 24h)</li>
 * </ul>
 *
 * <p>Both no-op when {@code pipe.consistency.enabled=false}. Each cycle iterates all
 * discovered topics; failures on one topic don't block others.
 */
@Singleton
public class PipeConsistencyScheduler {
    private static final Logger LOG = LoggerFactory.getLogger(PipeConsistencyScheduler.class);

    private final PipeConsistencyChecker checker;
    private final boolean enabled;
    private final boolean jitterApplied;
    private final long perTopicDelayMs;

    public PipeConsistencyScheduler(
            PipeConsistencyChecker checker,
            @Value("${pipe.consistency.enabled:false}") boolean enabled,
            @Value("${pipe.consistency.schedule.per-topic-delay-ms:5000}") long perTopicDelayMs
    ) {
        this.checker = checker;
        this.enabled = enabled;
        this.jitterApplied = enabled;
        this.perTopicDelayMs = Math.max(0, perTopicDelayMs);
        LOG.info("PipeConsistencyScheduler enabled={} perTopicDelayMs={}", enabled, this.perTopicDelayMs);
    }

    @Scheduled(initialDelay = "${pipe.consistency.schedule.initial-delay:10m}",
               fixedDelay = "${pipe.consistency.schedule.hop-interval:6h}")
    public void runHopCycle() {
        if (!enabled) return;
        sleepJitter();
        try {
            PipeConsistencyReport r = checker.runHopGlobal();
            LOG.info("HOP-global audit status={}", r.status);
        } catch (Exception e) {
            LOG.warn("HOP-global audit failed: {}", e.getMessage());
        }
    }

    @Scheduled(initialDelay = "${pipe.consistency.schedule.deep-initial-delay:30m}",
               fixedDelay = "${pipe.consistency.schedule.deep-interval:24h}")
    public void runDeepCycle() {
        if (!enabled) return;
        sleepJitter();
        try {
            PipeConsistencyReport r = checker.runDeepGlobal();
            LOG.info("DEEP-global audit status={}", r.status);
        } catch (Exception e) {
            LOG.warn("DEEP-global audit failed: {}", e.getMessage());
        }
    }

    private static void sleepQuietly(long ms) {
        try { Thread.sleep(ms); }
        catch (InterruptedException ie) { Thread.currentThread().interrupt(); }
    }

    private void sleepJitter() {
        if (!jitterApplied) return;
        // Avoid all brokers hitting the same upstream at the same instant. Jitter < 60s
        // is small enough to keep cadence intact but big enough to spread load.
        try {
            Thread.sleep(ThreadLocalRandom.current().nextInt(60_000));
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }
}
