package com.messaging.broker.monitoring;

import io.micrometer.core.instrument.Gauge;
import io.micrometer.core.instrument.MeterRegistry;
import io.micronaut.context.annotation.Value;
import io.micronaut.scheduling.annotation.Scheduled;
import jakarta.annotation.PostConstruct;
import jakarta.inject.Singleton;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.management.ManagementFactory;
import java.lang.management.MemoryMXBean;
import java.lang.management.MemoryUsage;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Monitors memory usage and detects potential OOM conditions
 */
@Singleton
public class MemoryMonitor {
    private static final Logger log = LoggerFactory.getLogger(MemoryMonitor.class);

    private final MemoryMXBean memoryMXBean;
    private final MeterRegistry meterRegistry;
    private final double heapWarningThreshold;
    private final double heapCriticalThreshold;
    private final AtomicBoolean heapWarning;
    private final AtomicBoolean heapCritical;
    private final AtomicLong lastGcTime;
    private final AtomicLong totalGcCount;

    public MemoryMonitor(
            MeterRegistry meterRegistry,
            @Value("${broker.memory.heap-warning-threshold:0.80}") double heapWarningThreshold,
            @Value("${broker.memory.heap-critical-threshold:0.90}") double heapCriticalThreshold) {

        this.memoryMXBean = ManagementFactory.getMemoryMXBean();
        this.meterRegistry = meterRegistry;
        this.heapWarningThreshold = heapWarningThreshold;
        this.heapCriticalThreshold = heapCriticalThreshold;
        this.heapWarning = new AtomicBoolean(false);
        this.heapCritical = new AtomicBoolean(false);
        this.lastGcTime = new AtomicLong(0);
        this.totalGcCount = new AtomicLong(0);
    }

    @PostConstruct
    public void init() {
        // Register custom metrics
        Gauge.builder("broker.memory.heap_usage_percent", this, MemoryMonitor::getHeapUsagePercent)
                .description("Heap memory usage percentage")
                .register(meterRegistry);

        Gauge.builder("broker.memory.heap_warning", heapWarning, b -> b.get() ? 1.0 : 0.0)
                .description("Heap memory warning state (1=warning, 0=ok)")
                .register(meterRegistry);

        Gauge.builder("broker.memory.heap_critical", heapCritical, b -> b.get() ? 1.0 : 0.0)
                .description("Heap memory critical state (1=critical, 0=ok)")
                .register(meterRegistry);

        Gauge.builder("broker.memory.nonheap_usage_percent", this, MemoryMonitor::getNonHeapUsagePercent)
                .description("Non-heap memory usage percentage")
                .register(meterRegistry);

        log.info("MemoryMonitor initialized: warningThreshold={}%, criticalThreshold={}%",
                heapWarningThreshold * 100, heapCriticalThreshold * 100);
    }

    /**
     * Check memory usage every 10 seconds
     */
    @Scheduled(fixedDelay = "10s", initialDelay = "10s")
    public void checkMemory() {
        MemoryUsage heapUsage = memoryMXBean.getHeapMemoryUsage();
        long used = heapUsage.getUsed();
        long max = heapUsage.getMax();
        double usagePercent = (double) used / max;

        // Update state flags
        boolean wasWarning = heapWarning.get();
        boolean wasCritical = heapCritical.get();

        heapWarning.set(usagePercent >= heapWarningThreshold);
        heapCritical.set(usagePercent >= heapCriticalThreshold);

        // Log state changes
        if (heapCritical.get() && !wasCritical) {
            log.error("🚨 CRITICAL: Heap memory usage at {:.1f}% ({} MB / {} MB) - OOM risk!",
                    usagePercent * 100,
                    used / (1024 * 1024),
                    max / (1024 * 1024));
            logMemoryDetails();
        } else if (heapWarning.get() && !wasWarning) {
            log.warn("⚠️  WARNING: Heap memory usage at {:.1f}% ({} MB / {} MB)",
                    usagePercent * 100,
                    used / (1024 * 1024),
                    max / (1024 * 1024));
        } else if (!heapWarning.get() && wasWarning) {
            log.info("✅ Heap memory back to normal: {:.1f}%", usagePercent * 100);
        }

        // Check for memory leak symptoms (high usage + frequent GC)
        detectMemoryLeak(usagePercent);
    }

    /**
     * Detect potential memory leak patterns
     */
    private void detectMemoryLeak(double usagePercent) {
        if (usagePercent > heapWarningThreshold) {
            // Check if memory stays high despite GC
            long gcCount = ManagementFactory.getGarbageCollectorMXBeans().stream()
                    .mapToLong(gc -> gc.getCollectionCount())
                    .sum();

            long previousGcCount = totalGcCount.getAndSet(gcCount);

            if (gcCount > previousGcCount + 5) {
                // More than 5 GC cycles since last check, but memory still high
                log.warn("🔍 Potential memory leak detected: High memory ({:.1f}%) + frequent GC ({} cycles)",
                        usagePercent * 100, gcCount - previousGcCount);
            }
        }
    }

    /**
     * Log detailed memory information
     */
    private void logMemoryDetails() {
        MemoryUsage heapUsage = memoryMXBean.getHeapMemoryUsage();
        MemoryUsage nonHeapUsage = memoryMXBean.getNonHeapMemoryUsage();

        log.error("Memory Details:");
        log.error("  Heap:     used={} MB, committed={} MB, max={} MB",
                heapUsage.getUsed() / (1024 * 1024),
                heapUsage.getCommitted() / (1024 * 1024),
                heapUsage.getMax() / (1024 * 1024));
        log.error("  Non-Heap: used={} MB, committed={} MB, max={} MB",
                nonHeapUsage.getUsed() / (1024 * 1024),
                nonHeapUsage.getCommitted() / (1024 * 1024),
                nonHeapUsage.getMax() > 0 ? nonHeapUsage.getMax() / (1024 * 1024) : -1);

        // Log GC stats
        ManagementFactory.getGarbageCollectorMXBeans().forEach(gc -> {
            log.error("  GC ({}): count={}, time={} ms",
                    gc.getName(),
                    gc.getCollectionCount(),
                    gc.getCollectionTime());
        });

        // Log thread count
        log.error("  Threads: {}", ManagementFactory.getThreadMXBean().getThreadCount());
    }

    /**
     * Get current heap usage percentage
     */
    public double getHeapUsagePercent() {
        MemoryUsage heapUsage = memoryMXBean.getHeapMemoryUsage();
        return (double) heapUsage.getUsed() / heapUsage.getMax();
    }

    /**
     * Get current non-heap usage percentage
     */
    public double getNonHeapUsagePercent() {
        MemoryUsage nonHeapUsage = memoryMXBean.getNonHeapMemoryUsage();
        long max = nonHeapUsage.getMax();
        if (max > 0) {
            return (double) nonHeapUsage.getUsed() / max;
        }
        return 0.0;
    }

    /**
     * Check if memory pressure is high (should apply backpressure)
     */
    public boolean isMemoryPressureHigh() {
        return heapWarning.get();
    }

    /**
     * Check if memory is critical (should reject new requests)
     */
    public boolean isMemoryCritical() {
        return heapCritical.get();
    }

    /**
     * Trigger GC suggestion when memory is high
     */
    public void suggestGC() {
        if (heapWarning.get()) {
            log.info("Suggesting GC due to high memory usage");
            System.gc(); // Suggestion only, JVM may ignore
        }
    }

    /**
     * Get memory status for health check
     */
    public MemoryStatus getMemoryStatus() {
        MemoryUsage heapUsage = memoryMXBean.getHeapMemoryUsage();
        MemoryUsage nonHeapUsage = memoryMXBean.getNonHeapMemoryUsage();

        return new MemoryStatus(
                heapUsage.getUsed(),
                heapUsage.getMax(),
                getHeapUsagePercent(),
                nonHeapUsage.getUsed(),
                nonHeapUsage.getMax() > 0 ? nonHeapUsage.getMax() : -1,
                getNonHeapUsagePercent(),
                heapWarning.get(),
                heapCritical.get()
        );
    }

    /**
     * Memory status information
     */
    public static class MemoryStatus {
        private final long heapUsed;
        private final long heapMax;
        private final double heapPercent;
        private final long nonHeapUsed;
        private final long nonHeapMax;
        private final double nonHeapPercent;
        private final boolean warning;
        private final boolean critical;

        public MemoryStatus(long heapUsed, long heapMax, double heapPercent,
                           long nonHeapUsed, long nonHeapMax, double nonHeapPercent,
                           boolean warning, boolean critical) {
            this.heapUsed = heapUsed;
            this.heapMax = heapMax;
            this.heapPercent = heapPercent;
            this.nonHeapUsed = nonHeapUsed;
            this.nonHeapMax = nonHeapMax;
            this.nonHeapPercent = nonHeapPercent;
            this.warning = warning;
            this.critical = critical;
        }

        public long getHeapUsed() { return heapUsed; }
        public long getHeapMax() { return heapMax; }
        public double getHeapPercent() { return heapPercent; }
        public long getNonHeapUsed() { return nonHeapUsed; }
        public long getNonHeapMax() { return nonHeapMax; }
        public double getNonHeapPercent() { return nonHeapPercent; }
        public boolean isWarning() { return warning; }
        public boolean isCritical() { return critical; }
    }
}
