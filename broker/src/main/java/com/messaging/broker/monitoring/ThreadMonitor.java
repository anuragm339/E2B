package com.messaging.broker.monitoring;

import io.micrometer.core.instrument.Gauge;
import io.micrometer.core.instrument.MeterRegistry;
import io.micronaut.scheduling.annotation.Scheduled;
import jakarta.annotation.PostConstruct;
import jakarta.inject.Singleton;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.management.ManagementFactory;
import java.lang.management.ThreadInfo;
import java.lang.management.ThreadMXBean;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;

/**
 * Monitors thread usage, CPU time, and detects problematic threads
 */
@Singleton
public class ThreadMonitor {
    private static final Logger log = LoggerFactory.getLogger(ThreadMonitor.class);
    private static final int TOP_THREADS_TO_LOG = 5;

    private final ThreadMXBean threadMXBean;
    private final MeterRegistry meterRegistry;
    private final Map<Long, ThreadStats> previousStats;
    private final AtomicLong blockedThreadCount;
    private final AtomicLong waitingThreadCount;

    public ThreadMonitor(MeterRegistry meterRegistry) {
        this.threadMXBean = ManagementFactory.getThreadMXBean();
        this.meterRegistry = meterRegistry;
        this.previousStats = new ConcurrentHashMap<>();
        this.blockedThreadCount = new AtomicLong(0);
        this.waitingThreadCount = new AtomicLong(0);

        // Enable CPU time tracking
        if (threadMXBean.isThreadCpuTimeSupported()) {
            threadMXBean.setThreadCpuTimeEnabled(true);
        }
    }

    @PostConstruct
    public void init() {
        // Register metrics
        Gauge.builder("broker.threads.blocked", blockedThreadCount, AtomicLong::get)
                .description("Number of blocked threads")
                .register(meterRegistry);

        Gauge.builder("broker.threads.waiting", waitingThreadCount, AtomicLong::get)
                .description("Number of waiting threads")
                .register(meterRegistry);

        Gauge.builder("broker.threads.deadlocked", this, ThreadMonitor::getDeadlockedThreadCount)
                .description("Number of deadlocked threads")
                .register(meterRegistry);

        // Register per-category resource metrics that are updated periodically
        Gauge.builder("broker.threads.category.cpu_time_ms", this,
                monitor -> getTopCategoryCpuTime("Network I/O"))
                .description("CPU time in ms for Network I/O threads")
                .tag("category", "Network I/O")
                .register(meterRegistry);

        Gauge.builder("broker.threads.category.cpu_time_ms", this,
                monitor -> getTopCategoryCpuTime("Consumer Delivery"))
                .description("CPU time in ms for Consumer Delivery threads")
                .tag("category", "Consumer Delivery")
                .register(meterRegistry);

        Gauge.builder("broker.threads.category.cpu_time_ms", this,
                monitor -> getTopCategoryCpuTime("Storage"))
                .description("CPU time in ms for Storage threads")
                .tag("category", "Storage")
                .register(meterRegistry);

        Gauge.builder("broker.threads.category.cpu_time_ms", this,
                monitor -> getTopCategoryCpuTime("Pipe/Replication"))
                .description("CPU time in ms for Pipe/Replication threads")
                .tag("category", "Pipe/Replication")
                .register(meterRegistry);

        Gauge.builder("broker.threads.category.cpu_time_ms", this,
                monitor -> getTopCategoryCpuTime("HTTP Server"))
                .description("CPU time in ms for HTTP Server threads")
                .tag("category", "HTTP Server")
                .register(meterRegistry);

        Gauge.builder("broker.threads.category.memory_mb", this,
                monitor -> getTopCategoryMemory("Network I/O"))
                .description("Allocated memory in MB for Network I/O threads")
                .tag("category", "Network I/O")
                .register(meterRegistry);

        Gauge.builder("broker.threads.category.memory_mb", this,
                monitor -> getTopCategoryMemory("Consumer Delivery"))
                .description("Allocated memory in MB for Consumer Delivery threads")
                .tag("category", "Consumer Delivery")
                .register(meterRegistry);

        Gauge.builder("broker.threads.category.memory_mb", this,
                monitor -> getTopCategoryMemory("Storage"))
                .description("Allocated memory in MB for Storage threads")
                .tag("category", "Storage")
                .register(meterRegistry);

        Gauge.builder("broker.threads.category.memory_mb", this,
                monitor -> getTopCategoryMemory("Pipe/Replication"))
                .description("Allocated memory in MB for Pipe/Replication threads")
                .tag("category", "Pipe/Replication")
                .register(meterRegistry);

        Gauge.builder("broker.threads.category.memory_mb", this,
                monitor -> getTopCategoryMemory("HTTP Server"))
                .description("Allocated memory in MB for HTTP Server threads")
                .tag("category", "HTTP Server")
                .register(meterRegistry);

        // Register per-category thread counts
        Gauge.builder("broker.threads.category.count", this,
                monitor -> getCategoryThreadCount("Network I/O"))
                .description("Thread count for Network I/O")
                .tag("category", "Network I/O")
                .register(meterRegistry);

        Gauge.builder("broker.threads.category.count", this,
                monitor -> getCategoryThreadCount("Consumer Delivery"))
                .description("Thread count for Consumer Delivery")
                .tag("category", "Consumer Delivery")
                .register(meterRegistry);

        Gauge.builder("broker.threads.category.count", this,
                monitor -> getCategoryThreadCount("Storage"))
                .description("Thread count for Storage")
                .tag("category", "Storage")
                .register(meterRegistry);

        Gauge.builder("broker.threads.category.count", this,
                monitor -> getCategoryThreadCount("Pipe/Replication"))
                .description("Thread count for Pipe/Replication")
                .tag("category", "Pipe/Replication")
                .register(meterRegistry);

        Gauge.builder("broker.threads.category.count", this,
                monitor -> getCategoryThreadCount("HTTP Server"))
                .description("Thread count for HTTP Server")
                .tag("category", "HTTP Server")
                .register(meterRegistry);

        // Enable thread memory allocation tracking if supported
        try {
            com.sun.management.ThreadMXBean sunThreadMXBean =
                (com.sun.management.ThreadMXBean) threadMXBean;
            if (sunThreadMXBean.isThreadAllocatedMemorySupported()) {
                sunThreadMXBean.setThreadAllocatedMemoryEnabled(true);
                log.debug("ThreadMonitor initialized - CPU time tracking: {}, Memory allocation tracking: {}",
                        threadMXBean.isThreadCpuTimeSupported(), true);
            } else {
                log.debug("ThreadMonitor initialized - CPU time tracking: {}, Memory allocation tracking: not supported",
                        threadMXBean.isThreadCpuTimeSupported());
            }
        } catch (Exception e) {
            log.warn("Could not enable thread memory allocation tracking", e);
            log.error("ThreadMonitor initialized, CPU time tracking enabled: {}",
                    threadMXBean.isThreadCpuTimeSupported());
        }
    }

    /**
     * Check thread usage every 30 seconds
     */
    @Scheduled(fixedDelay = "30s", initialDelay = "30s")
    public void checkThreads() {
        long[] allThreadIds = threadMXBean.getAllThreadIds();
        List<ThreadCpuUsage> cpuUsages = new ArrayList<>();
        Map<String, ThreadGroupStats> groupStats = new HashMap<>();

        long totalBlocked = 0;
        long totalWaiting = 0;

        for (long threadId : allThreadIds) {
            ThreadInfo info = threadMXBean.getThreadInfo(threadId, 0);
            if (info == null) continue; // Thread terminated

            // Count blocked/waiting threads
            Thread.State state = info.getThreadState();
            if (state == Thread.State.BLOCKED) totalBlocked++;
            if (state == Thread.State.WAITING || state == Thread.State.TIMED_WAITING) totalWaiting++;

            // Categorize thread by name pattern
            String threadName = info.getThreadName();
            String category = categorizeThread(threadName);

            // Calculate CPU usage
            if (threadMXBean.isThreadCpuTimeSupported()) {
                long cpuTime = threadMXBean.getThreadCpuTime(threadId);
                long userTime = threadMXBean.getThreadUserTime(threadId);
                long allocatedBytes = getThreadAllocatedBytes(threadId);

                ThreadStats previous = previousStats.get(threadId);
                if (previous != null && cpuTime > 0) {
                    long cpuDelta = cpuTime - previous.cpuTime;
                    long userDelta = userTime - previous.userTime;
                    long allocatedDelta = allocatedBytes - previous.allocatedBytes;

                    if (cpuDelta > 0 || allocatedDelta > 0) {
                        cpuUsages.add(new ThreadCpuUsage(
                                threadId,
                                threadName,
                                state,
                                cpuDelta,
                                userDelta,
                                allocatedDelta,
                                info.getBlockedCount(),
                                info.getWaitedCount()
                        ));
                    }

                    // Aggregate by category
                    groupStats.computeIfAbsent(category, k -> new ThreadGroupStats(category))
                            .add(cpuDelta, allocatedDelta);
                }

                // Store current stats for next comparison
                previousStats.put(threadId, new ThreadStats(cpuTime, userTime, allocatedBytes));
            }
        }

        // Update metrics
        blockedThreadCount.set(totalBlocked);
        waitingThreadCount.set(totalWaiting);

        // Log top CPU-consuming threads
        if (!cpuUsages.isEmpty()) {
            List<ThreadCpuUsage> topCpuThreads = cpuUsages.stream()
                    .sorted(Comparator.comparingLong(ThreadCpuUsage::getCpuTimeNanos).reversed())
                    .limit(TOP_THREADS_TO_LOG)
                    .collect(Collectors.toList());

            List<ThreadCpuUsage> topMemoryThreads = cpuUsages.stream()
                    .sorted(Comparator.comparingLong(ThreadCpuUsage::getAllocatedBytes).reversed())
                    .limit(TOP_THREADS_TO_LOG)
                    .collect(Collectors.toList());

            logTopThreads(topCpuThreads, topMemoryThreads, groupStats, totalBlocked, totalWaiting);
        }

        // Check for deadlocks
        checkForDeadlocks();

        // Cleanup terminated threads from stats
        cleanupTerminatedThreads(allThreadIds);
    }

    /**
     * Categorize thread by name pattern
     */
    private String categorizeThread(String threadName) {
        if (threadName.contains("nioEventLoopGroup") || threadName.contains("netty")) {
            return "Network I/O";
        } else if (threadName.contains("RemoteConsumerRegistry") || threadName.contains("consumer-delivery") || threadName.contains("delivery")) {
            return "Consumer Delivery";
        } else if (threadName.contains("StorageReader") || threadName.contains("storage") || threadName.contains("segment")) {
            return "Storage";
        } else if (threadName.contains("HttpPipeConnector") || threadName.contains("pipe")) {
            return "Pipe/Replication";
        } else if (threadName.contains("scheduled") || threadName.contains("executor")) {
            return "Scheduled Tasks";
        } else if (threadName.startsWith("G1 ") || threadName.contains("GC")) {
            return "Garbage Collection";
        } else if (threadName.contains("pool")) {
            return "Thread Pool";
        } else if (threadName.contains("http") || threadName.contains("Micronaut")) {
            return "HTTP Server";
        } else {
            return "Other";
        }
    }

    /**
     * Get bytes allocated by thread (if supported)
     */
    private long getThreadAllocatedBytes(long threadId) {
        try {
            com.sun.management.ThreadMXBean sunThreadMXBean =
                (com.sun.management.ThreadMXBean) threadMXBean;
            if (sunThreadMXBean.isThreadAllocatedMemorySupported() &&
                sunThreadMXBean.isThreadAllocatedMemoryEnabled()) {
                return sunThreadMXBean.getThreadAllocatedBytes(threadId);
            }
        } catch (Exception e) {
            // Not supported on this JVM
        }
        return 0;
    }

    /**
     * Log top CPU-consuming and memory-allocating threads
     */
    private void logTopThreads(List<ThreadCpuUsage> topCpuThreads,
                               List<ThreadCpuUsage> topMemoryThreads,
                               Map<String, ThreadGroupStats> groupStats,
                               long totalBlocked, long totalWaiting) {
        //log.info("═══════════════════════════════════════════════════════════════");
        //log.info("Thread Activity Report - Total: {}, Blocked: {}, Waiting: {}",
         //       threadMXBean.getThreadCount(), totalBlocked, totalWaiting);
       // log.info("───────────────────────────────────────────────────────────────");

        // Log by category
       // log.info("Memory Allocation by Category (last 30s):");
        groupStats.entrySet().stream()
                .sorted((a, b) -> Long.compare(b.getValue().allocatedBytes, a.getValue().allocatedBytes))
                .forEach(entry -> {
                    ThreadGroupStats stats = entry.getValue();
                    double allocatedMB = stats.allocatedBytes / (1024.0 * 1024.0);
                    double cpuMs = stats.cpuTimeNanos / 1_000_000.0;
                    log.debug("  {} - Memory: {:.2f} MB, CPU: {:.1f}ms",
                            entry.getKey(), allocatedMB, cpuMs);
                });

       // log.info("───────────────────────────────────────────────────────────────");
        //log.info("Top {} CPU-consuming threads (last 30s):", TOP_THREADS_TO_LOG);

        for (int i = 0; i < topCpuThreads.size(); i++) {
            ThreadCpuUsage usage = topCpuThreads.get(i);
            double cpuMs = usage.cpuTimeNanos / 1_000_000.0;
            double cpuPercent = (usage.cpuTimeNanos / 30_000_000_000.0) * 100;
            double allocatedKB = usage.allocatedBytes / 1024.0;

            log.debug("  {}. [{}] {} - CPU: {:.1f}ms ({:.1f}%), Memory: {:.1f} KB, State: {}",
                    i + 1,
                    usage.threadId,
                    usage.threadName,
                    cpuMs,
                    cpuPercent,
                    allocatedKB,
                    usage.state);
        }

        log.debug("───────────────────────────────────────────────────────────────");
        log.debug("Top {} Memory-allocating threads (last 30s):", TOP_THREADS_TO_LOG);

        for (int i = 0; i < topMemoryThreads.size(); i++) {
            ThreadCpuUsage usage = topMemoryThreads.get(i);
            double allocatedMB = usage.allocatedBytes / (1024.0 * 1024.0);
            double cpuMs = usage.cpuTimeNanos / 1_000_000.0;

            if (allocatedMB > 0.01) { // Only log if > 10KB
                log.debug("  {}. [{}] {} - Memory: {:.2f} MB, CPU: {:.1f}ms, State: {}",
                        i + 1,
                        usage.threadId,
                        usage.threadName,
                        allocatedMB,
                        cpuMs,
                        usage.state);
            }
        }
        log.debug("═══════════════════════════════════════════════════════════════");
    }

    /**
     * Check for deadlocked threads
     */
    private void checkForDeadlocks() {
        long[] deadlockedThreads = threadMXBean.findDeadlockedThreads();
        if (deadlockedThreads != null && deadlockedThreads.length > 0) {
            log.error("🚨 DEADLOCK DETECTED! {} threads are deadlocked:", deadlockedThreads.length);

            for (long threadId : deadlockedThreads) {
                ThreadInfo info = threadMXBean.getThreadInfo(threadId, Integer.MAX_VALUE);
                if (info != null) {
                    log.error("  Deadlocked Thread [{}]: {}", threadId, info.getThreadName());
                    log.error("    State: {}", info.getThreadState());
                    log.error("    Lock: {}", info.getLockName());
                    log.error("    Lock Owner: {}", info.getLockOwnerName());

                    // Log stack trace
                    StackTraceElement[] stackTrace = info.getStackTrace();
                    for (int i = 0; i < Math.min(10, stackTrace.length); i++) {
                        log.error("      at {}", stackTrace[i]);
                    }
                }
            }

            log.error("  Full thread dump saved to logs. Consider restarting broker!");
        }
    }

    /**
     * Get deadlocked thread count for metrics
     */
    private int getDeadlockedThreadCount() {
        long[] deadlockedThreads = threadMXBean.findDeadlockedThreads();
        return deadlockedThreads != null ? deadlockedThreads.length : 0;
    }

    /**
     * Cleanup stats for terminated threads
     */
    private void cleanupTerminatedThreads(long[] activeThreadIds) {
        Set<Long> activeSet = Arrays.stream(activeThreadIds).boxed().collect(Collectors.toSet());
        previousStats.keySet().removeIf(threadId -> !activeSet.contains(threadId));
    }

    /**
     * Get detailed thread dump as string
     */
    public String getThreadDump() {
        StringBuilder dump = new StringBuilder();
        dump.append("Full Thread Dump:\n");
        dump.append("═══════════════════════════════════════════════════════════════\n");

        ThreadInfo[] threadInfos = threadMXBean.dumpAllThreads(true, true);
        for (ThreadInfo info : threadInfos) {
            dump.append(formatThreadInfo(info));
            dump.append("\n");
        }

        return dump.toString();
    }

    /**
     * Format thread info for dumping
     */
    private String formatThreadInfo(ThreadInfo info) {
        StringBuilder sb = new StringBuilder();
        sb.append(String.format("\"%s\" #%d %s\n",
                info.getThreadName(), info.getThreadId(), info.getThreadState()));

        if (info.getLockName() != null) {
            sb.append(String.format("  Waiting on: %s\n", info.getLockName()));
        }

        if (info.getLockOwnerName() != null) {
            sb.append(String.format("  Owned by: \"%s\" #%d\n",
                    info.getLockOwnerName(), info.getLockOwnerId()));
        }

        sb.append(String.format("  Blocked count: %d, Waited count: %d\n",
                info.getBlockedCount(), info.getWaitedCount()));

        // Stack trace
        StackTraceElement[] stackTrace = info.getStackTrace();
        for (StackTraceElement element : stackTrace) {
            sb.append(String.format("    at %s\n", element));
        }

        return sb.toString();
    }

    /**
     * Get problematic threads (high blocked/waited counts)
     */
    public List<ProblematicThread> getProblematicThreads() {
        List<ProblematicThread> problematic = new ArrayList<>();
        long[] allThreadIds = threadMXBean.getAllThreadIds();

        for (long threadId : allThreadIds) {
            ThreadInfo info = threadMXBean.getThreadInfo(threadId, 0);
            if (info == null) continue;

            // Flag threads with excessive blocking/waiting
            if (info.getBlockedCount() > 1000 || info.getWaitedCount() > 10000) {
                problematic.add(new ProblematicThread(
                        threadId,
                        info.getThreadName(),
                        info.getThreadState(),
                        info.getBlockedCount(),
                        info.getWaitedCount(),
                        "High blocked/waited count"
                ));
            }

            // Flag threads stuck in BLOCKED state
            if (info.getThreadState() == Thread.State.BLOCKED) {
                String lockInfo = info.getLockName() != null ? info.getLockName() : "unknown";
                problematic.add(new ProblematicThread(
                        threadId,
                        info.getThreadName(),
                        info.getThreadState(),
                        info.getBlockedCount(),
                        info.getWaitedCount(),
                        "Currently blocked on: " + lockInfo
                ));
            }
        }

        return problematic;
    }

    /**
     * Thread statistics holder
     */
    private static class ThreadStats {
        final long cpuTime;
        final long userTime;
        final long allocatedBytes;

        ThreadStats(long cpuTime, long userTime, long allocatedBytes) {
            this.cpuTime = cpuTime;
            this.userTime = userTime;
            this.allocatedBytes = allocatedBytes;
        }
    }

    /**
     * Thread CPU usage information
     */
    private static class ThreadCpuUsage {
        final long threadId;
        final String threadName;
        final Thread.State state;
        final long cpuTimeNanos;
        final long userTimeNanos;
        final long allocatedBytes;
        final long blockedCount;
        final long waitedCount;

        ThreadCpuUsage(long threadId, String threadName, Thread.State state,
                      long cpuTimeNanos, long userTimeNanos, long allocatedBytes,
                      long blockedCount, long waitedCount) {
            this.threadId = threadId;
            this.threadName = threadName;
            this.state = state;
            this.cpuTimeNanos = cpuTimeNanos;
            this.userTimeNanos = userTimeNanos;
            this.allocatedBytes = allocatedBytes;
            this.blockedCount = blockedCount;
            this.waitedCount = waitedCount;
        }

        long getCpuTimeNanos() {
            return cpuTimeNanos;
        }

        long getAllocatedBytes() {
            return allocatedBytes;
        }
    }

    /**
     * Aggregated stats by thread category
     */
    private static class ThreadGroupStats {
        final String category;
        long cpuTimeNanos = 0;
        long allocatedBytes = 0;

        ThreadGroupStats(String category) {
            this.category = category;
        }

        void add(long cpuDelta, long allocatedDelta) {
            this.cpuTimeNanos += cpuDelta;
            this.allocatedBytes += allocatedDelta;
        }
    }

    /**
     * Get CPU time for a specific category (for metrics)
     */
    private double getTopCategoryCpuTime(String category) {
        Map<String, CategoryResourceInfo> categoryStats = getResourcesByCategory();
        CategoryResourceInfo info = categoryStats.get(category);
        return info != null ? info.getTotalCpuTimeNanos() / 1_000_000.0 : 0.0;
    }

    /**
     * Get memory allocation for a specific category (for metrics)
     */
    private double getTopCategoryMemory(String category) {
        Map<String, CategoryResourceInfo> categoryStats = getResourcesByCategory();
        CategoryResourceInfo info = categoryStats.get(category);
        return info != null ? info.getTotalAllocatedBytes() / (1024.0 * 1024.0) : 0.0;
    }

    /**
     * Get thread count for a specific category (for metrics)
     */
    private int getCategoryThreadCount(String category) {
        Map<String, CategoryResourceInfo> categoryStats = getResourcesByCategory();
        CategoryResourceInfo info = categoryStats.get(category);
        return info != null ? info.getThreadCount() : 0;
    }

    /**
     * Get all threads with resource metrics
     */
    public List<ThreadResourceInfo> getAllThreadResources() {
        List<ThreadResourceInfo> resources = new ArrayList<>();
        long[] allThreadIds = threadMXBean.getAllThreadIds();

        for (long threadId : allThreadIds) {
            ThreadInfo info = threadMXBean.getThreadInfo(threadId, 0);
            if (info == null) continue;

            long cpuTime = 0;
            long userTime = 0;
            long allocatedBytes = 0;

            if (threadMXBean.isThreadCpuTimeSupported()) {
                cpuTime = threadMXBean.getThreadCpuTime(threadId);
                userTime = threadMXBean.getThreadUserTime(threadId);
            }

            allocatedBytes = getThreadAllocatedBytes(threadId);

            resources.add(new ThreadResourceInfo(
                threadId,
                info.getThreadName(),
                categorizeThread(info.getThreadName()),
                info.getThreadState(),
                cpuTime,
                userTime,
                allocatedBytes,
                info.getBlockedCount(),
                info.getWaitedCount()
            ));
        }

        return resources;
    }

    /**
     * Get top memory-consuming threads
     */
    public List<ThreadResourceInfo> getTopMemoryThreads(int limit) {
        return getAllThreadResources().stream()
                .sorted(Comparator.comparingLong(ThreadResourceInfo::getAllocatedBytes).reversed())
                .limit(limit)
                .collect(Collectors.toList());
    }

    /**
     * Get top CPU-consuming threads
     */
    public List<ThreadResourceInfo> getTopCpuThreads(int limit) {
        return getAllThreadResources().stream()
                .sorted(Comparator.comparingLong(ThreadResourceInfo::getCpuTimeNanos).reversed())
                .limit(limit)
                .collect(Collectors.toList());
    }

    /**
     * Get threads grouped by category with aggregated stats
     */
    public Map<String, CategoryResourceInfo> getResourcesByCategory() {
        Map<String, CategoryResourceInfo> categoryMap = new HashMap<>();

        for (ThreadResourceInfo thread : getAllThreadResources()) {
            categoryMap.computeIfAbsent(thread.getCategory(),
                k -> new CategoryResourceInfo(k))
                .add(thread);
        }

        return categoryMap;
    }

    /**
     * Thread resource information (public API)
     */
    public static class ThreadResourceInfo {
        private final long threadId;
        private final String threadName;
        private final String category;
        private final Thread.State state;
        private final long cpuTimeNanos;
        private final long userTimeNanos;
        private final long allocatedBytes;
        private final long blockedCount;
        private final long waitedCount;

        public ThreadResourceInfo(long threadId, String threadName, String category,
                                 Thread.State state, long cpuTimeNanos, long userTimeNanos,
                                 long allocatedBytes, long blockedCount, long waitedCount) {
            this.threadId = threadId;
            this.threadName = threadName;
            this.category = category;
            this.state = state;
            this.cpuTimeNanos = cpuTimeNanos;
            this.userTimeNanos = userTimeNanos;
            this.allocatedBytes = allocatedBytes;
            this.blockedCount = blockedCount;
            this.waitedCount = waitedCount;
        }

        public long getThreadId() { return threadId; }
        public String getThreadName() { return threadName; }
        public String getCategory() { return category; }
        public Thread.State getState() { return state; }
        public long getCpuTimeNanos() { return cpuTimeNanos; }
        public long getUserTimeNanos() { return userTimeNanos; }
        public long getAllocatedBytes() { return allocatedBytes; }
        public long getBlockedCount() { return blockedCount; }
        public long getWaitedCount() { return waitedCount; }
    }

    /**
     * Category resource aggregation
     */
    public static class CategoryResourceInfo {
        private final String category;
        private long totalCpuTimeNanos = 0;
        private long totalAllocatedBytes = 0;
        private int threadCount = 0;

        public CategoryResourceInfo(String category) {
            this.category = category;
        }

        void add(ThreadResourceInfo thread) {
            this.totalCpuTimeNanos += thread.getCpuTimeNanos();
            this.totalAllocatedBytes += thread.getAllocatedBytes();
            this.threadCount++;
        }

        public String getCategory() { return category; }
        public long getTotalCpuTimeNanos() { return totalCpuTimeNanos; }
        public long getTotalAllocatedBytes() { return totalAllocatedBytes; }
        public int getThreadCount() { return threadCount; }
    }

    /**
     * Problematic thread information
     */
    public static class ProblematicThread {
        private final long threadId;
        private final String threadName;
        private final Thread.State state;
        private final long blockedCount;
        private final long waitedCount;
        private final String reason;

        public ProblematicThread(long threadId, String threadName, Thread.State state,
                                long blockedCount, long waitedCount, String reason) {
            this.threadId = threadId;
            this.threadName = threadName;
            this.state = state;
            this.blockedCount = blockedCount;
            this.waitedCount = waitedCount;
            this.reason = reason;
        }

        public long getThreadId() { return threadId; }
        public String getThreadName() { return threadName; }
        public Thread.State getState() { return state; }
        public long getBlockedCount() { return blockedCount; }
        public long getWaitedCount() { return waitedCount; }
        public String getReason() { return reason; }
    }
}
