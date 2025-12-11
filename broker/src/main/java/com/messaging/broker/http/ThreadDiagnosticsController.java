package com.messaging.broker.http;

import com.messaging.broker.monitoring.ThreadMonitor;
import io.micronaut.http.annotation.Controller;
import io.micronaut.http.annotation.Get;
import io.micronaut.http.annotation.Produces;
import io.micronaut.http.MediaType;
import jakarta.inject.Inject;

import java.lang.management.ManagementFactory;
import java.lang.management.ThreadInfo;
import java.lang.management.ThreadMXBean;
import java.util.*;

/**
 * HTTP endpoints for thread diagnostics
 */
@Controller("/diagnostics/threads")
public class ThreadDiagnosticsController {

    private final ThreadMonitor threadMonitor;
    private final ThreadMXBean threadMXBean;

    @Inject
    public ThreadDiagnosticsController(ThreadMonitor threadMonitor) {
        this.threadMonitor = threadMonitor;
        this.threadMXBean = ManagementFactory.getThreadMXBean();
    }

    /**
     * Get thread summary
     * GET /diagnostics/threads/summary
     */
    @Get("/summary")
    @Produces(MediaType.APPLICATION_JSON)
    public Map<String, Object> getThreadSummary() {
        Map<String, Object> summary = new HashMap<>();

        // Basic stats
        summary.put("totalThreads", threadMXBean.getThreadCount());
        summary.put("peakThreads", threadMXBean.getPeakThreadCount());
        summary.put("daemonThreads", threadMXBean.getDaemonThreadCount());
        summary.put("totalStartedThreads", threadMXBean.getTotalStartedThreadCount());

        // Thread states
        Map<Thread.State, Integer> stateCount = new HashMap<>();
        long[] allThreadIds = threadMXBean.getAllThreadIds();
        for (long threadId : allThreadIds) {
            ThreadInfo info = threadMXBean.getThreadInfo(threadId, 0);
            if (info != null) {
                stateCount.merge(info.getThreadState(), 1, Integer::sum);
            }
        }
        summary.put("threadsByState", stateCount);

        // Deadlocks
        long[] deadlockedThreads = threadMXBean.findDeadlockedThreads();
        summary.put("deadlockedThreads", deadlockedThreads != null ? deadlockedThreads.length : 0);

        // Problematic threads
        List<ThreadMonitor.ProblematicThread> problematic = threadMonitor.getProblematicThreads();
        summary.put("problematicThreads", problematic.size());

        return summary;
    }

    /**
     * Get top CPU-consuming threads
     * GET /diagnostics/threads/top-cpu?limit=10
     */
    @Get("/top-cpu{?limit}")
    @Produces(MediaType.APPLICATION_JSON)
    public List<Map<String, Object>> getTopCpuThreads(Optional<Integer> limit) {
        int maxThreads = limit.orElse(10);
        List<Map<String, Object>> topThreads = new ArrayList<>();

        long[] allThreadIds = threadMXBean.getAllThreadIds();

        // Collect all threads with CPU time
        List<ThreadCpuInfo> threadCpuInfos = new ArrayList<>();
        for (long threadId : allThreadIds) {
            ThreadInfo info = threadMXBean.getThreadInfo(threadId, 0);
            if (info != null && threadMXBean.isThreadCpuTimeSupported()) {
                long cpuTime = threadMXBean.getThreadCpuTime(threadId);
                long userTime = threadMXBean.getThreadUserTime(threadId);

                if (cpuTime > 0) {
                    threadCpuInfos.add(new ThreadCpuInfo(
                            threadId,
                            info.getThreadName(),
                            info.getThreadState(),
                            cpuTime,
                            userTime,
                            info.getBlockedCount(),
                            info.getWaitedCount()
                    ));
                }
            }
        }

        // Sort by CPU time and take top N
        threadCpuInfos.stream()
                .sorted(Comparator.comparingLong(ThreadCpuInfo::getCpuTime).reversed())
                .limit(maxThreads)
                .forEach(cpuInfo -> {
                    Map<String, Object> threadData = new HashMap<>();
                    threadData.put("threadId", cpuInfo.threadId);
                    threadData.put("threadName", cpuInfo.threadName);
                    threadData.put("state", cpuInfo.state.toString());
                    threadData.put("cpuTimeMs", cpuInfo.cpuTime / 1_000_000);
                    threadData.put("userTimeMs", cpuInfo.userTime / 1_000_000);
                    threadData.put("blockedCount", cpuInfo.blockedCount);
                    threadData.put("waitedCount", cpuInfo.waitedCount);
                    topThreads.add(threadData);
                });

        return topThreads;
    }

    /**
     * Get problematic threads
     * GET /diagnostics/threads/problematic
     */
    @Get("/problematic")
    @Produces(MediaType.APPLICATION_JSON)
    public List<Map<String, Object>> getProblematicThreads() {
        List<Map<String, Object>> result = new ArrayList<>();

        List<ThreadMonitor.ProblematicThread> problematic = threadMonitor.getProblematicThreads();
        for (ThreadMonitor.ProblematicThread thread : problematic) {
            Map<String, Object> threadData = new HashMap<>();
            threadData.put("threadId", thread.getThreadId());
            threadData.put("threadName", thread.getThreadName());
            threadData.put("state", thread.getState().toString());
            threadData.put("blockedCount", thread.getBlockedCount());
            threadData.put("waitedCount", thread.getWaitedCount());
            threadData.put("reason", thread.getReason());
            result.add(threadData);
        }

        return result;
    }

    /**
     * Get full thread dump
     * GET /diagnostics/threads/dump
     */
    @Get("/dump")
    @Produces(MediaType.TEXT_PLAIN)
    public String getThreadDump() {
        return threadMonitor.getThreadDump();
    }

    /**
     * Get specific thread details
     * GET /diagnostics/threads/123
     */
    @Get("/{threadId}")
    @Produces(MediaType.APPLICATION_JSON)
    public Map<String, Object> getThreadDetails(long threadId) {
        ThreadInfo info = threadMXBean.getThreadInfo(threadId, Integer.MAX_VALUE);

        if (info == null) {
            return Map.of("error", "Thread not found or terminated");
        }

        Map<String, Object> details = new HashMap<>();
        details.put("threadId", info.getThreadId());
        details.put("threadName", info.getThreadName());
        details.put("state", info.getThreadState().toString());
        details.put("blockedCount", info.getBlockedCount());
        details.put("blockedTime", info.getBlockedTime());
        details.put("waitedCount", info.getWaitedCount());
        details.put("waitedTime", info.getWaitedTime());

        if (info.getLockName() != null) {
            details.put("lockName", info.getLockName());
            details.put("lockOwnerName", info.getLockOwnerName());
            details.put("lockOwnerId", info.getLockOwnerId());
        }

        if (threadMXBean.isThreadCpuTimeSupported()) {
            long cpuTime = threadMXBean.getThreadCpuTime(threadId);
            long userTime = threadMXBean.getThreadUserTime(threadId);
            details.put("cpuTimeMs", cpuTime / 1_000_000);
            details.put("userTimeMs", userTime / 1_000_000);
        }

        // Stack trace
        StackTraceElement[] stackTrace = info.getStackTrace();
        List<String> stack = new ArrayList<>();
        for (StackTraceElement element : stackTrace) {
            stack.add(element.toString());
        }
        details.put("stackTrace", stack);

        return details;
    }

    /**
     * Check for deadlocks
     * GET /diagnostics/threads/deadlocks
     */
    @Get("/deadlocks")
    @Produces(MediaType.APPLICATION_JSON)
    public Map<String, Object> checkDeadlocks() {
        Map<String, Object> result = new HashMap<>();

        long[] deadlockedThreads = threadMXBean.findDeadlockedThreads();

        if (deadlockedThreads == null || deadlockedThreads.length == 0) {
            result.put("deadlocked", false);
            result.put("count", 0);
            result.put("message", "No deadlocks detected");
            return result;
        }

        result.put("deadlocked", true);
        result.put("count", deadlockedThreads.length);

        List<Map<String, Object>> deadlockInfo = new ArrayList<>();
        for (long threadId : deadlockedThreads) {
            ThreadInfo info = threadMXBean.getThreadInfo(threadId, Integer.MAX_VALUE);
            if (info != null) {
                Map<String, Object> threadData = new HashMap<>();
                threadData.put("threadId", info.getThreadId());
                threadData.put("threadName", info.getThreadName());
                threadData.put("state", info.getThreadState().toString());
                threadData.put("lockName", info.getLockName());
                threadData.put("lockOwnerName", info.getLockOwnerName());
                threadData.put("lockOwnerId", info.getLockOwnerId());

                // Stack trace
                StackTraceElement[] stackTrace = info.getStackTrace();
                List<String> stack = new ArrayList<>();
                for (int i = 0; i < Math.min(10, stackTrace.length); i++) {
                    stack.add(stackTrace[i].toString());
                }
                threadData.put("stackTrace", stack);

                deadlockInfo.add(threadData);
            }
        }

        result.put("threads", deadlockInfo);
        return result;
    }

    /**
     * Get top memory-consuming threads
     * GET /diagnostics/threads/top-memory?limit=10
     */
    @Get("/top-memory{?limit}")
    @Produces(MediaType.APPLICATION_JSON)
    public List<Map<String, Object>> getTopMemoryThreads(Optional<Integer> limit) {
        int maxThreads = limit.orElse(10);
        List<Map<String, Object>> result = new ArrayList<>();

        List<ThreadMonitor.ThreadResourceInfo> topThreads =
            threadMonitor.getTopMemoryThreads(maxThreads);

        for (ThreadMonitor.ThreadResourceInfo thread : topThreads) {
            Map<String, Object> threadData = new HashMap<>();
            threadData.put("threadId", thread.getThreadId());
            threadData.put("threadName", thread.getThreadName());
            threadData.put("category", thread.getCategory());
            threadData.put("state", thread.getState().toString());
            threadData.put("allocatedBytes", thread.getAllocatedBytes());
            threadData.put("allocatedMB", String.format("%.2f", thread.getAllocatedBytes() / (1024.0 * 1024.0)));
            threadData.put("cpuTimeMs", thread.getCpuTimeNanos() / 1_000_000);
            threadData.put("userTimeMs", thread.getUserTimeNanos() / 1_000_000);
            threadData.put("blockedCount", thread.getBlockedCount());
            threadData.put("waitedCount", thread.getWaitedCount());
            result.add(threadData);
        }

        return result;
    }

    /**
     * Get all thread resources with detailed metrics
     * GET /diagnostics/threads/resources?category=Network%20I/O
     */
    @Get("/resources{?category}")
    @Produces(MediaType.APPLICATION_JSON)
    public Map<String, Object> getThreadResources(Optional<String> category) {
        Map<String, Object> response = new HashMap<>();

        List<ThreadMonitor.ThreadResourceInfo> allThreads =
            threadMonitor.getAllThreadResources();

        // Filter by category if specified
        if (category.isPresent()) {
            allThreads = allThreads.stream()
                .filter(t -> t.getCategory().equals(category.get()))
                .collect(java.util.stream.Collectors.toList());
        }

        // Convert to response format
        List<Map<String, Object>> threads = new ArrayList<>();
        for (ThreadMonitor.ThreadResourceInfo thread : allThreads) {
            Map<String, Object> threadData = new HashMap<>();
            threadData.put("threadId", thread.getThreadId());
            threadData.put("threadName", thread.getThreadName());
            threadData.put("category", thread.getCategory());
            threadData.put("state", thread.getState().toString());
            threadData.put("cpuTimeMs", thread.getCpuTimeNanos() / 1_000_000);
            threadData.put("userTimeMs", thread.getUserTimeNanos() / 1_000_000);
            threadData.put("allocatedBytes", thread.getAllocatedBytes());
            threadData.put("allocatedMB", String.format("%.2f", thread.getAllocatedBytes() / (1024.0 * 1024.0)));
            threadData.put("blockedCount", thread.getBlockedCount());
            threadData.put("waitedCount", thread.getWaitedCount());
            threads.add(threadData);
        }

        response.put("threadCount", threads.size());
        response.put("threads", threads);

        return response;
    }

    /**
     * Get resource usage aggregated by thread category
     * GET /diagnostics/threads/by-category
     */
    @Get("/by-category")
    @Produces(MediaType.APPLICATION_JSON)
    public Map<String, Object> getResourcesByCategory() {
        Map<String, Object> response = new HashMap<>();

        Map<String, ThreadMonitor.CategoryResourceInfo> categoryStats =
            threadMonitor.getResourcesByCategory();

        List<Map<String, Object>> categories = new ArrayList<>();
        long totalCpuTime = 0;
        long totalMemory = 0;

        for (ThreadMonitor.CategoryResourceInfo stats : categoryStats.values()) {
            Map<String, Object> categoryData = new HashMap<>();
            categoryData.put("category", stats.getCategory());
            categoryData.put("threadCount", stats.getThreadCount());
            categoryData.put("totalCpuTimeMs", stats.getTotalCpuTimeNanos() / 1_000_000);
            categoryData.put("totalAllocatedBytes", stats.getTotalAllocatedBytes());
            categoryData.put("totalAllocatedMB", String.format("%.2f",
                stats.getTotalAllocatedBytes() / (1024.0 * 1024.0)));

            totalCpuTime += stats.getTotalCpuTimeNanos();
            totalMemory += stats.getTotalAllocatedBytes();

            categories.add(categoryData);
        }

        // Sort by memory consumption
        categories.sort((a, b) ->
            Long.compare((Long)b.get("totalAllocatedBytes"), (Long)a.get("totalAllocatedBytes")));

        response.put("categories", categories);
        response.put("totalCpuTimeMs", totalCpuTime / 1_000_000);
        response.put("totalAllocatedMB", String.format("%.2f", totalMemory / (1024.0 * 1024.0)));

        return response;
    }

    /**
     * Helper class for thread CPU information
     */
    private static class ThreadCpuInfo {
        final long threadId;
        final String threadName;
        final Thread.State state;
        final long cpuTime;
        final long userTime;
        final long blockedCount;
        final long waitedCount;

        ThreadCpuInfo(long threadId, String threadName, Thread.State state,
                     long cpuTime, long userTime, long blockedCount, long waitedCount) {
            this.threadId = threadId;
            this.threadName = threadName;
            this.state = state;
            this.cpuTime = cpuTime;
            this.userTime = userTime;
            this.blockedCount = blockedCount;
            this.waitedCount = waitedCount;
        }

        long getCpuTime() {
            return cpuTime;
        }
    }
}
