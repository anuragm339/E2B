package com.messaging.broker.consumer;

import java.util.Map;

/**
 * Recovers refresh state after broker restart.
 */
public interface RefreshRecovery {
    /**
     * Initialize recovery process on broker startup.
     * Loads saved state and resumes in-progress refreshes.
     *
     * @return Map of recovered refresh contexts by topic
     */
    Map<String, RefreshContext> recoverAndResumeRefreshes();

    /**
     * Resume a specific refresh from saved state.
     *
     * @param context Refresh context to resume
     */
    void resumeRefresh(RefreshContext context);

    /**
     * Repopulate metrics timing maps with original sent times.
     * Prevents inflated duration metrics when broker restarts mid-refresh.
     *
     * @param topic Topic name
     * @param context Refresh context with timing data
     */
    void repopulateMetricTimings(String topic, RefreshContext context);
}
