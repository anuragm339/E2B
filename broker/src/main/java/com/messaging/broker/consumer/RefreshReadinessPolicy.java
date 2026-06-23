package com.messaging.broker.consumer;

import java.util.Map;

/**
 * Policy that decides, during a multi-topic bootstrap/refresh, (a) when a topic that has reached its
 * settled READY point may resume LIVE (post-settled) delivery, and (b) how health is scoped.
 *
 * <p>This is the seam that keeps the "global completion barrier" swappable: today
 * {@link GlobalBarrierPolicy} holds every topic until ALL are settled and reports node-wide health;
 * a future per-topic / per-consumer model is a different bean selected by
 * {@code broker.refresh.readiness-policy}, not a rewrite. The delivery path and
 * {@code RefreshHealthIndicator} ask the policy — they never hard-code "global barrier".
 */
public interface RefreshReadinessPolicy {

    /** Granularity at which refresh health is aggregated/reported. */
    enum HealthScope { NODE_WIDE, PER_TOPIC, PER_CONSUMER }

    /**
     * May {@code topic} (which has reached its own settled point) resume live delivery / be completed,
     * given the current set of in-flight refreshes? For the global barrier this is true only when every
     * active refresh has reached its settled point — so no topic goes live while another is still
     * catching up (a consistent cross-topic view).
     */
    boolean canGoLive(String topic, Map<String, RefreshContext> activeRefreshes);

    /** How {@code /health} aggregates refresh state. */
    HealthScope healthScope();
}
