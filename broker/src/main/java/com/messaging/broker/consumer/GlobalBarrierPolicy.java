package com.messaging.broker.consumer;

import io.micronaut.context.annotation.Requires;
import jakarta.inject.Singleton;

import java.util.Map;

/**
 * Default readiness policy: a single node-wide completion barrier. A topic that has reached its
 * settled READY point is held until EVERY in-flight refresh has reached its settled point; only then
 * do all topics go live together, and {@code /health} is reported node-wide. This gives a consistent
 * cross-topic view (no topic serves fresh data while another is still backfilling its history).
 *
 * <p>Selected when {@code broker.refresh.readiness-policy} is {@code global} or unset.
 */
@Singleton
@Requires(property = "broker.refresh.readiness-policy", value = "global", defaultValue = "global")
public class GlobalBarrierPolicy implements RefreshReadinessPolicy {

    @Override
    public boolean canGoLive(String topic, Map<String, RefreshContext> activeRefreshes) {
        // Barrier open only when every active refresh has received all its READY acks (settled).
        for (RefreshContext context : activeRefreshes.values()) {
            if (!context.allReadyAcksReceived()) {
                return false;
            }
        }
        return true;
    }

    @Override
    public HealthScope healthScope() {
        return HealthScope.NODE_WIDE;
    }
}
