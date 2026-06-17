package com.messaging.broker.consumer;

import jakarta.inject.Singleton;

/**
 * Broker-wide "last successful delivery to a consumer" clock.
 *
 * <p>Updated whenever any consumer (modern or legacy) ACKs a delivered batch — i.e. proof that
 * fresh data actually reached a downstream POS. Read by the refresh READY gate so a refresh is
 * only allowed to complete (and the broker reported healthy/green) when real delivery happened
 * recently, OR the topic legitimately has nothing to deliver. See {@link RefreshReplayService}.
 *
 * <p>Scope is intentionally broker-wide (a single clock for the whole node), per the agreed
 * design — "the broker is actively delivering something" rather than per-topic accounting.
 *
 * <p>Cost: one {@code volatile long} write per ACK and a comparison on the gate path. Negligible.
 */
@Singleton
public class DeliveryFreshnessTracker {

    private volatile long lastSuccessfulDeliveryMs = 0L;

    /** Record that a batch was just successfully delivered (ACKed) to a consumer. */
    public void markDelivered() {
        lastSuccessfulDeliveryMs = System.currentTimeMillis();
    }

    /** Epoch-millis of the last successful delivery, or 0 if none since startup. */
    public long getLastSuccessfulDeliveryMs() {
        return lastSuccessfulDeliveryMs;
    }

    /**
     * @return true if a successful delivery happened within {@code windowMs} of {@code nowMs}.
     *         Never true before the first-ever delivery (timestamp 0).
     */
    public boolean deliveredWithin(long windowMs, long nowMs) {
        long ts = lastSuccessfulDeliveryMs;
        return ts > 0 && (nowMs - ts) <= windowMs;
    }
}
