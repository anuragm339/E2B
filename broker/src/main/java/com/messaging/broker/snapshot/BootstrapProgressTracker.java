package com.messaging.broker.snapshot;

import jakarta.inject.Singleton;

import java.util.LinkedHashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Live progress of an in-flight download-refresh, surfaced by the admin status endpoint.
 *
 * <p>Numerator/denominator are unit-agnostic so one tracker serves both paths:
 * <ul>
 *   <li><b>Snapshot</b>: bytes downloaded / Content-Length — an EXACT byte %.</li>
 *   <li><b>Incremental</b>: sum of per-topic cursor offsets / sum of per-topic head offsets — an
 *       APPROXIMATE offset %, since parent offsets are sparse.</li>
 *   <li><b>Cloud</b>: records ingested with no clean denominator (looping/unbounded) → no %, just a
 *       running count.</li>
 * </ul>
 */
@Singleton
public class BootstrapProgressTracker {

    public enum Phase { IDLE, DOWNLOADING, INGESTING, REFRESHING, DONE, FAILED }

    private volatile Phase phase = Phase.IDLE;
    private volatile String source = "";
    private final AtomicLong numerator = new AtomicLong();
    private volatile long denominator = -1; // -1 = unknown (no %)
    private volatile long startedAtMs = 0;

    /** Begin a new run; resets counters. */
    public void start(String source, Phase phase) {
        this.source = source;
        this.phase = phase;
        this.numerator.set(0);
        this.denominator = -1;
        this.startedAtMs = System.currentTimeMillis();
    }

    public void setPhase(Phase phase) {
        this.phase = phase;
    }

    public void setDenominator(long denominator) {
        this.denominator = denominator;
    }

    public void setNumerator(long n) {
        this.numerator.set(n);
    }

    public void addNumerator(long delta) {
        this.numerator.addAndGet(delta);
    }

    public void done() {
        this.phase = Phase.DONE;
    }

    public void failed() {
        this.phase = Phase.FAILED;
    }

    public Phase getPhase() {
        return phase;
    }

    /**
     * True while the node is actively wiping + re-sourcing its data (the DOWNLOADING/INGESTING
     * phases) — the destructive window where there is no per-topic {@code RefreshContext} yet and the
     * node has no serveable data. {@code /health} reports DOWN unconditionally during this window
     * (node-wide), independent of {@code health-critical-topics}. The subsequent REFRESHING phase is
     * deliberately excluded: by then data is restored and per-topic {@code RefreshContext}s drive
     * health with the normal {@code health-critical-topics} scoping.
     */
    public boolean isReSourcing() {
        Phase p = phase;
        return p == Phase.DOWNLOADING || p == Phase.INGESTING;
    }

    /** Percent complete in [0,100], or -1 when the denominator is unknown. */
    public double percent() {
        long d = denominator;
        if (d <= 0) {
            return -1.0;
        }
        return Math.min(100.0, numerator.get() * 100.0 / d);
    }

    /** Snapshot of the current progress for the status endpoint. */
    public Map<String, Object> snapshot() {
        Map<String, Object> m = new LinkedHashMap<>();
        m.put("phase", phase.toString());
        m.put("source", source);
        m.put("transferred", numerator.get());
        m.put("total", denominator);
        double pct = percent();
        m.put("percent", pct < 0 ? null : Math.round(pct * 10.0) / 10.0);
        m.put("elapsedMs", startedAtMs == 0 ? 0 : System.currentTimeMillis() - startedAtMs);
        return m;
    }
}
