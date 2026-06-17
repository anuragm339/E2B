package com.messaging.broker.monitoring;

import com.messaging.broker.consumer.RefreshContext;
import io.micronaut.context.annotation.Context;

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.List;

/**
 * Bounded in-memory ring of recently-terminated refreshes for the self-status API
 * (codebase-book ch.19, group 5). Recorded by {@code RefreshCoordinator} at the COMPLETED /
 * ABORTED terminal points (via the static bridge, so no constructor coupling), read by the
 * status controller. In-memory only — resets on restart.
 */
@Context
public class RefreshHistoryRecorder {

    private static final int MAX = 100;

    private static volatile RefreshHistoryRecorder instance;

    public RefreshHistoryRecorder() {
        instance = this;
    }

    public static RefreshHistoryRecorder instance() {
        return instance;
    }

    /** One terminated refresh. Fields are mapped to JSON by the controller. */
    public static final class Entry {
        public final String topic;
        public final String refreshId;
        public final String refreshType;
        public final String outcome;       // COMPLETED / ABORTED
        public final long startedMs;
        public final long endedMs;
        public final long durationMs;
        public final int expectedConsumers;
        public final int resetAcked;
        public final int readyAcked;

        Entry(String topic, String refreshId, String refreshType, String outcome,
              long startedMs, long endedMs, long durationMs,
              int expectedConsumers, int resetAcked, int readyAcked) {
            this.topic = topic;
            this.refreshId = refreshId;
            this.refreshType = refreshType;
            this.outcome = outcome;
            this.startedMs = startedMs;
            this.endedMs = endedMs;
            this.durationMs = durationMs;
            this.expectedConsumers = expectedConsumers;
            this.resetAcked = resetAcked;
            this.readyAcked = readyAcked;
        }
    }

    private final ArrayDeque<Entry> ring = new ArrayDeque<>(MAX);
    private final Object lock = new Object();

    /** Record a terminated refresh. Never throws — recording must not disturb the refresh path. */
    public void record(RefreshContext ctx, String outcome) {
        try {
            long started = ctx.getStartTime() == null ? 0 : ctx.getStartTime().toEpochMilli();
            long ended = System.currentTimeMillis();
            Entry e = new Entry(
                    ctx.getTopic(), ctx.getRefreshId(), ctx.getRefreshType(), outcome,
                    started, ended, started == 0 ? -1 : ended - started,
                    ctx.getExpectedConsumers() == null ? 0 : ctx.getExpectedConsumers().size(),
                    ctx.getReceivedResetAcks() == null ? 0 : ctx.getReceivedResetAcks().size(),
                    ctx.getReceivedReadyAcks() == null ? 0 : ctx.getReceivedReadyAcks().size());
            synchronized (lock) {
                if (ring.size() >= MAX) {
                    ring.removeFirst();
                }
                ring.addLast(e);
            }
        } catch (Exception ignored) {
            // never disturb the refresh state machine
        }
    }

    /** Most-recent-first snapshot. */
    public List<Entry> recent(int limit) {
        List<Entry> snap;
        synchronized (lock) {
            snap = new ArrayList<>(ring);
        }
        List<Entry> out = new ArrayList<>();
        for (int i = snap.size() - 1; i >= 0 && out.size() < limit; i--) {
            out.add(snap.get(i));
        }
        return out;
    }
}
