package com.messaging.broker.monitoring;

import io.micronaut.context.annotation.Context;

import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * Bounded registry of the most recent {@value #MAX} FAILED message records for the self-status API:
 * which record (topic / offset / key) failed, why, how many attempts so far, and its disposition.
 *
 * <p>Unlike the error log mirror ({@link ErrorRecorder}), this is a structured per-record view:
 * entries are keyed by {@code topic#offset}, so repeated failures of the SAME record increment one
 * entry's attempt count (surfacing poison messages) instead of flooding the list. Fixed-size with
 * LRU eviction — it never grows, so it's safe on a small heap.
 *
 * <p>Eager ({@link Context}) static bridge so failure call-sites that aren't easily DI-injected
 * (e.g. an ack-timeout lambda) can reach it via {@link #instance()} without constructor churn.
 */
@Context
public class FailedMessageRecorder {

    public static final int MAX = 20;
    /** At/above this many attempts a record is treated as a stuck "poison" message. */
    private static final int STUCK_ATTEMPTS = 10;

    private static volatile FailedMessageRecorder instance;

    public FailedMessageRecorder() {
        instance = this;
    }

    public static FailedMessageRecorder instance() {
        return instance;
    }

    /** One failed record. Mutable count/reason/timestamp; identity (topic/offset/key) is fixed. */
    public static final class Failure {
        public final String topic;
        public final long offset;
        public final String key;     // null when the failure is batch-level (no single key)
        public final String group;   // null for non-delivery failures
        public volatile String reason;
        public volatile int attempts;
        public final long firstTs;
        public volatile long lastTs;

        Failure(String topic, long offset, String key, String group, String reason, long ts) {
            this.topic = topic;
            this.offset = offset;
            this.key = key;
            this.group = group;
            this.reason = reason;
            this.attempts = 1;
            this.firstTs = ts;
            this.lastTs = ts;
        }

        /** RETRYING while attempts are low; STUCK once it crosses the poison threshold. */
        public String disposition() {
            return attempts >= STUCK_ATTEMPTS ? "STUCK" : "RETRYING";
        }
    }

    private final Object lock = new Object();
    // accessOrder=true → iteration is LRU→MRU; removeEldestEntry caps the size at MAX.
    private final LinkedHashMap<String, Failure> failures =
            new LinkedHashMap<>(32, 0.75f, true) {
                @Override
                protected boolean removeEldestEntry(Map.Entry<String, Failure> eldest) {
                    return size() > MAX;
                }
            };

    /**
     * Record a failed message. Repeated failures of the same {@code topic#offset} increment that
     * entry's attempt count (and refresh it to most-recently-used) rather than adding a new row.
     */
    public void record(String topic, long offset, String key, String group, String reason) {
        long now = System.currentTimeMillis();
        String id = topic + "#" + offset;
        synchronized (lock) {
            Failure f = failures.get(id);   // get() marks MRU under accessOrder
            if (f == null) {
                failures.put(id, new Failure(topic, offset, key, group, reason, now));
            } else {
                f.attempts++;
                f.reason = reason;
                f.lastTs = now;
            }
        }
    }

    /** Newest-first snapshot of the current failed records. */
    public List<Failure> recent() {
        synchronized (lock) {
            List<Failure> out = new ArrayList<>(failures.values());
            Collections.reverse(out);   // LRU→MRU becomes newest-first
            return out;
        }
    }

    public int size() {
        synchronized (lock) {
            return failures.size();
        }
    }
}
