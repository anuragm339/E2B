package com.messaging.broker.monitoring;

import io.micronaut.context.annotation.Context;

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Bounded in-memory ring of recent WARN/ERROR events for the self-status API
 * (codebase-book ch.19, group 7). Fed by {@link StatusErrorAppender}; read by the status
 * controller. In-memory only — a rolling window, not durable history (resets on restart).
 *
 * <p>Cheap and safe: writes happen only on the (rare) WARN/ERROR path, the ring is capped, and
 * the per-code tally is lock-free. Reads take a short lock to snapshot the ring.
 */
@Context
public class ErrorRecorder {

    private static final int MAX_ENTRIES = 500;

    // Static bridge: the logback appender (declared in logback.xml, instantiated by logback with
    // a no-arg constructor) cannot be DI-injected, so it reads this singleton. Eager (@Context)
    // so the instance exists early; events logged before it is set are dropped (startup noise).
    private static volatile ErrorRecorder instance;

    public ErrorRecorder() {
        instance = this;
    }

    public static ErrorRecorder instance() {
        return instance;
    }

    /** One recorded log event. Fields are read by the controller, which maps them to JSON. */
    public static final class Entry {
        public final long ts;
        public final String level;
        public final String logger;
        public final String message;
        public final String errorCode;       // null unless the throwable was a MessagingException
        public final String exceptionClass;  // null when there was no throwable
        public final Map<String, Object> context; // null unless the MessagingException carried context

        Entry(long ts, String level, String logger, String message,
              String errorCode, String exceptionClass, Map<String, Object> context) {
            this.ts = ts;
            this.level = level;
            this.logger = logger;
            this.message = message;
            this.errorCode = errorCode;
            this.exceptionClass = exceptionClass;
            this.context = context;
        }
    }

    private static final class CodeStat {
        final AtomicLong count = new AtomicLong();
        volatile long firstSeen;
        volatile long lastSeen;
    }

    private final ArrayDeque<Entry> ring = new ArrayDeque<>(MAX_ENTRIES);
    private final Object lock = new Object();
    private final ConcurrentHashMap<String, CodeStat> byCode = new ConcurrentHashMap<>();

    public void record(long ts, String level, String logger, String message,
                       String errorCode, String exceptionClass, Map<String, Object> context) {
        Entry e = new Entry(ts, level, logger, message, errorCode, exceptionClass, context);
        synchronized (lock) {
            if (ring.size() >= MAX_ENTRIES) {
                ring.removeFirst();
            }
            ring.addLast(e);
        }
        // Tally by the most specific identity we have: ErrorCode, else exception class, else level.
        String key = errorCode != null ? errorCode
                : (exceptionClass != null ? exceptionClass : level);
        CodeStat s = byCode.computeIfAbsent(key, k -> {
            CodeStat cs = new CodeStat();
            cs.firstSeen = ts;
            return cs;
        });
        s.count.incrementAndGet();
        s.lastSeen = ts;
    }

    /** Most-recent-first snapshot, filtered. Null filter args are ignored. */
    public List<Entry> recent(String level, String code, String logger, long sinceMs, int limit) {
        List<Entry> snapshot;
        synchronized (lock) {
            snapshot = new ArrayList<>(ring);
        }
        List<Entry> out = new ArrayList<>();
        for (int i = snapshot.size() - 1; i >= 0 && out.size() < limit; i--) {
            Entry e = snapshot.get(i);
            if (level != null && !level.equalsIgnoreCase(e.level)) {
                continue;
            }
            if (code != null && !code.equalsIgnoreCase(e.errorCode)) {
                continue;
            }
            if (logger != null && (e.logger == null || !e.logger.contains(logger))) {
                continue;
            }
            if (sinceMs > 0 && e.ts < sinceMs) {
                continue;
            }
            out.add(e);
        }
        return out;
    }

    /** Tally by ErrorCode/exception/level, highest count first. */
    public Map<String, Object> summary() {
        Map<String, Object> out = new LinkedHashMap<>();
        byCode.entrySet().stream()
                .sorted((a, b) -> Long.compare(b.getValue().count.get(), a.getValue().count.get()))
                .forEach(en -> {
                    Map<String, Object> m = new LinkedHashMap<>();
                    m.put("count", en.getValue().count.get());
                    m.put("firstSeen", en.getValue().firstSeen);
                    m.put("lastSeen", en.getValue().lastSeen);
                    out.put(en.getKey(), m);
                });
        return out;
    }

    public int size() {
        synchronized (lock) {
            return ring.size();
        }
    }
}
