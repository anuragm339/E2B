package com.messaging.broker.monitoring;

import io.micronaut.context.annotation.Context;

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;

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
        public final String traceId;         // from MDC; ties related entries into one trace chain
        public final String topic;           // from MDC (may be null)
        public final String group;           // from MDC (may be null)
        public final String clientId;        // from MDC (may be null)

        Entry(long ts, String level, String logger, String message,
              String errorCode, String exceptionClass, Map<String, Object> context,
              String traceId, String topic, String group, String clientId) {
            this.ts = ts;
            this.level = level;
            this.logger = logger;
            this.message = message;
            this.errorCode = errorCode;
            this.exceptionClass = exceptionClass;
            this.context = context;
            this.traceId = traceId;
            this.topic = topic;
            this.group = group;
            this.clientId = clientId;
        }
    }

    /** ERROR ranks above WARN; anything else is 0. Used for min-level filtering. */
    private static int rank(String level) {
        if (level == null) return 0;
        String l = level.toUpperCase();
        if (l.equals("ERROR")) return 2;
        if (l.equals("WARN")) return 1;
        return 0;
    }

    private static final java.util.regex.Pattern LEADING_CODE =
            java.util.regex.Pattern.compile("^\\[([A-Z][A-Z0-9_]{2,})\\]");

    /**
     * The "unique error" identity used to collapse repeats into one incrementing count. Grouping by
     * exception CLASS splits logically-identical failures (e.g. the same "Failed to connect" logged
     * as UnknownHostException vs NoRouteToHostException, or one REGISTRY_TOPOLOGY_FETCH_FAILED
     * surfacing as ReadTimeoutException vs HttpClientException). So we key by, in order:
     * the {@code ErrorCode}; else a {@code [ERROR_CODE]} prefix embedded in the message; else the
     * normalized message (timestamps/UUIDs/numbers masked so per-occurrence values don't fragment
     * the bucket); else the exception class; else the level.
     */
    private static String signature(Entry e) {
        if (e.errorCode != null) return e.errorCode;
        if (e.message != null && !e.message.isEmpty()) {
            java.util.regex.Matcher m = LEADING_CODE.matcher(e.message);
            if (m.find()) return m.group(1);
            String norm = normalizeMessage(e.message);
            if (!norm.isEmpty()) return norm;
        }
        return e.exceptionClass != null ? e.exceptionClass : e.level;
    }

    /** Mask per-occurrence values (ISO timestamps, UUIDs, number runs) so repeats collapse. */
    private static String normalizeMessage(String msg) {
        String s = msg
                .replaceAll("\\d{4}-\\d{2}-\\d{2}T[0-9:.]+Z?", "<ts>")
                .replaceAll("\\b[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}\\b", "<uuid>")
                .replaceAll("\\d+", "<n>")
                .replaceAll("\\s+", " ")
                .trim();
        return s.length() > 160 ? s.substring(0, 160) : s;
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
                       String errorCode, String exceptionClass, Map<String, Object> context,
                       String traceId, String topic, String group, String clientId) {
        Entry e = new Entry(ts, level, logger, message, errorCode, exceptionClass, context,
                traceId, topic, group, clientId);
        synchronized (lock) {
            if (ring.size() >= MAX_ENTRIES) {
                ring.removeFirst();
            }
            ring.addLast(e);
        }
        // Tally under the same collapsing identity used by topUnique (see signature()).
        String key = signature(e);
        CodeStat s = byCode.computeIfAbsent(key, k -> {
            CodeStat cs = new CodeStat();
            cs.firstSeen = ts;
            return cs;
        });
        s.count.incrementAndGet();
        s.lastSeen = ts;
    }

    /**
     * Most-recent-first snapshot, filtered. {@code minLevel} is a MINIMUM level (e.g. "ERROR"
     * returns only ERROR, "WARN" returns WARN+ERROR); null means no level filter. Other null
     * filter args are ignored.
     */
    public List<Entry> recent(String minLevel, String code, String logger, long sinceMs, int limit) {
        List<Entry> snapshot;
        synchronized (lock) {
            snapshot = new ArrayList<>(ring);
        }
        List<Entry> out = new ArrayList<>();
        int minRank = minLevel == null ? 0 : rank(minLevel);
        for (int i = snapshot.size() - 1; i >= 0 && out.size() < limit; i--) {
            Entry e = snapshot.get(i);
            if (rank(e.level) < minRank) {
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

    /**
     * Top unique errors at or above {@code minLevel}, computed from the ring, highest count first.
     * Each row carries count, first/last seen, a sample message, and a sample traceId (the most
     * recent occurrence that had one) so the caller can pull the full chain via {@link #trace}.
     */
    public List<Map<String, Object>> topUnique(String minLevel, int limit) {
        List<Entry> snapshot;
        synchronized (lock) {
            snapshot = new ArrayList<>(ring);
        }
        int minRank = minLevel == null ? 0 : rank(minLevel);
        Map<String, long[]> agg = new LinkedHashMap<>();     // sig -> [count, firstSeen, lastSeen]
        Map<String, Entry> sample = new HashMap<>();          // sig -> representative entry
        for (Entry e : snapshot) {                            // ring is oldest -> newest
            if (rank(e.level) < minRank) continue;
            String sig = signature(e);
            long[] a = agg.computeIfAbsent(sig, k -> new long[]{0L, e.ts, e.ts});
            a[0]++;
            if (e.ts < a[1]) a[1] = e.ts;
            if (e.ts > a[2]) a[2] = e.ts;
            // Prefer the latest entry that actually carries a traceId.
            Entry prev = sample.get(sig);
            if (prev == null || e.traceId != null) sample.put(sig, e);
        }
        return agg.entrySet().stream()
                .sorted((x, y) -> Long.compare(y.getValue()[0], x.getValue()[0]))
                .limit(Math.max(1, limit))
                .map(en -> {
                    Entry s = sample.get(en.getKey());
                    Map<String, Object> m = new LinkedHashMap<>();
                    m.put("what", ErrorExplainer.what(s.errorCode, s.exceptionClass, s.message));
                    m.put("how", ErrorExplainer.how(s.logger, s.exceptionClass));
                    m.put("why", ErrorExplainer.why(s.errorCode, s.exceptionClass, s.message));
                    m.put("count", en.getValue()[0]);
                    m.put("level", s.level);
                    m.put("firstSeen", en.getValue()[1]);
                    m.put("lastSeen", en.getValue()[2]);
                    m.put("sampleTraceId", s.traceId);
                    m.put("error", en.getKey());
                    return m;
                })
                .collect(Collectors.toList());
    }

    /**
     * All captured entries for one {@code traceId} at or above {@code minLevel}, in chronological
     * order (start -> end). The ring is append-ordered, so iterating it forward is already
     * oldest-first.
     */
    public List<Entry> trace(String traceId, String minLevel) {
        if (traceId == null || traceId.isEmpty()) return new ArrayList<>();
        List<Entry> snapshot;
        synchronized (lock) {
            snapshot = new ArrayList<>(ring);
        }
        int minRank = minLevel == null ? 0 : rank(minLevel);
        List<Entry> out = new ArrayList<>();
        for (Entry e : snapshot) {
            if (traceId.equals(e.traceId) && rank(e.level) >= minRank) {
                out.add(e);
            }
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
