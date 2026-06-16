package com.messaging.broker.http;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.messaging.broker.consistency.PipeConsistencyReport;
import com.messaging.broker.consistency.PipeConsistencyService;
import com.messaging.broker.consumer.ConsumerRegistry;
import com.messaging.broker.consumer.ConsumerStateService;
import com.messaging.broker.consumer.PendingAckStore;
import com.messaging.broker.consumer.RefreshContext;
import com.messaging.broker.consumer.RefreshCoordinator;
import com.messaging.broker.consumer.RemoteConsumer;
import com.messaging.broker.core.TopologyManager;
import com.messaging.broker.legacy.MergedBatch;
import com.messaging.broker.model.DeliveryKey;
import com.messaging.broker.monitoring.ErrorRecorder;
import com.messaging.broker.monitoring.RefreshHistoryRecorder;
import com.messaging.common.api.StorageEngine;
import com.messaging.common.model.MessageRecord;
import com.messaging.common.model.TopologyResponse;
import com.messaging.pipe.HttpPipeConnector;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.Gauge;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Timer;
import io.micronaut.http.HttpResponse;
import io.micronaut.http.MediaType;
import io.micronaut.http.annotation.Controller;
import io.micronaut.http.annotation.Get;
import io.micronaut.http.annotation.PathVariable;
import io.micronaut.http.annotation.Produces;
import io.micronaut.http.annotation.QueryValue;
import jakarta.inject.Inject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.EnumMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;
import java.util.concurrent.TimeUnit;

/**
 * Per-POS, read-only self-status surface (codebase-book ch.19). Answers "is THIS POS okay?"
 * from in-process broker state — no Prometheus/Grafana dependency.
 *
 * <p>Detect-only: every endpoint is a {@code GET} that reads existing state and never mutates
 * the broker. Built incrementally, group by group; this first cut exposes consistency.
 */
@Controller("/admin/status")
public class StatusController {

    private static final Logger log = LoggerFactory.getLogger(StatusController.class);

    /** A consumer with lag but no delivery attempt within this window is treated as STALLED. */
    private static final long STALE_DELIVERY_MS = 30_000L;

    /** A refresh in progress longer than this with pending acks is flagged stuck in /refresh. */
    private static final long REFRESH_SLA_MS = 120_000L;

    private final PipeConsistencyService consistencyService;
    private final TopologyManager topologyManager;
    private final ConsumerRegistry consumerRegistry;
    private final StorageEngine storage;
    private final PendingAckStore pendingAckStore;
    private final HttpPipeConnector pipeConnector;
    private final MeterRegistry meterRegistry;
    private final ConsumerStateService consumerStateService;
    private final ErrorRecorder errorRecorder;
    private final RefreshCoordinator refreshCoordinator;
    private final RefreshHistoryRecorder refreshHistory;
    private final ObjectMapper objectMapper = new ObjectMapper();

    @Inject
    public StatusController(PipeConsistencyService consistencyService,
                           TopologyManager topologyManager,
                           ConsumerRegistry consumerRegistry,
                           StorageEngine storage,
                           PendingAckStore pendingAckStore,
                           HttpPipeConnector pipeConnector,
                           MeterRegistry meterRegistry,
                           ConsumerStateService consumerStateService,
                           ErrorRecorder errorRecorder,
                           RefreshCoordinator refreshCoordinator,
                           RefreshHistoryRecorder refreshHistory) {
        this.consistencyService = consistencyService;
        this.topologyManager = topologyManager;
        this.consumerRegistry = consumerRegistry;
        this.storage = storage;
        this.pendingAckStore = pendingAckStore;
        this.pipeConnector = pipeConnector;
        this.meterRegistry = meterRegistry;
        this.consumerStateService = consumerStateService;
        this.errorRecorder = errorRecorder;
        this.refreshCoordinator = refreshCoordinator;
        this.refreshHistory = refreshHistory;
    }

    /**
     * GET /admin/status/refresh — the active refresh on this POS (if any), with per-consumer
     * RESET/READY progress (which consumer is blocking) and a stuck/SLA verdict. Reuses
     * {@link RefreshCoordinator}; the control plane (trigger) stays on {@code /admin/refresh-topic}.
     */
    @Get("/refresh")
    @Produces(MediaType.APPLICATION_JSON)
    public HttpResponse<String> refresh() {
        try {
            Map<String, Object> body = new LinkedHashMap<>();
            body.put("nodeId", topologyManager.getNodeId());

            RefreshContext ctx = refreshCoordinator.getCurrentRefreshContext();
            if (ctx == null) {
                body.put("active", false);
                return HttpResponse.ok(objectMapper.writeValueAsString(body));
            }

            long now = System.currentTimeMillis();
            long startMs = ctx.getStartTime() == null ? 0 : ctx.getStartTime().toEpochMilli();
            long ageMs = startMs == 0 ? -1 : now - startMs;
            Set<String> expected = ctx.getExpectedConsumers() == null ? Set.of() : ctx.getExpectedConsumers();
            Set<String> resetAcks = ctx.getReceivedResetAcks() == null ? Set.of() : ctx.getReceivedResetAcks();
            Set<String> readyAcks = ctx.getReceivedReadyAcks() == null ? Set.of() : ctx.getReceivedReadyAcks();

            List<String> pendingReset = new ArrayList<>();
            List<String> pendingReady = new ArrayList<>();
            for (String c : expected) {
                if (!resetAcks.contains(c)) {
                    pendingReset.add(c);
                }
                if (!readyAcks.contains(c)) {
                    pendingReady.add(c);
                }
            }
            boolean stuck = ageMs > REFRESH_SLA_MS && (!pendingReset.isEmpty() || !pendingReady.isEmpty());

            body.put("active", true);
            body.put("topic", ctx.getTopic());
            body.put("refreshId", ctx.getRefreshId());
            body.put("refreshType", ctx.getRefreshType());
            body.put("state", String.valueOf(ctx.getState()));
            body.put("ageMs", ageMs);
            body.put("expectedConsumers", expected.size());
            body.put("resetAcked", resetAcks.size());
            body.put("readyAcked", readyAcks.size());
            body.put("pendingResetConsumers", pendingReset);
            body.put("pendingReadyConsumers", pendingReady);
            body.put("stuck", stuck);
            if (stuck) {
                body.put("blockedBy", pendingReady.isEmpty() ? pendingReset : pendingReady);
            }
            return HttpResponse.ok(objectMapper.writeValueAsString(body));
        } catch (Exception e) {
            log.error("event=status.refresh_failed", e);
            return HttpResponse.serverError("{\"error\":\"" + e.getMessage() + "\"}");
        }
    }

    /**
     * GET /admin/status/refresh/history?limit= — recently-terminated refreshes (most recent
     * first) with outcome + duration, from the in-memory ring. Survives a refresh completing
     * (unlike {@code /admin/refresh-status}, which returns NONE once done).
     */
    @Get("/refresh/history")
    @Produces(MediaType.APPLICATION_JSON)
    public HttpResponse<String> refreshHistory(@QueryValue(defaultValue = "20") int limit) {
        try {
            int capped = Math.max(1, Math.min(limit, 100));
            List<Map<String, Object>> rows = new ArrayList<>();
            for (RefreshHistoryRecorder.Entry e : refreshHistory.recent(capped)) {
                Map<String, Object> m = new LinkedHashMap<>();
                m.put("topic", e.topic);
                m.put("refreshId", e.refreshId);
                m.put("refreshType", e.refreshType);
                m.put("outcome", e.outcome);
                m.put("startedMs", e.startedMs);
                m.put("endedMs", e.endedMs);
                m.put("durationMs", e.durationMs);
                m.put("expectedConsumers", e.expectedConsumers);
                m.put("resetAcked", e.resetAcked);
                m.put("readyAcked", e.readyAcked);
                rows.add(m);
            }
            Map<String, Object> body = new LinkedHashMap<>();
            body.put("nodeId", topologyManager.getNodeId());
            body.put("count", rows.size());
            body.put("history", rows);
            return HttpResponse.ok(objectMapper.writeValueAsString(body));
        } catch (Exception e) {
            log.error("event=status.refresh_history_failed", e);
            return HttpResponse.serverError("{\"error\":\"" + e.getMessage() + "\"}");
        }
    }

    /**
     * GET /admin/status/ack-integrity — cached ACK-reconciliation gaps (missing keys per
     * topic/group) read from the reconciliation gauges. Empty when reconciliation is disabled
     * or has not run yet.
     */
    @Get("/ack-integrity")
    @Produces(MediaType.APPLICATION_JSON)
    public HttpResponse<String> ackIntegrity() {
        try {
            List<Map<String, Object>> gaps = new ArrayList<>();
            for (Gauge g : meterRegistry.find("ack.reconciliation.missing.keys").gauges()) {
                Map<String, Object> m = new LinkedHashMap<>();
                m.put("topic", g.getId().getTag("topic"));
                m.put("group", g.getId().getTag("group"));
                m.put("missingKeys", (long) g.value());
                gaps.add(m);
            }
            Map<String, Object> body = new LinkedHashMap<>();
            body.put("nodeId", topologyManager.getNodeId());
            body.put("trackedSeries", gaps.size());
            body.put("note", gaps.isEmpty()
                    ? "no reconciliation data (disabled or not yet run)" : null);
            body.put("gaps", gaps);
            return HttpResponse.ok(objectMapper.writeValueAsString(body));
        } catch (Exception e) {
            log.error("event=status.ack_integrity_failed", e);
            return HttpResponse.serverError("{\"error\":\"" + e.getMessage() + "\"}");
        }
    }

    /**
     * GET /admin/status/errors?level=&code=&logger=&since=&limit= — recent WARN/ERROR events for
     * THIS POS (most recent first), from the in-memory ring. Replaces {@code docker logs | grep}.
     */
    @Get("/errors")
    @Produces(MediaType.APPLICATION_JSON)
    public HttpResponse<String> errors(@QueryValue(defaultValue = "") String level,
                                       @QueryValue(defaultValue = "") String code,
                                       @QueryValue(defaultValue = "") String logger,
                                       @QueryValue(defaultValue = "0") long since,
                                       @QueryValue(defaultValue = "100") int limit) {
        try {
            int capped = Math.max(1, Math.min(limit, 500));
            List<ErrorRecorder.Entry> entries = errorRecorder.recent(
                    level.isBlank() ? null : level,
                    code.isBlank() ? null : code,
                    logger.isBlank() ? null : logger,
                    since, capped);

            List<Map<String, Object>> rows = new ArrayList<>(entries.size());
            for (ErrorRecorder.Entry e : entries) {
                Map<String, Object> m = new LinkedHashMap<>();
                m.put("ts", e.ts);
                m.put("level", e.level);
                m.put("logger", e.logger);
                m.put("message", e.message);
                m.put("errorCode", e.errorCode);
                m.put("exceptionClass", e.exceptionClass);
                if (e.context != null) {
                    m.put("context", e.context);
                }
                rows.add(m);
            }

            Map<String, Object> body = new LinkedHashMap<>();
            body.put("nodeId", topologyManager.getNodeId());
            body.put("bufferSize", errorRecorder.size());
            body.put("returned", rows.size());
            body.put("errors", rows);
            return HttpResponse.ok(objectMapper.writeValueAsString(body));
        } catch (Exception e) {
            log.error("event=status.errors_failed", e);
            return HttpResponse.serverError("{\"error\":\"" + e.getMessage() + "\"}");
        }
    }

    /**
     * GET /admin/status/errors/summary — tally of recent errors by ErrorCode/exception/level
     * with count + first/last-seen, highest count first.
     */
    @Get("/errors/summary")
    @Produces(MediaType.APPLICATION_JSON)
    public HttpResponse<String> errorsSummary() {
        try {
            Map<String, Object> body = new LinkedHashMap<>();
            body.put("nodeId", topologyManager.getNodeId());
            body.put("bufferSize", errorRecorder.size());
            body.put("byCode", errorRecorder.summary());
            return HttpResponse.ok(objectMapper.writeValueAsString(body));
        } catch (Exception e) {
            log.error("event=status.errors_summary_failed", e);
            return HttpResponse.serverError("{\"error\":\"" + e.getMessage() + "\"}");
        }
    }

    /**
     * GET /admin/status/in-flight — deliveries currently pending an ACK on THIS POS.
     *
     * <p>Legacy in-flight comes from {@link PendingAckStore} (one pending batch per client,
     * deduped); modern in-flight from {@link ConsumerStateService#isInFlight}. Only items that
     * are actually pending are returned — healthy/idle consumers are omitted, so this stays small.
     */
    @Get("/in-flight")
    @Produces(MediaType.APPLICATION_JSON)
    public HttpResponse<String> inFlight() {
        try {
            long now = System.currentTimeMillis();
            List<Map<String, Object>> items = new ArrayList<>();
            Set<String> seenLegacyClients = new HashSet<>();

            for (RemoteConsumer c : consumerRegistry.getAllConsumers()) {
                if (c.isLegacy()) {
                    String clientId = c.getClientId();
                    if (!seenLegacyClients.add(clientId)) {
                        continue; // one pending batch per legacy client, spanning topics
                    }
                    MergedBatch batch = pendingAckStore.getPendingBatch(clientId);
                    if (batch == null) {
                        continue;
                    }
                    long sendTime = pendingAckStore.getSendTime(clientId);
                    Map<String, Object> m = new LinkedHashMap<>();
                    m.put("clientId", clientId);
                    m.put("group", c.getGroup());
                    m.put("legacy", true);
                    m.put("pendingAckAgeMs", sendTime > 0 ? Math.max(0, now - sendTime) : -1);
                    m.put("messageCount", batch.getMessageCount());
                    m.put("topics", batch.getMaxOffsetPerTopic().keySet());
                    items.add(m);
                } else if (consumerStateService.isInFlight(DeliveryKey.of(c.getGroup(), c.getTopic()))) {
                    Map<String, Object> m = new LinkedHashMap<>();
                    m.put("clientId", c.getClientId());
                    m.put("group", c.getGroup());
                    m.put("topic", c.getTopic());
                    m.put("legacy", false);
                    items.add(m);
                }
            }

            Map<String, Object> body = new LinkedHashMap<>();
            body.put("nodeId", topologyManager.getNodeId());
            body.put("inFlightCount", items.size());
            body.put("inFlight", items);
            return HttpResponse.ok(objectMapper.writeValueAsString(body));
        } catch (Exception e) {
            log.error("event=status.inflight_failed", e);
            return HttpResponse.serverError("{\"error\":\"" + e.getMessage() + "\"}");
        }
    }

    /**
     * GET /admin/status/pipe — cloud→broker ingestion status for THIS POS.
     *
     * <p>{@code upstreamCursor} is the single global pipe offset (the poll carries no topic —
     * not comparable to any per-topic head). {@code lastSuccessfulPollAgeMs} is time since the
     * last completed poll, not since the last data record. Fetch metrics come from the
     * Micrometer registry (same source Prometheus scrapes).
     */
    @Get("/pipe")
    @Produces(MediaType.APPLICATION_JSON)
    public HttpResponse<String> pipe() {
        try {
            long now = System.currentTimeMillis();
            TopologyResponse topo = topologyManager.getCurrentTopology();
            long lastPoll = pipeConnector.getLastSuccessfulPollMs();

            Map<String, Object> fetch = new LinkedHashMap<>();
            Timer ft = meterRegistry.find("pipe_fetch_latency_seconds").timer();
            fetch.put("avgMs", ft == null ? 0.0 : round1(ft.mean(TimeUnit.MILLISECONDS)));
            fetch.put("maxMs", ft == null ? 0.0 : round1(ft.max(TimeUnit.MILLISECONDS)));
            fetch.put("count", ft == null ? 0L : ft.count());

            Map<String, Object> body = new LinkedHashMap<>();
            body.put("nodeId", topologyManager.getNodeId());
            body.put("role", topo == null ? null : topo.getRole());
            body.put("parentUrl", topologyManager.getCurrentParentUrl());
            body.put("upstreamCursor", pipeConnector.getCurrentOffset());
            body.put("health", pipeConnector.getHealth().toString());
            body.put("lastSuccessfulPollAgeMs", lastPoll == 0 ? -1 : now - lastPoll);
            body.put("pausedForRefresh", pipeConnector.isPaused());
            body.put("pollIntervalMs", pipeConnector.getPollIntervalMs());
            body.put("fetchLatency", fetch);
            body.put("receivedTotal", (long) counterValue("pipe_messages_received_total"));
            body.put("fetchErrorsTotal", (long) counterValue("pipe_fetch_errors_total"));
            return HttpResponse.ok(objectMapper.writeValueAsString(body));
        } catch (Exception e) {
            log.error("event=status.pipe_failed", e);
            return HttpResponse.serverError("{\"error\":\"" + e.getMessage() + "\"}");
        }
    }

    private double counterValue(String name) {
        Counter c = meterRegistry.find(name).counter();
        return c == null ? 0.0 : c.count();
    }

    /**
     * GET /admin/status/consistency — this POS's pipe-consistency summary + per-topic detail.
     *
     * <p>Preserves the full {@link PipeConsistencyReport.State} enum (never collapses to a
     * consistent/inconsistent/unverifiable rollup), so callers can tell a benign clamp
     * ({@code CONSISTENT_UP_TO}) from "couldn't check" ({@code INCONCLUSIVE}/{@code UNREACHABLE}).
     */
    @Get("/consistency")
    @Produces(MediaType.APPLICATION_JSON)
    public HttpResponse<String> consistency() {
        try {
            Map<String, PipeConsistencyReport> reports = consistencyService.latestReports();

            // Count every state; "verified" = we reached a definitive verdict (consistent/
            // consistent-up-to/inconsistent). The rest are checks we could not decide.
            EnumMap<PipeConsistencyReport.State, Integer> byState =
                    new EnumMap<>(PipeConsistencyReport.State.class);
            for (PipeConsistencyReport.State s : PipeConsistencyReport.State.values()) {
                byState.put(s, 0);
            }
            int total = 0;
            int verified = 0;
            int inconsistent = 0;
            long newestCheckMs = 0;
            for (PipeConsistencyReport r : reports.values()) {
                if (r == null || r.state == null) {
                    continue;
                }
                total++;
                byState.merge(r.state, 1, Integer::sum);
                switch (r.state) {
                    case CONSISTENT:
                    case CONSISTENT_UP_TO:
                        verified++;
                        break;
                    case INCONSISTENT:
                        verified++;
                        inconsistent++;
                        break;
                    default:
                        // INCONCLUSIVE / UNREACHABLE / UNSUPPORTED_PARENT / ERROR — not verified
                        break;
                }
                newestCheckMs = Math.max(newestCheckMs, r.checkedAtMs);
            }

            Map<String, Object> summary = new LinkedHashMap<>();
            summary.put("total", total);
            summary.put("byState", byState);
            summary.put("verified", verified);
            summary.put("verifiedPct", total == 0 ? 0.0 : round1(verified * 100.0 / total));
            summary.put("inconsistent", inconsistent);
            summary.put("lastAuditAgeMs",
                    (total == 0 || newestCheckMs == 0) ? -1 : System.currentTimeMillis() - newestCheckMs);

            Map<String, Object> body = new LinkedHashMap<>();
            body.put("nodeId", topologyManager.getNodeId());
            body.put("enabled", consistencyService.isEnabled());
            body.put("running", consistencyService.isRunning());
            body.put("summary", summary);
            body.put("perTopic", reports);

            return HttpResponse.ok(objectMapper.writeValueAsString(body));
        } catch (Exception e) {
            // Controller boundary: surface as an HTTP 500, not a thrown exception.
            log.error("event=status.consistency_failed", e);
            return HttpResponse.serverError("{\"error\":\"" + e.getMessage() + "\"}");
        }
    }

    /**
     * GET /admin/status/consumers — per-consumer health for THIS POS.
     *
     * <p>{@code lag} is computed directly (storage head − delivered offset), so it is correct for
     * both legacy and modern consumers — unlike the consumer-lag metric, which only the modern
     * delivery path updates. {@code state} is derived from lag + {@code lastDeliveryAttempt} (set
     * on both paths) + failure counters; legacy in-flight comes from {@link PendingAckStore}.
     * {@code DISCONNECTED} is intentionally absent — the registry holds only connected consumers.
     */
    @Get("/consumers")
    @Produces(MediaType.APPLICATION_JSON)
    public HttpResponse<String> consumers() {
        try {
            long now = System.currentTimeMillis();
            List<RemoteConsumer> all = consumerRegistry.getAllConsumers();

            List<Map<String, Object>> rows = new ArrayList<>(all.size());
            Map<String, Integer> byState = new LinkedHashMap<>();
            long totalLag = 0;
            Map<String, Object> worst = null;
            long worstLag = -1;

            for (RemoteConsumer c : all) {
                String topic = c.getTopic();
                long head = storage.getCurrentOffset(topic, 0);
                long current = c.getCurrentOffset();
                long committed = consumerRegistry.getCommittedOffset(c.getGroup() + ":" + topic);
                long lag = Math.max(0, head - current);
                long lastAttempt = c.lastDeliveryAttempt;
                long lastDeliveryAgeMs = lastAttempt == 0 ? -1 : now - lastAttempt;
                int failures = c.getConsecutiveFailures();
                boolean legacy = c.isLegacy();

                boolean inFlight = false;
                long pendingAckAgeMs = -1;
                if (legacy && pendingAckStore.getPendingBatch(c.getClientId()) != null) {
                    inFlight = true;
                    long sendTime = pendingAckStore.getSendTime(c.getClientId());
                    pendingAckAgeMs = sendTime > 0 ? Math.max(0, now - sendTime) : -1;
                }

                String state;
                String reason;
                if (head == 0 && current == 0) {
                    state = "NO_DATA_YET";
                    reason = "no records stored for this topic yet";
                } else if (lag == 0) {
                    state = "IDLE";
                    reason = "caught up to head";
                } else if (lastAttempt > 0 && lastDeliveryAgeMs < STALE_DELIVERY_MS) {
                    state = "RECEIVING";
                    reason = "catching up, lag=" + lag;
                } else {
                    state = "STALLED";
                    reason = "lag=" + lag + ", no delivery attempt in "
                            + (lastDeliveryAgeMs < 0 ? "ever" : (lastDeliveryAgeMs / 1000) + "s");
                }
                // Sharpen the reason where a concrete cause is known.
                if (failures > 0) {
                    reason = "delivery failing: " + failures + " consecutive failures; lag=" + lag;
                } else if (inFlight && pendingAckAgeMs >= 0) {
                    reason = "awaiting ACK for " + (pendingAckAgeMs / 1000) + "s; lag=" + lag;
                }

                Map<String, Object> row = new LinkedHashMap<>();
                row.put("clientId", c.getClientId());
                row.put("group", c.getGroup());
                row.put("topic", topic);
                row.put("legacy", legacy);
                row.put("currentOffset", current);
                row.put("committedOffset", committed);
                row.put("headOffset", head);
                row.put("lag", lag);
                row.put("lastDeliveryAgeMs", lastDeliveryAgeMs);
                row.put("consecutiveFailures", failures);
                if (legacy) {
                    row.put("inFlight", inFlight);
                    row.put("pendingAckAgeMs", pendingAckAgeMs);
                }
                row.put("state", state);
                row.put("reason", reason);
                rows.add(row);

                byState.merge(state, 1, Integer::sum);
                totalLag += lag;
                if (lag > worstLag) {
                    worstLag = lag;
                    worst = row;
                }
            }

            Map<String, Object> summary = new LinkedHashMap<>();
            summary.put("consumerCount", rows.size());
            summary.put("byState", byState);
            summary.put("totalLag", totalLag);
            if (worst != null) {
                Map<String, Object> w = new LinkedHashMap<>();
                w.put("clientId", worst.get("clientId"));
                w.put("group", worst.get("group"));
                w.put("topic", worst.get("topic"));
                w.put("lag", worst.get("lag"));
                w.put("state", worst.get("state"));
                w.put("reason", worst.get("reason"));
                summary.put("worstByLag", w);
            }

            Map<String, Object> body = new LinkedHashMap<>();
            body.put("nodeId", topologyManager.getNodeId());
            body.put("summary", summary);
            body.put("consumers", rows);
            return HttpResponse.ok(objectMapper.writeValueAsString(body));
        } catch (Exception e) {
            log.error("event=status.consumers_failed", e);
            return HttpResponse.serverError("{\"error\":\"" + e.getMessage() + "\"}");
        }
    }

    /**
     * GET /admin/status/topics — every topic on this POS with its offsets.
     *
     * <p>Reports all three storage offsets so each is unambiguous: {@code headOffset} is the
     * in-memory write head, {@code durableMaxOffset} the metadata-DB max (can lag the head),
     * {@code earliestOffset} the oldest retained (the natural {@code peek} start after compaction).
     * No record count — that would require a scan and is sparse after compaction.
     */
    @Get("/topics")
    @Produces(MediaType.APPLICATION_JSON)
    public HttpResponse<String> topics() {
        try {
            List<Map<String, Object>> rows = new ArrayList<>();
            for (String t : new TreeSet<>(storage.getTopicNames())) {
                Map<String, Object> m = new LinkedHashMap<>();
                m.put("topic", t);
                m.put("headOffset", storage.getCurrentOffset(t, 0));
                m.put("durableMaxOffset", storage.getMaxOffsetFromMetadata(t, 0));
                m.put("earliestOffset", storage.getEarliestOffset(t, 0));
                rows.add(m);
            }
            Map<String, Object> body = new LinkedHashMap<>();
            body.put("nodeId", topologyManager.getNodeId());
            body.put("topicCount", rows.size());
            body.put("topics", rows);
            return HttpResponse.ok(objectMapper.writeValueAsString(body));
        } catch (Exception e) {
            log.error("event=status.topics_failed", e);
            return HttpResponse.serverError("{\"error\":\"" + e.getMessage() + "\"}");
        }
    }

    /**
     * GET /admin/status/topics/{topic}/peek?fromOffset=&limit= — forward peek of record
     * metadata (NOT payloads). {@code fromOffset} defaults to the earliest retained offset;
     * {@code limit} is capped at 100. Storage reads are forward-only, so this is a forward
     * window from an offset, not a reverse tail.
     */
    @Get("/topics/{topic}/peek")
    @Produces(MediaType.APPLICATION_JSON)
    public HttpResponse<String> peek(@PathVariable String topic,
                                     @QueryValue(defaultValue = "-1") long fromOffset,
                                     @QueryValue(defaultValue = "20") int limit) {
        try {
            long from = fromOffset >= 0 ? fromOffset : storage.getEarliestOffset(topic, 0);
            int capped = Math.max(1, Math.min(limit, 100));
            List<MessageRecord> recs = storage.read(topic, 0, from, capped);
            List<Map<String, Object>> out = new ArrayList<>(recs.size());
            for (MessageRecord r : recs) {
                Map<String, Object> m = new LinkedHashMap<>();
                m.put("offset", r.getOffset());
                m.put("key", r.getMsgKey());
                m.put("eventType", String.valueOf(r.getEventType()));
                m.put("dataSize", r.getData() == null ? 0 : r.getData().length());
                m.put("createdAt", r.getCreatedAt() == null ? null : r.getCreatedAt().toString());
                out.add(m);
            }
            Map<String, Object> body = new LinkedHashMap<>();
            body.put("topic", topic);
            body.put("fromOffset", from);
            body.put("returned", out.size());
            body.put("records", out);
            return HttpResponse.ok(objectMapper.writeValueAsString(body));
        } catch (Exception e) {
            log.error("event=status.peek_failed topic={}", topic, e);
            return HttpResponse.serverError("{\"error\":\"" + e.getMessage() + "\"}");
        }
    }

    /**
     * GET /admin/status/flow?topic=T — the per-topic offset chain on this POS.
     *
     * <p>{@code upstreamCursor} is the single GLOBAL pipe cursor, shown separately and NOT
     * comparable to {@code topicHead} (the poll carries no topic). The true per-topic chain is
     * {@code topicHead → currentOffset → ackedOffset} per consumer.
     */
    @Get("/flow")
    @Produces(MediaType.APPLICATION_JSON)
    public HttpResponse<String> flow(@QueryValue String topic) {
        try {
            long head = storage.getCurrentOffset(topic, 0);
            List<Map<String, Object>> consumers = new ArrayList<>();
            for (RemoteConsumer c : consumerRegistry.getAllConsumers()) {
                if (!c.getTopic().equals(topic)) {
                    continue;
                }
                long current = c.getCurrentOffset();
                Map<String, Object> m = new LinkedHashMap<>();
                m.put("clientId", c.getClientId());
                m.put("group", c.getGroup());
                m.put("legacy", c.isLegacy());
                m.put("currentOffset", current);
                m.put("ackedOffset", consumerRegistry.getCommittedOffset(c.getGroup() + ":" + topic));
                m.put("lag", Math.max(0, head - current));
                consumers.add(m);
            }
            Map<String, Object> body = new LinkedHashMap<>();
            body.put("nodeId", topologyManager.getNodeId());
            body.put("topic", topic);
            body.put("upstreamCursor", pipeConnector.getCurrentOffset());
            body.put("upstreamCursorNote",
                    "global pipe cursor (no topic) — NOT comparable to topicHead");
            body.put("topicHead", head);
            body.put("consumers", consumers);
            return HttpResponse.ok(objectMapper.writeValueAsString(body));
        } catch (Exception e) {
            log.error("event=status.flow_failed topic={}", topic, e);
            return HttpResponse.serverError("{\"error\":\"" + e.getMessage() + "\"}");
        }
    }

    /**
     * GET /admin/status/diagnosis — one computed verdict for THIS POS, combining the other
     * groups: is ingestion healthy, are consumers caught up, is data consistent? Returns
     * {@code status} (HEALTHY / DEGRADED / STALLED) + per-check detail + the top issues.
     */
    @Get("/diagnosis")
    @Produces(MediaType.APPLICATION_JSON)
    public HttpResponse<String> diagnosis() {
        try {
            long now = System.currentTimeMillis();
            List<Map<String, Object>> checks = new ArrayList<>();
            List<String> issues = new ArrayList<>();

            // 1. Pipe ingestion.
            String pipeHealth = pipeConnector.getHealth().toString();
            boolean pipeOk = !"UNHEALTHY".equals(pipeHealth);
            addCheck(checks, "pipe", pipeOk, "health=" + pipeHealth + ", paused=" + pipeConnector.isPaused());
            if (!pipeOk) {
                issues.add("pipe " + pipeHealth);
            }

            // 2. Consumers — same stall logic as /consumers (lag>0 with no recent delivery).
            int stalled = 0;
            int total = 0;
            long maxLag = 0;
            for (RemoteConsumer c : consumerRegistry.getAllConsumers()) {
                total++;
                long head = storage.getCurrentOffset(c.getTopic(), 0);
                long lag = Math.max(0, head - c.getCurrentOffset());
                maxLag = Math.max(maxLag, lag);
                long lastAttempt = c.lastDeliveryAttempt;
                if (lag > 0 && (lastAttempt == 0 || now - lastAttempt >= STALE_DELIVERY_MS)) {
                    stalled++;
                }
            }
            boolean consumersOk = stalled == 0;
            addCheck(checks, "consumers", consumersOk,
                    total + " consumers, " + stalled + " stalled, maxLag=" + maxLag);
            if (!consumersOk) {
                issues.add(stalled + " consumer(s) stalled (maxLag=" + maxLag + ")");
            }

            // 3. Consistency.
            int inconsistent = 0;
            int ctotal = 0;
            for (PipeConsistencyReport r : consistencyService.latestReports().values()) {
                if (r == null || r.state == null) {
                    continue;
                }
                ctotal++;
                if (r.state == PipeConsistencyReport.State.INCONSISTENT) {
                    inconsistent++;
                }
            }
            boolean consistencyOk = inconsistent == 0;
            addCheck(checks, "consistency", consistencyOk,
                    ctotal == 0 ? "no audit yet" : (inconsistent + " inconsistent of " + ctotal));
            if (!consistencyOk) {
                issues.add(inconsistent + " topic(s) inconsistent");
            }

            String status = (!pipeOk || stalled > 0) ? "STALLED"
                    : (inconsistent > 0) ? "DEGRADED" : "HEALTHY";

            Map<String, Object> body = new LinkedHashMap<>();
            body.put("nodeId", topologyManager.getNodeId());
            body.put("status", status);
            body.put("checks", checks);
            body.put("topIssues", issues);
            return HttpResponse.ok(objectMapper.writeValueAsString(body));
        } catch (Exception e) {
            log.error("event=status.diagnosis_failed", e);
            return HttpResponse.serverError("{\"error\":\"" + e.getMessage() + "\"}");
        }
    }

    private static void addCheck(List<Map<String, Object>> checks, String name, boolean ok, String detail) {
        Map<String, Object> m = new LinkedHashMap<>();
        m.put("name", name);
        m.put("ok", ok);
        m.put("detail", detail);
        checks.add(m);
    }

    /**
     * GET /admin/status/performance — storage + pipe latency and the worst consumers by lag.
     *
     * <p>Storage latency timers are <b>broker-global</b>, not per-topic. Timers expose avg/max
     * (always available); windowed rates are a Prometheus concern, not the registry's.
     */
    @Get("/performance")
    @Produces(MediaType.APPLICATION_JSON)
    public HttpResponse<String> performance() {
        try {
            long now = System.currentTimeMillis();

            Map<String, Object> storageM = new LinkedHashMap<>();
            storageM.put("readLatency", timerStats("broker.storage.read.latency"));
            storageM.put("writeLatency", timerStats("broker.storage.write.latency"));
            storageM.put("reads", (long) counterValue("broker.storage.reads"));
            storageM.put("writes", (long) counterValue("broker.storage.writes"));

            Map<String, Object> pipeM = new LinkedHashMap<>();
            pipeM.put("fetchLatency", timerStats("pipe_fetch_latency_seconds"));

            // Worst consumers by lag (computed — correct for legacy; the latency metric is modern-only).
            List<Map<String, Object>> withLag = new ArrayList<>();
            for (RemoteConsumer c : consumerRegistry.getAllConsumers()) {
                long lag = Math.max(0, storage.getCurrentOffset(c.getTopic(), 0) - c.getCurrentOffset());
                if (lag <= 0) {
                    continue;
                }
                Map<String, Object> m = new LinkedHashMap<>();
                m.put("group", c.getGroup());
                m.put("topic", c.getTopic());
                m.put("lag", lag);
                long la = c.lastDeliveryAttempt;
                m.put("lastDeliveryAgeMs", la == 0 ? -1 : now - la);
                withLag.add(m);
            }
            withLag.sort((a, b) -> Long.compare((long) b.get("lag"), (long) a.get("lag")));
            List<Map<String, Object>> worst = new ArrayList<>(
                    withLag.subList(0, Math.min(3, withLag.size())));

            Map<String, Object> body = new LinkedHashMap<>();
            body.put("nodeId", topologyManager.getNodeId());
            body.put("storage", storageM);
            body.put("pipe", pipeM);
            body.put("worstConsumersByLag", worst);
            return HttpResponse.ok(objectMapper.writeValueAsString(body));
        } catch (Exception e) {
            log.error("event=status.performance_failed", e);
            return HttpResponse.serverError("{\"error\":\"" + e.getMessage() + "\"}");
        }
    }

    private Map<String, Object> timerStats(String name) {
        Timer t = meterRegistry.find(name).timer();
        Map<String, Object> m = new LinkedHashMap<>();
        m.put("avgMs", t == null ? 0.0 : round1(t.mean(TimeUnit.MILLISECONDS)));
        m.put("maxMs", t == null ? 0.0 : round1(t.max(TimeUnit.MILLISECONDS)));
        m.put("count", t == null ? 0L : t.count());
        return m;
    }

    private static double round1(double v) {
        return Math.round(v * 10.0) / 10.0;
    }
}
