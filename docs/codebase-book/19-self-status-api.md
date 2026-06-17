# 19. Roadmap — Broker self-status API (`/admin/status/*`)

> **Status: IMPLEMENTED (2026-06-15).** All endpoints built & verified live in
> `broker/.../http/StatusController.java`: `pipe`, `topics`, `topics/{topic}/peek`,
> `consumers`, `in-flight`, `consistency`, `errors`, `errors/summary`, `performance`, `flow`,
> `diagnosis`, `refresh`, `refresh/history`, `ack-integrity`. Net-new infra:
> `broker/.../monitoring/ErrorRecorder.java` + `StatusErrorAppender.java` (declared in
> `logback.xml`) for the error ring; `RefreshHistoryRecorder.java` for refresh history
> (recorded by `RefreshCoordinator` at its COMPLETED/ABORTED terminal points — two additive,
> exception-safe calls). All fidelity caveats below are honored (legacy-correct computed lag,
> `upstreamCursor` separate, head vs durable-max, forward-only `peek`, full consistency
> `State` enum, modern-only storage latency, per-consumer refresh blocking detail).

## Purpose

A per-POS, read-only HTTP surface that answers "is **this** POS okay?" directly from the
broker's own in-process state — usable when Grafana/Prometheus are unavailable.

## Conventions

- Base path `/admin/status/*`, **read-only GET**, admin HTTP port (8081).
- **Per-POS only** — this broker's own state; no fleet aggregation.
- **Source: in-process** — `MeterRegistry` (Micrometer, the same source Prometheus scrapes),
  `ConsumerRegistry`, `StorageEngine`, the RocksDB ack store, `HttpPipeConnector`,
  `PipeConsistencyService`, `RefreshCoordinator`. **No Prometheus dependency.**
- Failures throw `MessagingException` + `ErrorCode` (see [ch.9](09-error-retry-recovery.md)).
- *Latency caveat:* timers expose **avg / p99 / max** (cumulative or percentile-window),
  not Prometheus `rate(...[5m])`. Rate-per-second is derived by the caller or a short
  rolling stat.
- `reason` / `verdict` fields are **best-effort diagnosis** from real signals (lag, ack-age,
  latency, last-delivery), not ground truth.

## 1. Pipe / ingestion — "is cloud data still coming in?"

| Endpoint | Returns | Source | Status |
|---|---|---|---|
| `GET /admin/status/pipe` | `nodeId, role, parentUrl, upstreamCursor, health, lastSuccessfulPollAgeMs, pausedForRefresh, pollIntervalMs, fetchLatency{count,avgMs,totalMs,recentMaxMs}, receivedTotal, fetchErrorsTotal` | `HttpPipeConnector`, `TopologyManager`, pipe-fetch timer | New |

> **Timer fields:** `count`/`avgMs`/`totalMs` are **cumulative** since startup (`avgMs == totalMs/count`);
> `recentMaxMs` is Micrometer's **decaying rolling-window** max, so a cumulative `avgMs` may read
> higher than `recentMaxMs` — that is expected, not a bug. `fetchLatency.count` is the number of
> poll round-trips recorded (`recordFetchLatency` runs in a `finally`), i.e. it counts **every** poll
> — data, empty, and failed — **not** data downloads; it is non-zero whenever the poll loop runs.

> **Fidelity:** `upstreamCursor` is a **single global** pipe cursor — the poll carries no
> topic parameter (`pipe/.../HttpPipeConnector.java:294`) and the parent endpoint defaults to
> `price-topic` (`pipe/.../PipeServer.java:39`). It is **not** a per-topic offset and **not**
> directly comparable to a topic head. `lastSuccessfulPollAgeMs` is time since the last
> *completed poll*, not since the last data record (`HttpPipeConnector.java:469`).

## 2. Topics / storage — "what's stored, where's a key?"

| Endpoint | Returns | Source | Status |
|---|---|---|---|
| `GET /admin/status/topics` | per topic: `topic, headOffset, segmentCount, lastWriteAgeMs` | `StorageEngine`, segments | New |
| `GET /admin/status/topics/{topic}` | above (no per-topic latency — storage timers are global) | as above | New |
| `GET /admin/status/topics/{topic}/peek?fromOffset=O&limit=N` | up to N records read **forward** from `fromOffset` | `storage.read(...)` | New |
| `GET /admin/status/topics/{topic}/key/{key}` | `{ key, offset, found }` | RocksDB compaction index | New |
| `GET /admin/status/storage?disk=` | all-topics storage roll-up: totals (`topicCount, totalSegments, totalBytes`) + per topic `{headOffset, durableMaxOffset, earliestOffset, durabilityLag, sealedSegmentCount, totalSegments, activeBytes, sealedBytes, totalBytes, largestSegmentBytes, maxSegmentSize, dataDir}` | `SegmentAccess`/`SegmentManager` in-memory accessors | New |
| `GET /admin/status/storage/{topic}?disk=` | the per-topic summary **plus** `segments[]`: `{baseOffset, nextOffset, recordCount, sizeBytes, active, full, logFile}` | `SegmentManager.getAllSegments()` + `Segment` | New |

Cost: head/key O(1); peek bounded; **no full record counts (scan)**. The `storage` views read in-memory
segment state only (zero file IO); pass `disk=true` to additionally sum on-disk segment file sizes
(`diskBytesOnDisk` / per-segment `diskBytes`) — one `stat()` per segment file, opt-in to keep the
default poll IO-free. Tests: `StatusControllerStorageSpec`.

> **Fidelity:** `StorageEngine.read` is **forward-only** (`common/.../StorageEngine.java:32`),
> so a true "last N records" is not directly bounded — and the offset range is sparse after
> compaction. Expose a forward `peek?fromOffset=` rather than a reverse `tail`. Per-topic
> read/write latency is **not available** — the storage latency timers are global
> (`broker/.../monitoring/BrokerMetrics.java:209`); see Performance.
>
> **Fidelity:** `headOffset` must say *which* of three offsets it means
> (`StorageEngine.java`): `getCurrentOffset` (in-memory write head, `:40`),
> `getMaxOffsetFromMetadata` (durable max — can lag the head, `:50`), or `getEarliestOffset`
> (`:60`, the right default `fromOffset` for `peek` after compaction). Pick one and label it.
> Key lookup is backed by `CompactionIndex.getLatestOffsetAndTimestamp(topic, key)` (`:51`).

## 3. Consumers — "is each consumer up / receiving / stuck?"

| Endpoint | Returns | Source | Status |
|---|---|---|---|
| `GET /admin/status/consumers` | per consumer: `clientId, group, topics[], state, currentOffset, headOffset, lag, lastDeliveryAgeMs, lastAckAgeMs, deliveryLatency{avgMs,p99Ms,maxMs}, pendingAcks, reason` | `ConsumerRegistry`, ack store, consumer timers | New |
| `GET /admin/status/consumers/{group}` | group rollup | as above | New |

`state` ∈ `RECEIVING` / `IDLE` (lag=0, healthy) / `NO_DATA_YET` / `STALLED` (lag>0, not
flowing), each with a computed `reason`.

> **Fidelity:** `ConsumerRegistry` holds only **connected** consumers
> (`broker/.../consumer/ConsumerRegistry.java:173`), so `DISCONNECTED` **cannot be derived**
> here — a vanished consumer is simply absent. Reporting `DISCONNECTED` would need a separate
> expected/last-seen set (out of scope for v1).
>
> **Fidelity (legacy vs modern — important):** `lag` and `deliveryLatency` are updated only by
> the **modern** delivery path (`BatchDeliveryService.updateConsumerLag` `:372`,
> `BatchAckService`). The **legacy** path (`LegacyConsumerDeliveryManager`) records only
> `recordConsumerOffsetForceReset` and exposes `broker_legacy_batch_*`
> (`pending_age`, `last_send/ack_time`, `events_total`, `delivery_blocked`). So for
> `LEGACY_MODE` consumers — **what the POS fleet actually runs** — those two fields are
> empty/stale; the state machine and `worst-consumer` ranking must source legacy health from
> `broker_legacy_batch_*` and treat `broker_consumer_*` as modern-only. Don't present one
> unified latency field as if both paths populate it.

## 4. In-flight & ack integrity — "are deliveries pending / are acks complete?"

`RocksDbAckStore` holds **completed** ack records (committed offsets), **not** pending
deliveries (`broker/.../ack/AckStore.java:19`). Pending state lives elsewhere, so this splits
into two endpoints:

| Endpoint | Returns | Source | Status |
|---|---|---|---|
| `GET /admin/status/in-flight` | current pending delivery state: per consumer/topic `pendingCount, oldestPendingAgeMs, deliveredNotAcked` | modern: `ConsumerStateService` (`:53`); legacy batches: `PendingAckStore` (`:11`) | New |
| `GET /admin/status/ack-integrity` | cached reconciliation gaps (missing/duplicate acks) | `AckReconciliationScheduler` (cached results) | New |

Committed offsets (the *completed* side) come from `RocksDbAckStore` and are surfaced in the
consumers group, not here.

## 5. Refresh — "is a refresh stuck, and who's blocking it?"

Control plane already exists (`RefreshController`): `POST /admin/refresh-topic`,
`GET /admin/refresh-status?topic=X`, `GET /admin/refresh-current`. These add the
**observability** the control plane lacks.

| Endpoint | Returns | Source | Status |
|---|---|---|---|
| `GET /admin/status/refresh` | active refresh(es) with **per-consumer RESET/READY progress** (which consumer hasn't acked) + computed **stuck/SLA verdict** | `RefreshCoordinator` | New |
| `GET /admin/status/refresh/history` | recent refreshes ring: `topic, started, ended, durationMs, outcome (COMPLETED/FAILED/TIMED_OUT/RECOVERED), participatedConsumers` | `RefreshCoordinator` + small in-memory ring | New |

Gap filled: existing `/refresh-status` gives aggregate ack *counts* ("3 expected, 2 READY")
but not *which* consumer is missing, and returns `NONE` once done (no history). The
`RefreshCoordinator` already holds the expected-vs-acked set in-process.

## 6. Consistency — "is my data consistent with the parent?"

| Endpoint | Returns | Source | Status |
|---|---|---|---|
| `GET /admin/status/consistency` | per-topic full `state`, plus rollup `verifiedPct, lastAuditAgeMs` | `PipeConsistencyService` / report | New summary, reuses `/admin/pipe-consistency/report` ([ch.16](16-pipe-consistency-system-design.md)) |

> **Fidelity:** preserve the **full** `State` enum — `CONSISTENT`, `CONSISTENT_UP_TO`,
> `INCONSISTENT`, `INCONCLUSIVE`, `UNREACHABLE`, … (`broker/.../consistency/PipeConsistencyReport.java:13`).
> Do **not** collapse to the 3-way `0/1/2` rollup, or callers can't distinguish a benign clamp
> (`CONSISTENT_UP_TO`) from "couldn't check" (`INCONCLUSIVE`/`UNREACHABLE`).

## 7. Errors — "what's gone wrong lately?" (replaces `docker logs | grep ERROR`)

| Endpoint | Returns | Source | Status |
|---|---|---|---|
| `GET /admin/status/errors?level=&code=&logger=&since=&limit=` | recent feed: `ts, level, errorCode, exceptionClass, message, traceId, topic, group, clientId, context` | in-memory ring (logback appender, WARN+ERROR), enriched with `ErrorCode`+context for `MessagingException`, and with MDC `traceId`/topic/group/clientId | New |
| `GET /admin/status/errors/top?level=&limit=` | **top unique errors**, highest count first, each as **`what` / `how` / `why`** (+ `count, level, firstSeen, lastSeen, sampleTraceId, error`) | ring grouped by signature, explained by `ErrorExplainer` | New |
| `GET /admin/status/errors/trace/{traceId}?level=` | one failure's **full chain** for `traceId`, **chronological (start→end)**: `chain[]` of `{ts, level, logger, errorCode, message, …}` | ring filtered by traceId | New |
| `GET /admin/status/errors/summary` | all-time tally: `code, count, firstSeen, lastSeen` (lifetime counters, not level-filtered) | `byCode` map | New |

**What / how / why:** every error row is rendered in plain English by `ErrorExplainer` —
`what` (the human message, `[CODE]` prefix stripped), `how` (`surfaced as <Exception> in <Component>`),
`why` (ErrorCode `category`/`retriable` + a known cause for the exception, e.g. UnknownHostException →
"host name could not be resolved (DNS)"). Example row:

```json
{ "what": "Failed to connect to remote",
  "how":  "surfaced as UnknownHostException in HttpPipeConnector",
  "why":  "host name could not be resolved (DNS); the upstream is unreachable",
  "count": 4, "level": "ERROR", "lastSeen": 1781..., "sampleTraceId": "-" }
```

**Dedup signature:** repeats collapse into one incrementing count by identity — the `ErrorCode`,
else a `[ERROR_CODE]` prefix in the message, else the **normalized message** (timestamps/UUIDs/numbers
masked), else exception class, else level. This deliberately merges the *same* failure logged under
different exception classes (e.g. one "Failed to connect" surfacing as `UnknownHostException` vs
`NoRouteToHostException`, or a `REGISTRY_TOPOLOGY_FETCH_FAILED` arriving as `ReadTimeoutException` vs
`HttpClientException`) instead of fragmenting it; the representative `exceptionClass` is still shown.

**Level model:** `level` is a **minimum** — `ERROR` (the default) returns only errors; `WARN` returns
WARN+ERROR. The default is set by `broker.status.errors.default-level` (default `ERROR`) and overridable
per-call with `?level=WARN`, so the buffer keeps capturing WARN+ but the views stay error-focused
unless asked. Workflow: `/errors/top` to see the worst offenders + grab a `sampleTraceId`, then
`/errors/trace/{traceId}?level=WARN` to read that one failure's whole sequence top-to-bottom.

> **Noise:** `legacy_delivery.empty_batch` (the benign "consumer caught up, nothing to send" state)
> is logged at DEBUG, not WARN — otherwise it floods the ring and buries real errors. Genuine cursor
> failures still log at ERROR.

Cost: bounded ring (~500 entries); **in-memory → resets on restart** (rolling window, not
durable history). `top`/`trace` compute from a ring snapshot on demand (no extra memory).

### Failed messages — "which records failed?" (structured, not log text)

`GET /admin/status/failed-messages` — a bounded registry (last **20**) of failed *data records*,
distinct from the error-log mirror: one row per record with `topic, offset, key, group, reason,
attempts, disposition (RETRYING|STUCK), firstSeen, lastSeen`. Keyed by `topic#offset`, so repeated
failures of the **same** record increment one entry's `attempts` (a climbing count / `STUCK` is the
poison message) rather than flooding the list. Fixed-size LRU — never grows.

| Recorded at | Row |
|---|---|
| Consumer ack-timeout (batch reverted + redelivered) — `BatchDeliveryService` | `topic, offset=batchStart, key=null, group, reason="consumer ack timeout (redelivering)"` |
| Pipe store failure — `BrokerService.handlePipeMessage` | `topic, offset, key, reason="storage write failed"` |

Source: `FailedMessageRecorder` (eager `@Context` static bridge; call-sites record via
`instance()`). Note: at-least-once means a "failed" record is reverted/redelivered, not dropped —
so a `STUCK` row is a record that keeps failing, not a lost one. Consumer-side `handleBatch`
exceptions are logged on the consumer, so here they appear only as the broker's ack-timeout.

## 8. Performance — "storage / read / write / pipe latency + worst consumer"

| Endpoint | Returns | Source | Status |
|---|---|---|---|
| `GET /admin/status/performance` | `storage{read,write}, pipe{fetchLatency,freshness}, worstConsumers[{clientId,latency,reason}], verdict` | timers + consumer state | New |
| `GET /admin/status/performance/storage` | read/write `{count, avgMs, totalMs, recentMaxMs}` + ops — **broker-global, not per-topic** (`BrokerMetrics.java:209`) | storage timers | New |
| `GET /admin/status/performance/pipe` | fetch latency, freshness, health | pipe timer | New |

> **Fidelity:** `worstConsumers` ranks by `deliveryLatency`, which is **modern-path only** (see
> Consumers §3). For `LEGACY_MODE` consumers the ranking must use `broker_legacy_batch_*`
> (pending age / blocked / timeout) instead, or it will silently rank only modern consumers.

## 9. Offset chain — "where along the path is a topic stuck?"

| Endpoint | Returns | Source | Status |
|---|---|---|---|
| `GET /admin/status/flow?topic=T` | `upstreamCursor` (global, shown **separately**) + the true per-topic chain `topicHead → {consumer: currentOffset → ackedOffset}` | pipe + storage + consumers + in-flight | New |

> **Fidelity:** the per-topic chain is only `topicHead → currentOffset → ackedOffset`. The
> pipe `upstreamCursor` is a single global cursor (no topic on the poll —
> `HttpPipeConnector.java:294`, `PipeServer.java:39`), so it is **not** comparable to
> `topicHead`; present it as a separate value, never as `pipeOffset → topicHead`.

## 10. Diagnosis — the computed verdict

| Endpoint | Returns | Source | Status |
|---|---|---|---|
| `GET /admin/status/diagnosis` | `status (HEALTHY/DEGRADED/STALLED), checks[{name,ok,detail}], topIssues[]` | combines groups 1–9 | New |

Checks: pipe progressing? consumers caught up? acks flowing? refresh stuck? consistent?
recent errors? Built last — it consumes all the others.

## Cross-cutting

- All read-only GET, on-demand → no background cost; respects the flat-memory/CPU budget
  ([ch.17](17-performance-tuning.md), `CLAUDE.md`).
- No Prometheus/Grafana dependency — works when monitoring is fully down.
- Container/memory metrics are intentionally **out of scope** for this surface.
- Exact meter/method names confirmed against `BrokerMetrics` / `ConsumerRegistry` /
  `RefreshCoordinator` at build time.
- **Fields needing new accessors** (data exists in-process but isn't exposed yet — build-time
  additions, not blockers): pipe `pollIntervalMs` (private `adaptiveDelay`) and
  `pausedForRefresh` (private flag) on `HttpPipeConnector`; the **per-consumer RESET/READY
  set** for §5 lives inside `RefreshContext` (held in `RefreshCoordinator.activeRefreshes`) and
  needs a read accessor.

## Suggested phasing

1. **Performance + consumers** (state machine + worst-consumer) — most pain.
2. **Refresh** (current + history) — built alongside consumers (same consumer-ack machinery).
3. **Errors** (feed + summary).
4. **Pipe + in-flight/ack-integrity + flow** (the offset chain).
5. **Topics + consistency + diagnosis** (diagnosis last — it consumes the others).
