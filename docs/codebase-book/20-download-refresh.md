# Download Refresh

Related: [POS machine state](07-pos-machine-state.md), [Compaction](08-compaction-handling.md), [Pipe consistency](16-pipe-consistency-system-design.md), [Runtime config](11-runtime-config.md), [Test map](10-test-map.md).

Status: implemented on branch `feature/download-refresh` (not yet merged). Validated by unit suites + a context-boot smoke; full multi-broker journey tests are still pending (see [Pending](#pending)).

> **Agreed redesign pending — see [Planned redesign (P1–P6)](#planned-redesign-p1p6).** A reworked flow has been agreed (unified RESET-after-restart lifecycle, network-server stop during the wipe, snapshot pipe-offset resume, topology-first boot ordering, real BARE_METAL exit, refresh topic allow-list). The sections below **"## Source selection" onward describe the current as-built behavior**; the redesign section describes the agreed target and is **not yet implemented**.

## What it is

A second kind of refresh, alongside the existing **LOCAL** refresh (which replays a node's *own* local segments to consumers; see [POS state](07-pos-machine-state.md#refresh-state-machine)). **Download refresh** throws away the node's local copy and re-sources authoritative data from upstream, then runs the normal RESET→replay→READY so consumers receive the fresh copy.

Plain terms: "wipe my data and get a fresh copy from above." A neighbour POS with a ready-made snapshot ZIP → download + unzip (fast). No snapshot → stream every topic from the parent. Talks straight to the cloud, or the parent is busy → pull from the cloud.

## Bootstrap redesign — pipe-only (2026-06, target design)

> **This supersedes the bulkFetch + network-server-stop approach for the FRESH-BOOT path.** That
> approach shipped on `feature/download-refresh` and caused a live regression — the fresh-boot reuse
> of the heavy `runBootstrapAndRefresh` stopped the consumer network server for the whole (slow,
> double-pulling) load → consumer reconnect-thrash / OOM-loop, plus a `pipe_message.duplicate_skipped`
> flood (bulkFetch and the normal pipe both pulled from offset 0). **The admin in-process refresh of a
> populated node KEEPS** the network-server stop (real wipe → real ACK race). Fresh-boot is different
> and must not stop anything.
>
> **Status: IMPLEMENTED.** Snapshot N\* capture + restore-resume; `bulkFetch` removed (STREAM/CLOUD via
> the normal pipe + `resetOffset`); dynamic settled-target (re-evaluated vs the live head with a
> stability guard); fresh-boot no-longer stops the network server + degrades to normal pipe on failure;
> and the global completion barrier via the pluggable `RefreshReadinessPolicy` / `GlobalBarrierPolicy`.
> Fresh-install stays gated `false` in docker-compose pending live re-verification; flip to `true` to
> enable. The STREAM/CLOUD topic-set is resolved from the **configured** `legacy-clients.service-topics`
> (union with storage), so a pipe-stream fresh-boot RESET→replay→READYs the right topics even though
> storage is empty at refresh-start (the dynamic settled-target then waits per topic as the pipe fills).

### Principles
1. **Never `bulkFetch` — always trust the pipe.** The only "download" is the snapshot ZIP (POS→POS).
   STREAM (parent) and CLOUD are the normal continuous pipe poller (`HttpPipeConnector`).
   `bulkFetchFromParent`/`bulkFetchFromCloud` are removed.
2. **Don't stop the broker/network server for fresh-boot** — empty storage, nothing to wipe, no
   consumer-offset race to protect. Stopping it just locks consumers out for the whole load.
3. **The pipe never says "caught up."** It streams forever (normal process). The **broker** decides
   health-green based on what it has **delivered to consumers**, not on pipe ingestion state.

### Source cascade (resolve topology FIRST, before the pipe goes live)
```
parent = reachable POS:
    has snapshot? → download ZIP (segments only) → seed pipe-offset = N* → pipe resumes from N*
                    (download/restore fails → STREAM from parent)
    no snapshot   → STREAM from parent (normal pipe from 0)
no parent / unreachable:
    → STREAM from cloud (normal pipe).   Snapshots exist ONLY on a POS, never the cloud.
```

### Snapshot N\* (P4 / P4b)
The ZIP already contains only per-topic segment folders — it **excludes** `ack-store/`, `topology`,
`consumer-offsets`, `pipe-offset`, `delivery-state`, `data-refresh-state` (all parent-specific; the
child rebuilds them). The only addition is to **convey N\*** — the global pipe cursor at build time:
- **Build:** `SnapshotScheduler` pauses the pipe, captures `topicHeads` + `N* = pipeConnector.getCurrentOffset()` as one consistent cut, stores N\* in `SnapshotManifest.pipeOffset`. *(done)*
- **Restore:** seed `pipe-offset = N*` via `PipeConnector.resetOffset()` (inside the paused-pipe window, after `clearState`), so the pipe resumes **forward** from N\* (only the tail the parent appended after the snapshot) instead of re-streaming the whole history from 0. Legacy snapshot (N\* = -1) → seed 0. *(done)*

### Health — delivery-gated on the X-hour SETTLED target (not the live head)
`/health` is DOWN during the load and goes UP when (a) the **pipe is drained** — the "load finished"
signal: `PipeConnector.isUpstreamDrained()` is true once a poll returns 0 records (the whole backlog is
in storage) — AND (b) consumers have been delivered up to the **X-hour settled target**
(`RefreshReplayWindowResolver.settledTarget`, re-evaluated against the live head). The drained gate is
what stops a fresh boot from completing instantly against still-empty storage (the live-discovered
0-messages-transferred bug); `isUpstreamDrained()` resets on `resetOffset()` so each bootstrap re-stream
starts "not drained."
Records inside the last X hours flow via normal delivery afterwards (they don't gate green). While the
pipe is still pouring in the backlog the head moves and consumers chase it → DOWN; once the backlog is
in and delivery catches up → UP.

### Global completion barrier (atomic across topics) + pluggable readiness policy
A topic that reaches its settled point does **not** go live (deliver new/within-X records) until
**every** refreshed topic is settled. Held records wait in storage (nothing dropped), released
together when the barrier opens; `/health` UP only then. *(Edge case: topic X settled + a new record,
topic Y not settled → X's new record is held until Y settles — a consistent cross-topic view.)*

This barrier is a **swappable policy** so a future per-consumer health model is a bean/config change,
not a rewrite:
```
interface RefreshReadinessPolicy {
    boolean canGoLive(String topic, RefreshSet state);   // may a settled topic deliver live records yet?
    HealthScope healthScope();                            // NODE_WIDE | PER_TOPIC | PER_CONSUMER
}
```
Default `GlobalBarrierPolicy` (node-wide, all-settled). Future `PerTopic`/`PerConsumer` selected by
`broker.refresh.readiness-policy`. The delivery path and `RefreshHealthIndicator` ask the policy; they
never hard-code "global barrier". (Generalises the existing `health-critical-topics` scoping.)

### Failure → normal pipe fallback
The pipe never stops, so it is the always-available baseline. Any bootstrap failure (snapshot or
otherwise) **drops the health gate and continues in normal pipe-transfer mode** — health then goes
green through normal delivery; data keeps flowing. **No stuck-DOWN, no crash-loop.** The bootstrap is
an optimisation of the boot experience, never a hard dependency.

### Boot ordering
```
storage.recover()
resolveTopologyNow(~5s)                 # parent known up front → correct source selection
if fresh (empty + no data-refresh-state + no pipe-offset):
    progress.start(LOADING) → /health DOWN
    [snapshot path] download ZIP → restore → seed pipe-offset = N*
    topologyManager.start()             # pipe goes live (from N*, or 0 for STREAM/CLOUD)
    async: RESET→replay→READY with live-re-evaluated X-hour settled target, governed by the
           readiness policy (global barrier); on completion → /health UP; on failure → drop gate, normal pipe
else:
    topologyManager.start()             # normal boot
```

---

## Planned redesign (P1–P6)

> **Status: the unified lifecycle (P1 + P5) is IMPLEMENTED.** A non-LOCAL refresh now stops the
> consumer network server (`NettyTcpServer.stopAccepting()`) for the wipe + re-source window,
> quiesces the offset/delivery stores so the deleted state files cannot reappear, restarts the server
> (`resumeAccepting()`), and only then runs RESET→replay→READY; `/health` is DOWN node-wide during the
> re-source phase. BARE_METAL drives the `System.exit` hard reset (`BareMetalResetService`). The
> **P3 empty-boot bootstrap** is also implemented: a fresh / post-bare-metal node auto-runs the
> bootstrap-and-refresh on boot (health-gated, RESET→replay→READY) instead of streaming in green.
> Still **planned**: P2 (snapshot-fail cascade + bootstrap-source metric), the full P3 boot **reorder**,
> P4/P4b (snapshot `N*` pipe-offset capture/resume), P6 (refresh topic allow-list).
> Everything from [Source selection](#source-selection) onward is the current as-built behavior.

### Unified refresh lifecycle (corrected order)

Every **non-LOCAL** refresh runs one sequence; `/health` is DOWN for the whole span. The key correction from as-built: **RESET/READY happen *after* the data is re-sourced and the broker is back up** — we do not message consumers while the network server is stopped.

```
 ┌─ progress.start(DOWNLOADING)  →  /health DOWN (re-sourcing, node-wide) ─────────────┐
 1│ pause pipe  +  quiesce ConsumerOffsetTracker / DeliveryStateStore                  │
 2│ stop network server (stopAccepting → consumers disconnect, NO consumer-offset moves)│
 3│ clear data  (wipe topic data + consumer-offsets + delivery-state + pipe-offset)    │
  │             [data-refresh-state still cleared — P5 "preserve across wipe" deferred] │
 4│ re-source:  snapshot restore / stream from 0 / cloud  (HTTP, not the consumer TCP)  │
 5│ start network server (resumeAccepting, finally) → consumers reconnect (5s→60s)      │
  │            resume stores (reload from wiped/restored file) + resume pipe            │
 6│ RESET eligible-topic consumers → await RESET_ACK   (/health DOWN via RefreshContext)│
 7│ replay fresh data (bounded by replay window) → READY → await READY_ACK             │
 └─ COMPLETED → /health UP ───────────────────────────────────────────────────────────┘
```

**LOCAL** is exempt: `RESET → replay local segments → READY`; no pipe pause, no broker stop, no wipe.

**Why stop the network server during the wipe (step 2):** it guarantees `consumer-offsets.properties` cannot advance while we delete it (no `BATCH_ACK` is processed), closing the race where a stray ACK re-creates a stale offset file after `clearState`. Consumers are expected to reconnect on their own and be RESET/READY'd via the `SubscribeHandler` late-join path on return.

> ⚠️ **Known limitation (verified live 2026-06-21): the legacy consumer client does NOT reconnect after a server EOF.** On `stopAccepting()` the consumer logs `📪 EOF received - shutting down` and stops — it never retries. So an **admin `stopServer=true` refresh permanently disconnects every consumer**: each topic's RESET then fires with `consumerCount=0`, the refresh has no one to drive to READY, `awaitRefreshesComplete` hits its 15-min cap (`event=download_refresh.await_refresh_timeout`), and `/health` stays **503** with `data_refresh_messages_transferred=0` until the consumer **containers are manually restarted**. (The data wipe + pipe re-stream + RocksDB in-place clear all succeed — only the consumer handshake is stranded.) The offset-advance race is **independently** closed by `quiesceWipeRecover`'s `ConsumerOffsetTracker.quiesceForWipe()` / `DeliveryStateStore.quiesceForWipe()` (drop in-memory state + reject writes), so the server-stop is not the only protection. The **fresh-install bootstrap deliberately uses `stopServer=false`** and is unaffected. *Decision (for now): left as-is and documented; revisit either by dropping the admin server-stop (rely on store-quiescing) or by making the client auto-reconnect after EOF.*

### Source selection & failure cascade (step 4)

```
no parent (root)     → CLOUD
parent /health DOWN  → CLOUD
parent has snapshot  → SNAPSHOT ──fail──► STREAM from parent ──fail / parent now in-refresh──► CLOUD
else                 → STREAM
```
- **SNAPSHOT success** → restore topic data + **resume the pipe from `N*`** (the snapshot's captured pipe cursor) — fetches only the tail the parent appended after the snapshot, no full re-stream from 0.
- **STREAM** → wipe, `pipe-offset = 0`, resume the **normal** pipe → the node live-follows the parent from 0 (no dedicated bulk-fetch).
- **CLOUD** → cloud sync (jittered if escalated).
- Each outcome recorded: `data_refresh_bootstrap_source_total{requested_type, source, fallback_from, outcome}`.

### Snapshot pipe-offset resume (`N*`)

As-built, `clearState` wipes `pipe-offset.properties` after a snapshot restore, so the pipe re-streams the **entire** history from offset 0 (idempotent but wasteful). Fix:
- **Build side** (`SnapshotScheduler`, serving node): **pause the pipe**, then capture `topicHeads` **and** the global pipe cursor `N*` **atomically**, build the ZIP, resume. `N*` is stored in `SnapshotManifest.pipeOffset`.
- **Restore side**: seed `pipe-offset = N*`; `clearState` no longer wipes `pipe-offset` on the snapshot path. The pipe resumes at `N*`.
- The pipe cursor is a single **global** upstream offset; `topicHeads` are per-topic **storage** offsets — different spaces, so the manifest must carry `N*` explicitly.

### State preserved across the wipe

The wipe clears the RocksDB **ACK + compaction column families** and the `consumer-offsets`, `pipe-offset`, `delivery-state`, **and** `data-refresh-state` state files (the snapshot path runs the same `clearState`). **Preserved:** `topology.properties` only.

> **RocksDB is cleared in place, not by deleting `ack-store/`.** `SharedRocksDb` is a long-lived singleton handle shared by the ACK store and the compaction index; deleting its directory out from under the open handle corrupts it (`RocksDBException: While open a file for appending: NNN.log: No such file or directory`) so the **first pipe record after the wipe fails its compaction-index write and the pipe stalls on that record forever** (looks like a premature drain, but it is *blocked*). The wipe therefore calls `SharedRocksDb.clearCompactionAndAck()` (range tombstones over both CFs) from `quiesceWipeRecover()`, right after `storage.close()`. `LocalStateCleaner.clearState` no longer touches the `ack-store/` directory. See Debugging Guide → "Pipe frozen on one record … `CompactionIndex write failed`".

To keep the deleted `consumer-offsets`/`delivery-state` files from reappearing while `clearState` removes them, the in-memory stores are quiesced for the whole wipe — `DownloadRefreshOrchestrator.quiesceWipeRecover()` calls `ConsumerOffsetTracker.quiesceForWipe()` / `DeliveryStateStore.quiesceForWipe()` (cancel periodic flush, drop in-memory state, reject writes) before the wipe and `resumeAfterWipe()` (reload from the wiped/restored file) after `storage.recover()`. *Planned (P5 follow-up):* preserve `data-refresh-state.properties` across the wipe so a crash mid-wipe is resumed by `RefreshRecoveryService`; today it is cleared like the others.

### Health & metrics

`/health` DOWN = **re-sourcing in progress** (`BootstrapProgressTracker.isReSourcing()` — the DOWNLOADING/INGESTING phases) **OR** an active `RefreshContext` — together they cover steps 1–7 with no UP gap in the wipe window. The re-sourcing gate is **node-wide** (the node has no serveable data); the `RefreshContext` branch keeps the `health-critical-topics` scoping for the later RESET→READY phase (`BootstrapProgressTracker.Phase.REFRESHING` is deliberately excluded from `isReSourcing()` so that scoping still applies). The two created_time gates: [replay start window](#replay-window-and-ready) and the [READY settle gate](#ready-settle-gate-created_time).

### Refresh topic allow-list

```yaml
broker.refresh.topics: []                 # which topics a refresh may target; empty = all storage topics
broker.refresh.health-critical-topics: [] # subset whose refresh blocks /health
```
Refresh targets `storage/snapshot topics ∩ refresh.topics`. An eligible topic with **no connected consumers** re-sources its data and **completes immediately** (so `/health` releases instead of hanging on ACKs that never arrive). Decouples refresh eligibility from runtime consumer presence.

### Startup / BARE_METAL / fresh-install convergence

- **BARE_METAL (admin):** RESET eligible-topic consumers (best-effort) → stop → wipe → `System.exit(70)` → external supervisor restarts → empty node → the empty-boot bootstrap below re-sources it. **Implemented:** `DownloadRefreshService.runBareMetalRefresh()` broadcasts RESET via `refreshTopics(topics, "BARE_METAL")` then calls `BareMetalResetService.reset()`. (Previously BARE_METAL was a no-op alias — `bareMetalReset` was injected but never called.)
- **Fresh install / empty boot (P3, implemented):** on startup `BrokerService` detects a fresh node — `storage.getTopicNames()` empty **and** no `data-refresh-state.properties` (refresh-recovery owns that case) **and** no `pipe-offset.properties` — and, when `broker.bootstrap.fresh-install.enabled` (default true; **false in tests**), runs the **same** bootstrap a bare-metal converges to instead of silently streaming in while `/health` reads green:
  ```
  mark /health DOWN (BootstrapProgressTracker)         # synchronous, no green window
  async:  topologyManager.resolveTopologyNow(~5s)      # bounded wait so a child doesn't escalate to cloud
          downloadRefreshService.runBootstrapAndRefresh()  # re-source → RESET→replay→READY → /health UP
  ```
  It calls `runBootstrapAndRefresh()` (the auto re-source), **not** the `BARE_METAL` type — that one `System.exit`s and would loop on an empty node. `resolveTopologyNow` blocks on a latch counted down after the first registry query.

  **Dashboard label:** the fresh-install bootstrap records its per-topic consumer refreshes under the synthetic type **`FRESH_INSTALL`**, not the raw bootstrap source. A root fresh node re-sources from the cloud (`CLOUD_SYNC`) and a fresh POS from its parent (`PIPE_AND_PROVIDER_*`), but operationally "the node came up empty and bootstrapped itself" is distinct from "an operator triggered a re-source" — which keeps the actual source label. `DownloadRefreshService.runDownloadRefresh` takes a `refreshLabelOverride` (`"FRESH_INSTALL"` for `runBootstrapAndRefresh`, `null` → `result.getSource().name()` for the admin path). The label is non-LOCAL, so the settled/drained replay gate still applies. *(Earlier bug, fixed: `RefreshReadyService.completeRefresh` hardcoded `recordRefreshCompleted(topic, "LOCAL", …)`, so every non-LOCAL refresh split into a started-only row under its real type and a completed-only `LOCAL` row; it now uses `context.getRefreshType()`.)*
  - **Deferred:** the full boot **reorder** (resolve topology → recover → refresh-recovery, all *before* the pipe goes live). The current order keeps `topologyManager.start()` then triggers the bootstrap; the orchestrator's pipe-pause + idempotent ingestion absorb any overlap with the now-live pipe.

### Phases

| Phase | What | Risk | Status |
|---|---|---|---|
| P1 | BARE_METAL → `System.exit` + RESET selected consumers | low — fixes the confirmed no-op bug | ✅ done |
| P5 | unified lifecycle: stop network server during wipe; quiesce offset/delivery stores; RESET/READY after restart; health DOWN during re-source | **high** (core restructure) | ✅ done |
| P2 | snapshot-fail cascade (stream-from-0 → cloud) + bootstrap-source metric | low | planned |
| P3 | empty-boot bootstrap (fresh install / post-bare-metal) + bounded topology resolve | **high** (boot lifecycle) | ✅ done (empty-boot bootstrap + `resolveTopologyNow`); full boot **reorder** (`topology→recover→refresh-recovery` first) deferred |
| P4 | snapshot captures `N*`; restore resumes pipe from it | medium | planned |
| P4b | pause pipe while building the ZIP | low | planned |
| P6 | refresh topic allow-list + consumerless graceful completion + health scoping | low–medium | planned |

> **P5 deviations from the original sketch (resolved during implementation):**
> - *Network server stop is real, not a "reject ACKs" fallback* — `NettyTcpServer` proved cleanly
>   restartable: `start(port)` reassigns fresh event-loop groups, `shutdown()`/`stopAccepting()` keep
>   the registered handler lists, and `SO_REUSEADDR` lets the listen port rebind. Admin + `/health`
>   live on a separate Micronaut HTTP server, so they stay reachable through the bounce.
> - *Stopping the server is necessary but not sufficient* — `consumer-offsets`/`delivery-state` are
>   written by **periodically-flushing** stores (and a decoupled `ackExecutor`), so they would
>   reappear after `clearState`. `DownloadRefreshOrchestrator.quiesceWipeRecover()` now also
>   `quiesceForWipe()`s `ConsumerOffsetTracker`/`DeliveryStateStore` (cancel flush, drop in-memory,
>   reject writes) before the wipe and `resumeAfterWipe()`s (reload from the wiped/restored file) after.
> - *`data-refresh-state` preservation deferred* — still cleared by `clearState` (the P4/P5 "preserve
>   across wipe" item is a follow-up); crash-mid-wipe recovery behaves as before.

### Open implementation nuances (resolved in-phase)

1. **RESET timing vs reconnect backoff** — RESET fires after `resumeAccepting()`; consumers may be 5–60s behind on the client reconnect backoff. The existing RESET retry (5s), the `SubscribeHandler` late-join path (`registerLateJoiningConsumer`), and the 10-min abort watchdog absorb the gap, so no explicit reconnect wait is needed.
2. **Expected-consumers across reconnect** — RESET/READY are tracked by **group-topic**, not ephemeral client id, so a consumer returning with a new id still satisfies its expected-set slot.
3. **NetworkServer stop→start rebind** — RESOLVED to a real stop/restart (not a "reject ACKs" fallback): `start(port)` reassigns fresh event-loop groups, `stopAccepting()`/`shutdown()` retain the handler lists, and `SO_REUSEADDR` makes the listen port rebind cleanly. See the P5 deviations note above.
4. **STREAM async timing** — with "resume normal pipe from 0," data arrives asynchronously, so replay/READY runs against a filling store, gated by the READY settle window (a conscious choice).

## Source selection

`DownloadRefreshOrchestrator.chooseSource` (reactive, no cross-node coordination):

| Condition | Source |
|---|---|
| No parent (root node) | `CLOUD_SYNC` |
| Parent `/health` DOWN (mid-refresh / unreachable) | `CLOUD_SYNC` (escalate) |
| Parent has a snapshot (`/pipe/snapshot/info`) | `PIPE_AND_PROVIDER_FILE_DOWNLOAD` |
| Otherwise | `PIPE_AND_PROVIDER_STREAM` |

A parent reports `/health` DOWN during any blocking refresh (`RefreshHealthIndicator`). If `broker.refresh.health-critical-topics` is configured, only those topics block parent health; non-critical refreshes can stay green.

## Flow

`POST /admin/download-refresh` → `DownloadRefreshController` (async, single-flight, virtual thread) → `DownloadRefreshService.runBootstrapAndRefresh`:

1. `DownloadRefreshOrchestrator.bootstrap()` re-sources data (blocking, synchronous):
   - **PIPE_AND_PROVIDER_FILE_DOWNLOAD**: `downloadSnapshot` (stream to `snapshots/incoming.zip`) → `SnapshotRestorer.restore` (stage → atomic per-topic swap) → `LocalStateCleaner.clearState`. Crash-safe: download + swap happen *before* clearing ack/offset state, so a failed download leaves live data intact.
   - **PIPE_AND_PROVIDER_STREAM**: `clearState` + `clearTopicData` → k-way-merge pull from the parent (`/pipe/poll` with `X-Pipe-Cursors`) until 204.
   - **CLOUD_SYNC**: `clearState` + `clearTopicData` → global pull from the cloud from offset 0.
   - **Mid-stream escalation**: if a parent path fails partway (parent enters refresh / dies), escalate to a jittered cloud bootstrap instead of failing.
   - **Pipe pause scope**: pipe polling is paused only while storage is closed and local restore/wipe/state-clear work mutates topic folders or `pipe-offset.properties`; it resumes before parent/cloud bulk ingest.
2. On success, trigger `RefreshCoordinator.startRefresh(topic)` for every affected topic — from the snapshot manifest (file-download) or the topics present in storage (incremental/cloud). The existing RESET→replay→READY then delivers the fresh copy, gated by the captured [replay start window](#replay-window-and-ready) and the [READY settle gate](#ready-settle-gate-created_time).

`BARE_METAL` now follows this same in-process destructive lifecycle and auto-selects the best upstream source. The older stop/wipe/`System.exit` helper remains as a compatibility bean but is not used by the admin refresh type.

## The snapshot

`SnapshotBuilder` produces a ZIP of **pure topic data** + a `manifest.json` of per-topic head watermarks.

- **Included**: each `<topic>/segment_metadata.db` (SQLite) + `partition-0/` segment + index files.
- **Excluded** (parent-specific state the child rebuilds or re-derives): `ack-store/` (RocksDB ACK + compaction CF — rebuilt via the existing backfill path), all top-level state files (`consumer-offsets`/`topology`/`pipe-offset`/`data-refresh-state`/`delivery-state`.properties), `events.db` (cloud-resident), logs. Rule: include only files inside a non-excluded topic subfolder (depth ≥ 2); drop every top-level file.

`SnapshotScheduler` (parent-capable POS, `broker.snapshot.enabled`) periodically builds + atomically publishes the ZIP (`SnapshotStore`); `SnapshotController` serves `/pipe/snapshot` + `/pipe/snapshot/info`.

`LocalStateCleaner.clearState` deletes the parent-specific state files but keeps `topology.properties` (this node's own registry identity) and topic data; the RocksDB ACK + compaction CFs are cleared in place by `SharedRocksDb.clearCompactionAndAck()` (not by deleting `ack-store/`, which would corrupt the open singleton handle). `clearTopicData` is the full topic wipe for the cloud/no-snapshot path.

## The pipe k-way merge (`/pipe/poll`)

The incremental pull uses a single `/pipe/poll` endpoint with two modes, selected by the presence of the `X-Pipe-Cursors` header:

- **Merge mode** (header present): a Kafka-style k-way merge (min-heap over per-topic buffered cursors, `PipeServer`) across all topics in one ~1 MB response, ordered by offset. Each topic has its **own** offset space (topic A in the 10000s, topic B in the 20000s), so a single cursor would lose or duplicate data — the child passes a **per-topic** cursor map in `X-Pipe-Cursors`; the server serves each topic strictly above its cursor and returns the advanced cursors in the same header. Each topic advances independently and exclusively → **no duplicates** across polls; a 1 MB truncation leaves un-drained topics' cursors unchanged. `X-Pipe-Heads` carries per-topic head offsets (progress denominator).
- **Legacy mode** (no header): the original single-topic poll the steady-state `HttpPipeConnector` uses — unchanged.

Ingestion is offset-idempotent (skip when `offset <= topic head`), so a retry after a partial pull resumes without double-storing.

## Replay window and READY

`broker.refresh.replay.window-hours` is the replay **start** horizon (how far back to re-deliver). `0` = full available history (start at the earliest retained offset). A positive value scans `MessageRecord.createdAt` and starts from the first record inside the last N hours. The READY **target** is set separately by the settle gate below.

Legacy consumers persist last-delivered offsets, so their reset offset is `replayStartOffset - 1`. Modern consumers persist next-to-deliver offsets, so they must commit `target + 1` before READY.

## READY settle gate (created_time)

READY (and `/health` green) for a topic fires once consumers have caught up to the **settled history**: every record whose immutable `created_time` is **older than** `broker.refresh.ready-settle-window-ms` (default 6h). Records created *within* the window are still settling and are **not** required for READY — they arrive via normal delivery afterwards. `RefreshReplayWindowResolver.resolve()` computes the target:

- **some records inside the window** (e.g. a current-time record) → target = the last record *before* the window; the recent tail is excluded. (READY before the latest record is delivered — intended.)
- **no record inside the window** (a slow/static topic, all data old) → target = head; deliver everything, then READY.
- **every record inside the window** (a brand-new/fast topic) → target = `-1`; nothing required, READY immediately.
- **empty topic** (`head < 0`) → nothing to replay.

`0` disables the gate (require full catch-up to head). **LOCAL refresh is exempt** — it re-pushes existing local segments (no re-source), so it always targets the head and waits for full catch-up regardless of the settle window. The gate is purely catch-up to this target — there is no separate delivery-wall-clock check. `created_time` is authoritative and immutable: preserved verbatim through the pipe and through `DataHandler` (which reads `created` from the producer payload rather than stamping ingest time). See [POS state](07-pos-machine-state.md).

## Coordination during refresh

- **Compaction**: `CompactionScheduler` skips any topic under active refresh (`isRefreshActive`) — a refresh swaps/wipes that topic's segments; per-topic skip, other topics still compact.
- **Pipe consistency**: `PipeConsistencyScheduler` skips the whole audit while any refresh is in progress (`isRefreshInProgress`) — a local-vs-parent compare mid-rebuild would report false drift.

`RefreshCoordinator` is injected into both via a lazy `BeanProvider` to avoid a DI cycle.

## Progress

`GET /admin/download-refresh/status` exposes `BootstrapProgressTracker`:

- **Snapshot**: exact byte % (streamed bytes / Content-Length).
- **Incremental**: approximate offset % (cursor-sum / head-sum — sparse offsets).
- **Cloud**: running record count, no % (looping/unbounded source).
- Phases: `DOWNLOADING → INGESTING → REFRESHING → DONE/FAILED`.

## Configuration

| Key | Default | Purpose |
|---|---|---|
| `broker.refresh.ready-settle-window-ms` | `21600000` (6h) | READY settle window by `created_time`; READY waits only for records older than this. 0 = full catch-up to head |
| `broker.refresh.replay.window-hours` | `0` | Replay start horizon (how far back to re-deliver); 0 = full available history |
| `broker.refresh.health-critical-topics` | `[]` | Topics whose active refreshes block `/health`; empty = any active refresh blocks |
| `broker.bootstrap.fresh-install.enabled` | `true` (false in tests) | Auto-bootstrap an empty/fresh node on boot (health-gated re-source + RESET→READY) instead of plain pipe streaming |
| `broker.refresh.readiness-policy` | `global` | Completion-barrier policy: `global` = node-wide, hold every topic until all settle (the only impl today; seam for future per-topic/per-consumer) |
| `broker.snapshot.enabled` | `false` | Build snapshots (parent-capable POS) |
| `broker.snapshot.interval` / `initial-delay` | `6h` / `10m` | Snapshot scheduler cadence |
| `broker.bootstrap.max-batches` | `100000` | Pull-loop safety cap (finite source ends on 204) |
| `broker.bootstrap.escalation-jitter-ms` | `30000` | Cloud-escalation jitter (anti-stampede) |
| `broker.cloud.data-url` | → registry url | Cloud target for escalation (any node) |
| `broker.bare-metal.exit-code` | `70` | Legacy BareMetalResetService exit code; not used by admin `BARE_METAL` refresh |
| `broker.bare-metal.settle-ms` | `500` | Legacy delay before teardown; not used by admin `BARE_METAL` refresh |

## Refresh type (caller-chosen)

The refresh is type-driven: `POST /admin/download-refresh` takes `?type=` or a JSON body `{"type":..}` (`RefreshType.from`, default `PIPE_AND_PROVIDER_REFRESH`). `DownloadRefreshService.runRefresh(type)` dispatches:

| `type` | Behavior |
|---|---|
| `LOCAL` | Replay the node's own segments to consumers — no download (the original refresh). |
| `PIPE_AND_PROVIDER_REFRESH` | Wipe + re-source from the provider, **auto-resolving** file-download vs stream (default). |
| `PIPE_AND_PROVIDER_FILE_DOWNLOAD` / `PIPE_AND_PROVIDER_STREAM` | Advanced: wipe + re-source, **forcing** that provider source. |
| `CLOUD_SYNC` | Wipe + re-source from the cloud. |
| `BARE_METAL` | Destructive re-source using the normal in-process lifecycle; auto-selects the upstream source and then refreshes consumers. |

A forced provider source that is unavailable (e.g. file-download with no snapshot, or a parent that dies) escalates to `CLOUD_SYNC` via the [mid-stream escalation](#flow).

## API

- `POST /admin/download-refresh?type=PIPE_AND_PROVIDER_REFRESH` — start (async, single-flight); `type` also accepted in the JSON body.
- `GET /admin/download-refresh/status` — running / type / progress / last result.
- `GET /pipe/snapshot`, `GET /pipe/snapshot/info` — serve snapshot ZIP / availability + watermark.
- `GET /pipe/poll` (+ `X-Pipe-Cursors` header) — k-way-merge multi-topic pull.

## Tests

Unit: `RefreshReplayWindowResolverSpec` (settle-target: X/Y excluded-tail, Z full, fast→-1, empty), `PipeServerSpec` (merge, no-dup, legacy), `SnapshotBuilderSpec`, `SnapshotRestorerSpec`, `SnapshotSchedulerSpec`, `LocalStateCleanerSpec`, `DownloadRefreshOrchestratorSpec` (source selection, crash-safe order, mid-stream escalation), `HttpBootstrapSourceClientSpec` (dedup ingest), `DownloadRefreshServiceSpec`, `BootstrapProgressTrackerSpec`, scheduler skip specs. Context-boot: `DownloadRefreshWiringSystemSpec` (full broker context boots, whole bean graph resolves).

## Pending

- **Multi-broker journey tests** (pairwise + invariant oracle + fuzz; in-process two-context + black-box multi-JVM) — the end-to-end parent→child download is not yet exercised over real HTTP in a journey.
- **Deferred streaming bootstrap** — the current bootstrap is synchronous: data is loaded before consumer refresh starts, then READY waits on the captured replay target. A future async-streaming model would need an explicit bootstrap watermark gate.
- **Steady-state multi-topic POS→POS** — `HttpPipeConnector` still uses a single global offset and the legacy single-topic poll; migrating it to the per-topic cursor protocol (and reconciling the cloud's single-offset shape) is deferred. The download-refresh bootstrap uses the merge path correctly.
