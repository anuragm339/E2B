# Download Refresh

Related: [POS machine state](07-pos-machine-state.md), [Compaction](08-compaction-handling.md), [Pipe consistency](16-pipe-consistency-system-design.md), [Runtime config](11-runtime-config.md), [Test map](10-test-map.md).

Status: implemented on branch `feature/download-refresh` (not yet merged). Validated by unit suites + a context-boot smoke; full multi-broker journey tests are still pending (see [Pending](#pending)).

## What it is

A second kind of refresh, alongside the existing **LOCAL** refresh (which replays a node's *own* local segments to consumers; see [POS state](07-pos-machine-state.md#refresh-state-machine)). **Download refresh** throws away the node's local copy and re-sources authoritative data from upstream, then runs the normal RESET→replay→READY so consumers receive the fresh copy.

Plain terms: "wipe my data and get a fresh copy from above." A neighbour POS with a ready-made snapshot ZIP → download + unzip (fast). No snapshot → stream every topic from the parent. Talks straight to the cloud, or the parent is busy → pull from the cloud.

## Source selection

`DownloadRefreshOrchestrator.chooseSource` (reactive, no cross-node coordination):

| Condition | Source |
|---|---|
| No parent (root node) | `CLOUD_SYNC` |
| Parent `/health` DOWN (mid-refresh / unreachable) | `CLOUD_SYNC` (escalate) |
| Parent has a snapshot (`/pipe/snapshot/info`) | `PIPE_AND_PROVIDER_FILE_DOWNLOAD` |
| Otherwise | `PIPE_AND_PROVIDER_STREAM` |

A parent reports `/health` DOWN during any refresh (`RefreshHealthIndicator`), so "don't source from a refreshing parent" falls out of the existing health probe.

## Flow

`POST /admin/download-refresh` → `DownloadRefreshController` (async, single-flight, virtual thread) → `DownloadRefreshService.runBootstrapAndRefresh`:

1. `DownloadRefreshOrchestrator.bootstrap()` re-sources data (blocking, synchronous):
   - **PIPE_AND_PROVIDER_FILE_DOWNLOAD**: `downloadSnapshot` (stream to `snapshots/incoming.zip`) → `SnapshotRestorer.restore` (stage → atomic per-topic swap) → `LocalStateCleaner.clearState`. Crash-safe: download + swap happen *before* clearing ack/offset state, so a failed download leaves live data intact.
   - **PIPE_AND_PROVIDER_STREAM**: `clearState` + `clearTopicData` → k-way-merge pull from the parent (`/pipe/poll` with `X-Pipe-Cursors`) until 204.
   - **CLOUD_SYNC**: `clearState` + `clearTopicData` → global pull from the cloud from offset 0.
   - **Mid-stream escalation**: if a parent path fails partway (parent enters refresh / dies), escalate to a jittered cloud bootstrap instead of failing.
2. On success, trigger `RefreshCoordinator.startRefresh(topic)` for every affected topic — from the snapshot manifest (file-download) or the topics present in storage (incremental/cloud). The existing RESET→replay→READY then delivers the fresh copy, gated by the [delivery-freshness gate](#delivery-freshness-gate).

## The snapshot

`SnapshotBuilder` produces a ZIP of **pure topic data** + a `manifest.json` of per-topic head watermarks.

- **Included**: each `<topic>/segment_metadata.db` (SQLite) + `partition-0/` segment + index files.
- **Excluded** (parent-specific state the child rebuilds or re-derives): `ack-store/` (RocksDB ACK + compaction CF — rebuilt via the existing backfill path), all top-level state files (`consumer-offsets`/`topology`/`pipe-offset`/`data-refresh-state`/`delivery-state`.properties), `events.db` (cloud-resident), logs. Rule: include only files inside a non-excluded topic subfolder (depth ≥ 2); drop every top-level file.

`SnapshotScheduler` (parent-capable POS, `broker.snapshot.enabled`) periodically builds + atomically publishes the ZIP (`SnapshotStore`); `SnapshotController` serves `/pipe/snapshot` + `/pipe/snapshot/info`.

`LocalStateCleaner.clearState` deletes `ack-store/` + the parent-specific state files but keeps `topology.properties` (this node's own registry identity) and topic data; `clearTopicData` is the full topic wipe for the cloud/no-snapshot path.

## The pipe k-way merge (`/pipe/poll`)

The incremental pull uses a single `/pipe/poll` endpoint with two modes, selected by the presence of the `X-Pipe-Cursors` header:

- **Merge mode** (header present): a Kafka-style k-way merge (min-heap over per-topic buffered cursors, `PipeServer`) across all topics in one ~1 MB response, ordered by offset. Each topic has its **own** offset space (topic A in the 10000s, topic B in the 20000s), so a single cursor would lose or duplicate data — the child passes a **per-topic** cursor map in `X-Pipe-Cursors`; the server serves each topic strictly above its cursor and returns the advanced cursors in the same header. Each topic advances independently and exclusively → **no duplicates** across polls; a 1 MB truncation leaves un-drained topics' cursors unchanged. `X-Pipe-Heads` carries per-topic head offsets (progress denominator).
- **Legacy mode** (no header): the original single-topic poll the steady-state `HttpPipeConnector` uses — unchanged.

Ingestion is offset-idempotent (skip when `offset <= topic head`), so a retry after a partial pull resumes without double-storing.

## Delivery-freshness gate

A refresh may reach READY (and the broker report healthy/green) only once a real delivery reached a consumer within `broker.refresh.delivery-freshness-window-ms` (default 6h), unless the topic is empty (`head < 0`, healthy-idle). Implemented in `RefreshReplayService` against the broker-wide `DeliveryFreshnessTracker` (stamped on every modern + legacy ACK in `BatchAckService`). Applies to LOCAL refresh too. See [POS state](07-pos-machine-state.md).

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
| `broker.refresh.delivery-freshness-window-ms` | `21600000` (6h) | READY gate window; 0 disables |
| `broker.snapshot.enabled` | `false` | Build snapshots (parent-capable POS) |
| `broker.snapshot.interval` / `initial-delay` | `6h` / `10m` | Snapshot scheduler cadence |
| `broker.bootstrap.max-batches` | `100000` | Pull-loop safety cap (finite source ends on 204) |
| `broker.bootstrap.escalation-jitter-ms` | `30000` | Cloud-escalation jitter (anti-stampede) |
| `broker.cloud.data-url` | → registry url | Cloud target for escalation (any node) |
| `broker.bare-metal.exit-code` | `70` | BARE_METAL `System.exit` code (non-zero for the supervisor) |
| `broker.bare-metal.settle-ms` | `500` | Delay before teardown so the admin response flushes |

## Refresh type (caller-chosen)

The refresh is type-driven: `POST /admin/download-refresh` takes `?type=` or a JSON body `{"type":..}` (`RefreshType.from`, default `PIPE_AND_PROVIDER_REFRESH`). `DownloadRefreshService.runRefresh(type)` dispatches:

| `type` | Behavior |
|---|---|
| `LOCAL` | Replay the node's own segments to consumers — no download (the original refresh). |
| `PIPE_AND_PROVIDER_REFRESH` | Wipe + re-source from the provider, **auto-resolving** file-download vs stream (default). |
| `PIPE_AND_PROVIDER_FILE_DOWNLOAD` / `PIPE_AND_PROVIDER_STREAM` | Advanced: wipe + re-source, **forcing** that provider source. |
| `CLOUD_SYNC` | Wipe + re-source from the cloud. |
| `BARE_METAL` | Factory reset: stop everything, wipe the storage dir's contents, `System.exit` for an external supervisor to restart (no in-process re-source). |

A forced provider source that is unavailable (e.g. file-download with no snapshot, or a parent that dies) escalates to `CLOUD_SYNC` via the [mid-stream escalation](#flow).

## API

- `POST /admin/download-refresh?type=PIPE_AND_PROVIDER_REFRESH` — start (async, single-flight); `type` also accepted in the JSON body.
- `GET /admin/download-refresh/status` — running / type / progress / last result.
- `GET /pipe/snapshot`, `GET /pipe/snapshot/info` — serve snapshot ZIP / availability + watermark.
- `GET /pipe/poll` (+ `X-Pipe-Cursors` header) — k-way-merge multi-topic pull.

## Tests

Unit: `DeliveryFreshnessTrackerSpec`, `RefreshReplayGateSpec`, `PipeServerSpec` (merge, no-dup, legacy), `SnapshotBuilderSpec`, `SnapshotRestorerSpec`, `SnapshotSchedulerSpec`, `LocalStateCleanerSpec`, `DownloadRefreshOrchestratorSpec` (source selection, crash-safe order, mid-stream escalation), `HttpBootstrapSourceClientSpec` (dedup ingest), `DownloadRefreshServiceSpec`, `BootstrapProgressTrackerSpec`, scheduler skip specs. Context-boot: `DownloadRefreshWiringSystemSpec` (full broker context boots, whole bean graph resolves).

## Pending

- **Multi-broker journey tests** (pairwise + invariant oracle + fuzz; in-process two-context + black-box multi-JVM) — the end-to-end parent→child download is not yet exercised over real HTTP in a journey.
- **Watermark gate** — guarding against false-green while data loads is unnecessary in the current *synchronous* bootstrap (data is fully loaded before the refresh triggers); it belongs with the deferred async-streaming model.
- **Steady-state multi-topic POS→POS** — `HttpPipeConnector` still uses a single global offset and the legacy single-topic poll; migrating it to the per-topic cursor protocol (and reconciling the cloud's single-offset shape) is deferred. The download-refresh bootstrap uses the merge path correctly.
