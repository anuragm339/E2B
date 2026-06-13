# API Catalog

Related: [Features](03-feature-catalog.md), [Events](06-event-kafka-flow.md), [POS state](07-pos-machine-state.md), [Runtime config](11-runtime-config.md), [Risks](13-risk-and-edge-cases.md).

## Pipe API

### `GET /pipe/poll`

- Controller: `pipe/src/main/java/com/messaging/pipe/PipeServer.java`
- Query: `offset` default `0`, `limit` default `100`, `topic` default `price-topic`
- Success: JSON array of `MessageRecord`
- No data: HTTP `204`
- Failure: HTTP `500` with text
- Service flow: `StorageEngine.read(topic, 0, offset, limit)`
- Data model: [MessageRecord](05-data-model.md#record-models)
- Consumer: `pipe/src/main/java/com/messaging/pipe/HttpPipeConnector.java`

Important: the connector builds `/pipe/poll?offset=...&limit=...` without a `topic` query. Which topic an external parent returns is **not confirmed from code**.

## Pipe Consistency API

Detect-only audit of this node against its parent/the cloud. Fail-closed: every endpoint
answers `404` when `pipe.consistency.enabled=false` (default); the cloud must mirror the
three served endpoints. Design: `readme/PIPE_CONSISTENCY_V2.md`.

### `GET /pipe/consistency/head`

- O(1) head probe (in-memory storage head, no scan, no semaphore) — lets a child filter
  verifier candidates before asking anyone to digest-scan. `{head: N}`, `404` when disabled.

### `GET /pipe/consistency/digest`

- Controller: `broker/src/main/java/com/messaging/broker/http/PipeConsistencyController.java`
- Query: `topic`, `watermark` (child head), `buckets` default `64`
- Success: `{parentHead, effectiveWatermark, digests[], counts[]}` (~1 KB)
- Guards: `400` invalid params, `429` above `max-concurrent-scans`
- Service flow: one streaming `CompactionIndex.forEachEntry` scan folded by `KeyspaceDigest`

### `GET /pipe/consistency/bucket`

- Query: `topic`, `watermark`, `buckets`, `bucket` = comma-separated mismatched bucket ids
- Success: `{entries: {bucketId: [{h: keyHash64, o: latestOffset}, ...]}}` — one scan serves all requested buckets
- Guards: `413` above `max-bucket-entries`

### `POST /pipe/consistency/classify`

- Body: `{topic, watermark, offsets[], keys[]}`
- Success: `offsets` → record-at-exact-offset physically readable (distinguishes missed data
  from expired tombstones); `keys` → `PRESENT_BEYOND_WATERMARK | PRESENT_AT_OR_BELOW_WATERMARK | ABSENT`;
  `authoritative: true` only when the answering node's keyspace is complete and never expires
  (the cloud sets it; POS brokers never do). An authoritative `ABSENT` proves the child key
  was fabricated/corrupted (`fabricatedKeys`, INCONSISTENT); a non-authoritative `ABSENT`
  stays a benign `extraKeys` warning (a freshly provisioned parent may simply lack history).

### Escalation (clamped parent → in-store verifiers)

A clamped watermark (parent behind this node — only possible after a reshuffle) marks the
report `verificationPending`. When the clamp persists `escalation.after-clamped-checks`
consecutive checks, the broker probes the registry-provided `verifierCandidates` (cached
from the existing topology poll; dead/behind candidates skipped via the head probe) and
obtains the FULL verdict from the first one whose head covers the watermark
(`escalatedFrom` records the original parent). LAN-only; the cloud is never called.
The registry's topology response carries `verifierCandidates` (omitted when empty —
brokers fall back to `requestToFollow[1..]`).

### `POST /admin/pipe-consistency/check?topic=all|T&target=parent|cloud`

- Controller: `broker/src/main/java/com/messaging/broker/http/PipeConsistencyAdminController.java`
- `202` started (async on `compactionExecutor`), `409` already running, `503` disabled
- Service: `broker/src/main/java/com/messaging/broker/consistency/PipeConsistencyService.java` (single-flight)

### `GET /admin/pipe-consistency/report`

- Latest verdict per topic + bounded history; states `CONSISTENT | CONSISTENT_UP_TO |
  INCONSISTENT | INCONCLUSIVE | UNREACHABLE | UNSUPPORTED_PARENT | ERROR`
  (`INCONCLUSIVE` = drill-down cap exceeded at a clamped watermark — divergence may be
  benign child-ahead, e.g. the dev cloud's loopback replay); counts for missing/stale/
  zombie/fabricated/lagging/extra/child-newer keys and `refreshRecommended`

### `POST /test/consistency/tamper-index` — TEMPORARY, delete after validation

- Controller: `broker/src/main/java/com/messaging/broker/http/ConsistencyTamperTestController.java`
- Query: `topic`, `removeCount` (delete first N compaction-index entries → next check vs
  cloud shows `missingKeys`), `addCount` (insert fake keys at the topic head → `fabricatedKeys`)
- Fault-injection tool for live INCONSISTENT validation only; no specs by design.

## Refresh API

Controller: `broker/src/main/java/com/messaging/broker/http/RefreshController.java`.

### `POST /admin/refresh-topic`

- Body forms:
  - `{"topic":"prices-v1,reference-data-v5"}`
  - `{"topics":["prices-v1","reference-data-v5"]}`
- Returns immediately with `success`, `message`, `topics`, and `status: INITIATED`.
- Empty/malformed input returns a map with `success:false`; explicit HTTP error status is not set.
- Service: `RefreshCoordinator.startRefresh`.
- State flow: [POS state](07-pos-machine-state.md#refresh-state-machine).

### `GET /admin/refresh-status?topic=...`

Returns state, counts, timestamps, and per-consumer RESET/READY/replay/offset details from `RefreshContext`.

### `GET /admin/refresh-current`

Returns one arbitrary current context via `activeRefreshes.values().stream().findFirst()`. With concurrent topic refreshes it is not a complete batch view.

## Compaction API

Controller: `broker/src/main/java/com/messaging/broker/http/CompactionController.java`.

### `POST /admin/compaction/trigger`

Submits an asynchronous compaction run. Its preparation callback force-rolls every active topic segment before compaction. If another run is active, `triggered:false`.

Response: `triggered`, `sealedTopics`, `sealErrors`, `message`.

Service/data links: [Compaction](08-compaction-handling.md), [Segment storage](05-data-model.md#segment-storage).

### `GET /admin/compaction/status`

Returns per-topic sealed segment count/bytes, total sealed count, and an operator tip.

## Runtime Logging API

Controller: `broker/src/main/java/com/messaging/broker/http/LoggingController.java`.

- `GET /admin/logging/features`
- `GET /admin/logging/feature/{feature}`
- `POST /admin/logging/feature/{feature}?level=DEBUG|INFO|...|RESET`

Service: `broker/src/main/java/com/messaging/broker/monitoring/RuntimeLogLevelService.java`.

The test consumer exposes the same paths from `test-consumer/src/main/java/com/example/consumer/http/LoggingController.java`.

## Diagnostics And Management

Controller: `broker/src/main/java/com/messaging/broker/http/ThreadDiagnosticsController.java`.

| Method/path | Purpose |
|---|---|
| `GET /diagnostics/threads/summary` | Thread counts, states, deadlock/problem counts |
| `GET /diagnostics/threads/top-cpu?limit=N` | Highest cumulative CPU-time threads |
| `GET /diagnostics/threads/problematic` | Monitor-classified blocked/waiting threads |
| `GET /diagnostics/threads/dump` | Plain-text thread dump |
| `GET /diagnostics/threads/{threadId}` | Thread details and stack |
| `GET /diagnostics/threads/deadlocks` | Deadlock report |
| `GET /diagnostics/threads/top-memory?limit=N` | Per-thread allocation estimates |
| `GET /diagnostics/threads/resources?category=...` | Resource details |
| `GET /diagnostics/threads/by-category` | Aggregated resource categories |

Micronaut management enables `/health`, `/prometheus`, and `/metrics` in `broker/src/main/resources/application.yml`.

## Test Data API

Controller: `broker/src/main/java/com/messaging/broker/http/TestDataController.java`.

### `POST /test/load-from-sqlite`

Body: `sqliteFilePath`, optional `tableName` default `messages`, optional `topic` default `price-topic`.

The controller opens `jdbc:sqlite:<path>`, executes `SELECT * FROM <tableName>`, converts each row to JSON text, and appends it. The table name is concatenated directly; see [Security risks](13-risk-and-edge-cases.md#security).

### `POST /test/inject-messages`

Body: optional `count`, `topic`, `prefix`; appends generated records directly to storage.

### `GET /test/stats`

Returns placeholder/fake statistics. This is marked TODO in `TestDataController.java` and must not be used as an operational source of truth.

## TCP Data Plane

Message envelope: `[type:1][messageId:8][payloadLength:4][payload]`, implemented by `BinaryMessageEncoder.java` and `BinaryMessageDecoder.java`.

| Type | Direction | Payload/behavior | Handler |
|---|---|---|---|
| `DATA` | producer -> broker | JSON producer record | `DataHandler.java` |
| `ACK` | broker -> producer/client | Empty or generic legacy ACK | client connection / legacy state |
| `SUBSCRIBE` | consumer -> broker | Modern `{topic,group}` or legacy adapter JSON | `SubscribeHandler.java` |
| `COMMIT_OFFSET` | client -> broker | JSON `{topic,group,offset}` | `CommitOffsetHandler.java` |
| `RESET` | broker -> consumer | Topic bytes | client `onReset` |
| `READY` | broker -> consumer | Topic bytes or empty startup payload | client `onReady` |
| `DISCONNECT` | either/control | Empty | client reconnect logic |
| `HEARTBEAT` | defined | No registered broker handler found |
| `BATCH_HEADER` | broker -> consumer | count, bytes, topic, group; raw bytes follow | `ZeroCopyBatchDecoder.java` |
| `BATCH_ACK` | consumer -> broker | topic/group lengths and bytes | `BatchAckHandler.java` |
| `RESET_ACK` | consumer -> broker | topic/group lengths and bytes | `ResetAckHandler.java` |
| `READY_ACK` | consumer -> broker | topic/group lengths and bytes | `ReadyAckHandler.java` |

Definitions: `common/src/main/java/com/messaging/common/model/BrokerMessage.java`.

Detailed flow: [Event chapter](06-event-kafka-flow.md).

## API Error Behavior

- TCP validation failures generally close the connection.
- ACK handler malformed length fields close the connection; unexpected legacy group lookup soft-fails.
- HTTP controllers generally return maps with error fields rather than `HttpResponse` error status.
- Pipe server is the exception: it explicitly returns `204`, `200`, or `500`.

Sources: handlers under `broker/src/main/java/com/messaging/broker/handler/` and controllers under `broker/src/main/java/com/messaging/broker/http/`.

## Authentication

No inbound API authentication implementation was found. `AuthTokenClientFilter.java` is outbound only. See [Runtime security](11-runtime-config.md#security-and-authentication) before exposing these endpoints.

### Cloud-server mirror (separate repo)

`cloud-server` implements the same three served endpoints over its SQLite `event` table
(`cloud-server/src/main/java/com/messaging/cloudserver/controller/PipeConsistencyController.java`),
applying the identical `event_size <= 19990` serving filter as `/pipe/poll` and always clamping
the watermark to `dbMaxOffset` (loopback virtual offsets are unrecorded and unverifiable).
Hash functions are duplicated and pinned bit-identical via shared test vectors in both repos.
Enabled via `PIPE_CONSISTENCY_ENABLED=true` (fail-closed default false, same as the broker).
