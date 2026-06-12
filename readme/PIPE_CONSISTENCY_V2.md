# Pipe Consistency v2 — compaction-invariant keyspace audit

Status: IMPLEMENTED both sides — broker 2026-06-11, cloud-server 2026-06-12
(`cloud-server/src/main/java/com/messaging/cloudserver/{consistency,controller}`,
SQLite-backed, watermark always clamped to `dbMaxOffset` because loopback virtual offsets —
including random re-sends — are unrecorded and unverifiable). Hash functions are duplicated
across the repos and pinned bit-identical by shared test vectors
(`KeyspaceDigestSpec` ↔ `KeyspaceDigestVectorTest`).

Auto-detection added 2026-06-12: `PipeConsistencyScheduler` runs `check?topic=all` on a
configurable cadence (`pipe.consistency.schedule.*`, default 6h/10m, target parent|cloud) —
no manual API call needed. Skip-don't-queue guards: schedule kill-switch, single-flight,
no-parent (offline POS), heap pressure. Bean exists only when the feature flag is on.

Verdict added 2026-06-12: `INCONCLUSIVE` — drill-down cap exceeded at a CLAMPED watermark
(divergence may be entirely benign child-ahead keys, e.g. a child that wrapped through the
dev cloud's loopback replay). At equal watermarks the same condition stays `INCONSISTENT`
(real mass divergence). Drill-down also collects the child's bucket entries UNFILTERED so a
key advanced past the clamp classifies as benign child-ahead instead of false-missing.

## 0. As-built deltas from the original draft

Implementation investigation refined three protocol points:

1. **One bucket request, not one per bucket.** `GET /pipe/consistency/bucket` takes a
   comma-separated bucket list and the parent serves all of them from a single index scan
   (`max-bucket-entries` cap, HTTP 413 above it). One parent-side scan per drill-down
   instead of one per mismatched bucket.
2. **Compaction-index entries are never deleted** (verified in `RocksDbCompactionIndex`),
   which makes detection of missed deletes *stronger* than the draft assumed (the parent's
   index remembers a tombstone's offset even after the record physically expires) but
   creates one false-positive risk: a freshly-provisioned child never ingests records whose
   tombstones already expired upstream, so the parent's remembered entry would flag as
   "missing". Resolved by the classify step: the parent reports whether the record at the
   suspicious offset is **physically readable**. Compacted-away + child-has-nothing ⇒ benign
   (both sides agree the key is dead). Compacted-away + child-holds-an-old-version ⇒
   **zombie** (the offline-past-tombstone-retention case). Physically-present + child
   missing/older ⇒ **missing/stale** (real missed data).
3. **Classify works on offsets and key strings, not key hashes** — the parent cannot look up
   by hash without a scan. Only the few suspicious entries (capped by
   `max-classify-entries`) are ever sent; key strings cross the wire only for child-extra
   keys the child already holds.

Implementation lives in `broker/src/main/java/com/messaging/broker/consistency/`
(`KeyspaceDigest`, `PipeConsistencyService`, `ParentConsistencyClient`,
`PipeConsistencyReport`) plus `broker/http/PipeConsistencyController` (served endpoints)
and `broker/http/PipeConsistencyAdminController` (trigger + report). Config:
`pipe.consistency.*` in `application.yml`, `enabled: false` default.

## 1. Problem

POS brokers replicate cloud data down a waterfall (cloud → POS → POS → …). Every node
compacts its own log on its own schedule, nodes go offline for arbitrary periods, and the
parent/child topology is reshuffled at any time by the registry. We need an on-demand
answer to: **"does this POS have everything its parent has, or did we miss data in
between?"** — without meaningful CPU/memory/disk-IO cost and with minimal network calls.

Definitions agreed:

- **Consistent** = for every message key the parent holds (up to the child's covered
  watermark), the child holds the same key at the same latest offset. Logical state, not
  physical bytes — compaction on either side at any time must never affect the verdict.
- **Lag is not inconsistency.** A child that simply hasn't pulled offsets yet is healthy.
- **Detect only.** No auto-repair in v1; report + metrics.
- **Initiation**: child checks against its *current* parent (from `TopologyManager`); can
  be force-pointed at the cloud. Trigger is an admin API now, scheduler later.
- **Offsets are immutable** from cloud to every POS (confirmed) — this is the keystone.

## 2. Why v1 (HOP/DEEP, commit 1bf5563) was structurally wrong

v1 hashed **physical records and segments** (per-record CRC32C on the append hot path,
rolling Murmur3 per segment, Merkle topic roots). Physical layout is exactly what
independent compaction changes, so v1 needed epoch bumps, `HashCache` invalidation on
every compaction, lineage stores, and bounded multi-hop walks — ~4,500 lines and a
permanent hot-path tax, and any compaction anywhere forced re-hashing.

v2 compares a quantity that compaction **cannot change**: the latest offset per key.
Compacting deletes only superseded records; the `(key → latestOffset)` map is invariant.
Both sides already maintain exactly this map — the `CompactionIndex`
(`RocksDbCompactionIndex`), updated on every pipe ingest. No new hot-path work, no
epochs, no invalidation.

## 3. Core mechanism

### 3.1 Digest

For topic `T` and watermark `W` (the child's ingested head for `T`):

```
bucket(key)        = hash64(key) % B                  (B = 64 buckets, configurable)
contribution(k,o)  = mix64(hash64(k) ^ o)             (order-independent)
digest[b]          = XOR of contribution(k, o) for every index entry (k, o) with o ≤ W
                     and bucket(k) == b
```

Computed **on demand** by a single streaming prefix-scan of the topic's compaction-index
column family. No persistent digest state, no incremental maintenance, no memory beyond
`B × 8` bytes. A 100k-key topic scans in tens of milliseconds; the scan yields every N
entries to stay polite on POS CPUs.

### 3.2 Protocol (one round-trip when consistent)

```
child                                        parent
  |-- GET /pipe/consistency/digest?topic=T&watermark=W&buckets=64 -->
  |                                          scans own index, entries with offset ≤ W
  |<- { parentHead, effectiveWatermark, digests[64] } --------------|
  compare against own digests (own scan, all entries ≤ W)
  equal  -> CONSISTENT (1 network call, ~1 KB)
```

Watermark rules:

- `W = child's ingested head for T`. The parent filters its index to entries with
  `latestOffset ≤ W` so the child is never penalised for lag.
- If `parentHead < W` (possible right after a reshuffle: new parent is behind the
  child), the parent clamps to `effectiveWatermark = parentHead` and the child re-digests
  at that watermark. Verdict is then `CONSISTENT_UP_TO(effectiveWatermark)`.

### 3.3 Drill-down (only on mismatch)

For each mismatched bucket (expected: zero or few):

```
  |-- GET /pipe/consistency/bucket?topic=T&bucket=i&watermark=W&cursor=c -->
  |<- page of { keyHash64, latestOffset } pairs + nextCursor ---------|
```

The child diffs the parent's pairs against its own bucket scan and classifies each
difference:

| Case | Meaning | Verdict |
|---|---|---|
| parent has `(k, o≤W)`, child missing `k` | child claims it ingested through W but never stored `k@o` | **INCONSISTENT — missing key** |
| parent has `(k, o≤W)`, child has `(k, o'<o)` | child missed the newer record despite `o ≤ W` | **INCONSISTENT — stale key** |
| child has `k`, parent's entry for `k` has `offset > W` | parent superseded `k` after the child's watermark — pure lag | benign, ignored |
| child has `k`, parent has **no entry** for `k` | parent expired a DELETE tombstone the child never saw (child was offline past tombstone retention) → child holds a **zombie key** forever | **INCONSISTENT — zombie key** |

The last two cases are distinguished with one batched call:

```
  |-- POST /pipe/consistency/classify { topic, keyHashes[] } -->
  |<- per hash: PRESENT_BEYOND_WATERMARK | ABSENT ---------------|
```

The zombie-key case is the detector for the known unrecoverable scenario: a POS offline
longer than tombstone retention can never converge via pipe replay alone. v1 could not
detect this at all; v2 reports it explicitly so an operator (or later, the scheduler) can
trigger a data refresh.

Payload sizes: digest reply ≈ `64 × 8 B + headers` ≈ 1 KB; one bucket page ≈
`(keys/B) × 16 B` (≈ 25 KB for a 100k-key topic), paginated by cursor.

### 3.4 Why bucket digests instead of comparing heads or counts

Offsets are sparse and per-topic interleaved; equal heads prove nothing about interior
gaps (the exact "missed data in between" case). Counts collide trivially. The XOR-bucket
digest detects any single-key difference with probability `1 − 2⁻⁶⁴` and localises it to
a bucket of `keys/B` entries, keeping drill-down traffic proportional to the damage, not
the keyspace.

## 4. Topology, offline, forced-to-cloud

- The check resolves its target at call time: `TopologyManager.getCurrentParentUrl()`,
  or the cloud registry URL when the API is called with `target=cloud`. Reshuffles
  between checks are irrelevant — whoever the parent is *now* is who the child must
  converge toward, and pairwise child→parent checks compose transitively up the
  waterfall to the cloud.
- Parent unreachable (POS offline, parent down) → verdict `UNREACHABLE`, no retries
  beyond the HTTP client's defaults, nothing scheduled. The API is the only trigger.
- The same three endpoints are served by every broker (children query them) **and must
  be implemented by cloud-server** for `target=cloud` and for the top-of-waterfall hop.
  Cross-repo contract — broker-side server + client land first; `MockCloudServer`
  implements the contract for tests.

## 5. API surface

Served to children (and queried on the parent) — same port as existing pipe HTTP:

| Endpoint | Purpose |
|---|---|
| `GET /pipe/consistency/digest?topic&watermark&buckets` | bucket digests at watermark |
| `GET /pipe/consistency/bucket?topic&bucket&watermark&cursor` | (keyHash, latestOffset) page |
| `POST /pipe/consistency/classify` | zombie-vs-lag classification |

Local admin (trigger + results):

| Endpoint | Purpose |
|---|---|
| `POST /admin/pipe-consistency/check?topic=T\|all&target=parent\|cloud` | run a check (single-flight; 409 if one is running) |
| `GET /admin/pipe-consistency/report` | last verdict per topic: state, watermark, missing/stale/zombie key counts (+ first few key hashes), duration, target node |

Metrics (Micrometer): `pipe.consistency.state{topic}` (0 ok / 1 inconsistent / 2
unreachable), `pipe.consistency.missing_keys{topic}`, `pipe.consistency.zombie_keys{topic}`,
`pipe.consistency.last_check_epoch{topic}`, `pipe.consistency.check_duration`.

## 6. Resource budget (proposed defaults — tunable)

| Resource | Cost |
|---|---|
| Steady-state memory / CPU / IO | **zero** — nothing runs unless the API is called |
| Per check, network | 1 request/topic when consistent (~1 KB); + 1 page per mismatched bucket + 1 classify call when not |
| Per check, CPU | one streaming scan of the topic's compaction index on each side (tens of ms per 100k keys), yielding every 10k entries; single-flight, runs on `compactionExecutor` |
| Per check, memory | `B × 8` bytes per side + one bucket page (≤ 64 KB) during drill-down |
| Hot path | **unchanged** — no per-record hashing, no digest maintenance |

## 7. Implementation plan

1. `CompactionIndex.forEachEntry(topic, consumer)` — prefix-scan iteration on
   `RocksDbCompactionIndex` + map iteration on `InMemoryCompactionIndex` (new interface
   method; both impls trivial).
2. `pipe/consistency` package in broker:
   `KeyspaceDigest` (pure function: scan → B digests, watermark filter),
   `PipeConsistencyService` (orchestrates digest/drill-down/classify, builds report),
   `PipeConsistencyController` (the three served endpoints),
   `PipeConsistencyAdminController` (check trigger + report),
   `ParentConsistencyClient` (Micronaut HTTP client against parent/cloud).
3. Single-flight guard + scan yield; reports kept in a bounded in-memory ring (last 16).
4. Config block `pipe.consistency.*`: `enabled` (default **false**, fail-closed like v1),
   `buckets:64`, `scan-yield-every:10000`, `bucket-page-size:2048`, `http-timeout:10s`.
5. Tests: unit (digest math incl. watermark filtering and all four classification
   cases), integration (two real brokers wired parent→child via `BrokerTestApp`),
   journey (compaction on parent only / child only / both + offline-past-retention
   zombie detection), MockCloudServer contract.

## 8. Known limits (accepted)

- Records with a **null msgKey** are not in the compaction index and are outside the
  audit (they're also outside compaction semantics).
- The audit trusts the compaction index as the source of truth for "what this node
  holds". The index is written on the same code path as storage append, and it is
  already load-bearing for delivery filtering; a node with a corrupted index has bigger
  problems, and the audit will (correctly) flag it as inconsistent.
- Key-hash collisions: two distinct keys colliding on 64-bit hash could mask a
  difference within a bucket — probability ≪ 10⁻⁹ at million-key scale.
- v1's per-record CRC "data integrity" goal (bit-rot detection) is explicitly NOT a goal
  of v2; that belongs to the deferred storage-CRC work item.
