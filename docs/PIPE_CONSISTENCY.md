# PipeConsistency

Periodic audit that proves whether a broker's pipe-ingested data matches what its
upstream actually emitted. **Diagnostic only — never changes topology, parent
assignment, or polling source.**

## 1. Overview

The system is a tree topology where data flows downward through the pipe (cloud →
ROOT broker → L2 broker → … → LOCAL broker). Without a defensive audit there is
no way to detect that the pipe dropped a record or that an intermediate broker
silently has bad data.

PipeConsistency adds two complementary checks:

```
   cloud
     │  pipe poll          DEEP audit (walks all hops)
     ▼
   ROOT broker  ────────►  HOP audit (only one hop up)
     │
     ▼
   L2 broker    ────────►  HOP audit (only one hop up)
     │
     ▼
   LOCAL broker
```

Each broker exposes the same `/pipe/consistency/*` contract that cloud exposes,
so the same code can audit any hop uniformly.

## 2. Concepts

- **Rolling hash** — a 16-byte Murmur3-128 maintained on `Segment.append()`.
  Sealed segments persist the final value and become O(1) to serve later. Never
  re-read from disk in the steady state.
- **CRC32C per record** — 32-bit per-record signature. Safety from the unique
  `offset` baked into the hash material plus the 128-bit combine.
- **Topic Merkle root** — combine sealed segment hashes in offset order via
  `RecordHasher.mergeNodes`. Used only when every sealed segment is
  `compaction_epoch == 0`.
- **Compaction epoch** — bumped to `max(input epochs) + 1` whenever
  `CompactionRewriter` produces a merged segment. Audits with mixed epochs go
  per-segment with `projection=compacted`.
- **Pipe lineage** — append-only log of `(offset_range, parent_url)` rows.
  Resolves an audit range into per-parent subranges so HOP routes each subrange
  to the correct upstream after a rebalance.

## 3. Modes

| Mode | Default cadence | What it does | Cost |
|------|-----------------|--------------|------|
| **HOP** | every 6h | Compare against the immediate parent for each lineage subrange | 1 small request per topic when caches are warm |
| **DEEP** | every 24h | Walk parent-by-parent toward root/cloud, pinpoint first divergent hop | One request per hop per topic; bounded by `deep.max-hops` |

HOP also short-circuits on `recordCount` mismatch (no hash request needed),
clamps to `min(local.max, upstream.max)` so a lagging parent doesn't cause
phantom mismatches, and falls back to `LINEAGE_STALE` for any subrange whose
historical parent is unreachable.

DEEP uses a visited-`nodeId` set and a hop-count cap to break topology cycles.

## 4. Data flow walkthrough

### HOP audit (one topic)

1. Discover topics from `StorageEngine.getTopicNames()`.
2. Snapshot the active segment rolling hash under the segment monitor.
3. Load sealed segment hashes from `segment_metadata`.
4. For each lineage subrange:
   1. `GET parent/pipe/consistency/max-offset?topic=` → clamp `to` to that value.
   2. If `local.recordCount != upstream.recordCount` → MISMATCH, drill down.
   3. Else `GET parent/pipe/consistency/hash?topic=&from=&to=&projection=`.
   4. Compare hashes; on mismatch drill down by record page.
5. Build `PipeConsistencyReport` and persist via `PipeConsistencyReportStore`.

### DEEP audit (one topic)

1. Compute local hash once.
2. Walk parents: ask each one for the hash of the same `[from, to]` range.
3. Each response carries `nodeId` and `parentUrl` so the walk knows where to go
   next.
4. First hop whose returned hash differs from local → `firstDivergentHopNodeId`.
5. Stops on root (`parentUrl=null`), max-hops cap, or visited-`nodeId` cycle.

### Compaction interaction

```
seg[100-199] hash=AA  ┐
seg[200-299] hash=BB  ├─► compaction ─► seg[100-299] hash=CC, epoch=1
seg[300-399] hash=CC  ┘
                                          │
                                          ▼
                            HashCache.invalidateOverlapping(topic, 100, 299)
```

`CompactionRewriter` accumulates the merged segment's rolling hash while writing
survivors, bumps the epoch, and tells the HashCache to drop any cached entries
overlapping the compacted window. Cloud-server hashes for the same range must be
requested with `projection=compacted`.

## 5. Endpoint reference

### Operator (broker-only)

| Method | Path | Purpose |
|--------|------|---------|
| GET | `/admin/consistency/pipe/{topic}/{partition}/root` | Local Merkle root + max offset |
| GET | `/admin/consistency/pipe/{topic}/{partition}/segments` | Sealed segment summaries |
| POST | `/admin/consistency/pipe/{topic}/{partition}/run` | Trigger on-demand HOP audit |
| POST | `/admin/consistency/pipe/{topic}/{partition}/run-deep` | Trigger on-demand DEEP audit |
| GET | `/admin/consistency/pipe/{topic}/{partition}/latest-report?mode=hop\|deep` | Latest persisted report |
| GET | `/admin/consistency/pipe/lineage` | Dump `pipe_lineage` rows |

### Upstream (broker AND cloud expose identically)

| Method | Path | Purpose |
|--------|------|---------|
| GET | `/pipe/consistency/hash?topic=&from=&to=&projection=raw\|compacted` | Hash + recordCount over the range |
| GET | `/pipe/consistency/range?topic=&from=&to=&cursor=&pageSize=` | Records in range for drill-down. **Cursor-paged**: omit `cursor` for the first page; pass the previous response's `nextCursor` to fetch the next page. Empty response = no more records in `(cursor, to]`. |
| GET | `/pipe/consistency/max-offset?topic=` | Highest emitted offset on this node |

`hash` response shape:
```json
{
  "hash": "1234abcd...",
  "recordCount": 1500,
  "projection": "raw",
  "cached": true,
  "source": "rolling",
  "nodeId": "broker-002",
  "parentUrl": "http://broker-001:9092",
  "from": 0,
  "to": 1499,
  "topic": "prices-v1"
}
```
Cloud always returns `"parentUrl": null` and `"source": "derived"`.

## 6. Operating runbook

### Enable the feature

Once cloud-server **and** every broker tier is upgraded to a build that contains
the contract, flip:
```bash
PIPE_CONSISTENCY_ENABLED=true
```
The scheduler picks it up on the next cycle. Until then, the feature is dormant
and all metrics stay at zero.

### Trigger an on-demand check

```bash
curl -XPOST http://broker:8081/admin/consistency/pipe/prices-v1/0/run
curl http://broker:8081/admin/consistency/pipe/prices-v1/0/latest-report?mode=hop
```

### Reading a report

| Field | Meaning |
|-------|---------|
| `status` | `consistent` / `mismatch` / `error` / `lineage_stale` / `deep_walk_aborted` |
| `localRoot` / `upstreamRoot` | Hex Murmur3-128 over the audited range |
| `mismatchedSegments` | One entry per drifted segment with both hashes |
| `missingOnBroker` / `extraOnBroker` / `dataMismatch` | Capped record-level differences |
| `firstDivergentHopNodeId` | DEEP only — which hop introduced the divergence |
| `truncated` | True when capped by `drill-down.max-mismatch-report` |

### Metrics

All Prometheus metric names live under `pipe_consistency_*`. The ones to alert on:

| Metric | Meaning |
|--------|---------|
| `pipe_consistency_check_total{result="mismatch",mode,parent_node_id}` | Drifts found |
| `pipe_consistency_check_total{result="error"}` | Upstream unreachable / DB failure |
| `pipe_consistency_chain_first_divergent_hop{topic}` | Stable hash of the bad hop's nodeId |
| `pipe_consistency_throttled_total{parent_node_id}` | Upstream returned HTTP 429 |
| `pipe_consistency_last_check_timestamp_seconds{topic,mode}` | Liveness — should keep advancing |

### Error states

| Status | What it means | What to do |
|--------|---------------|------------|
| `lineage_stale` | A historical parent URL is unreachable | Run DEEP — divergence may still be visible via the chain root |
| `deep_walk_aborted` | DEEP hit `deep.max-hops` or a topology cycle | Inspect topology; ensure parentUrls don't loop |
| `error` | Upstream consistency endpoint unavailable | Check the parent broker / cloud is upgraded and reachable |
| `mismatch` | Hash differs after recordCount + projection checks | Read drill-down arrays; cross-check with /pipe/consistency/range |

## 7. Configuration reference

### Broker (`provider/broker/src/main/resources/application.yml`)

| Key | Default | Effect |
|-----|---------|--------|
| `pipe.consistency.enabled` | `false` | Master kill switch |
| `pipe.consistency.schedule.hop-interval` | `6h` | HOP cadence |
| `pipe.consistency.schedule.initial-delay` | `10m` | First HOP run delay after startup |
| `pipe.consistency.schedule.deep-interval` | `24h` | DEEP cadence |
| `pipe.consistency.schedule.deep-initial-delay` | `30m` | First DEEP run delay after startup |
| `pipe.consistency.upstream.timeout` | `30s` | Per-request HTTP timeout to upstream |
| `pipe.consistency.deep.max-hops` | `8` | Cycle/loop cap for DEEP walk |
| `pipe.consistency.hash-cache.max-entries` | `10000` | LRU bound for cached range hashes |
| `pipe.consistency.drill-down.batch-size` | `1000` | Records per drill-down page |
| `pipe.consistency.drill-down.max-mismatch-report` | `100` | Cap on record-level mismatches reported |
| `pipe.consistency.endpoint.enabled` | `true` | Per-PipeServer kill switch for `/pipe/consistency/*` |

### Cloud (`cloud-server/src/main/resources/application.yml`)

| Key | Default | Effect |
|-----|---------|--------|
| `pipe.consistency.endpoint.enabled` | `true` | Kill switch for cloud `/pipe/consistency/*` |
| `pipe.consistency.hash-cache.max-entries` | `10000` | LRU bound for cached derived hashes |
| `pipe.loopback-emission.enabled` | `true` | Persist `(virtual_offset, source_offset)` for loopback rows |
| `pipe.loopback-emission.db-path` | `/data/loopback_emission.db` | SQLite path |
| `pipe.loopback-emission.retention-days` | `30` | Sweep window for old loopback entries |
| `cloud.node-id` | `cloud-root` | NodeId advertised in `/pipe/consistency/hash` responses |

## 8. Failure modes and limits

- **Pre-feature segments** use lazy backfill — first audit pays a one-time re-scan
  via `SegmentHasher.reconstructFromScratch`.
- **Pre-feature lineage** is best-effort: a single open seed row is inserted at
  startup pointing at the current parent. Audits over older ranges fall back to
  comparing against root/cloud.
- **HOP after rebalance** can mark whole subranges `LINEAGE_STALE` if the old
  parent is decommissioned. DEEP still works because it goes to the chain root.
- **Cold HashCache + simultaneous DEEP from many brokers** would collapse via
  in-flight coalescing (one compute, all callers share) so cloud serves the
  group at the cost of one fold.
- **Active head excluded** — each audit clamps `to` at the snapshot offset taken
  under the segment monitor; appends after the snapshot belong to the next cycle.

## 9. Glossary

| Term | Meaning |
|------|---------|
| HOP | One-hop audit against the immediate parent |
| DEEP | Multi-hop audit walking to root/cloud |
| Lineage subrange | An offset range belonging to one historical parent |
| Compaction epoch | Number of times this segment range has been rewritten by compaction |
| Rolling hash | Per-segment 16-byte Murmur3-128 built incrementally at append time |
| Topic root | Merkle root over a topic's sealed segment hashes |
| Projection | `raw` (every row) vs `compacted` (latest per key in range) |
| Drill-down | Record-page diff after a hash mismatch |
| Head-clamp | Capping the audit range at `min(local.max, upstream.max)` |
| First-divergent-hop | The nodeId of the highest hop whose hash diverges from this broker's |
