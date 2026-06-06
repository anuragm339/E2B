# PipeConsistency for Broker Lineage

## Summary

Implement PipeConsistency as part of the existing broker consistency model, not as a separate audit subsystem.

The feature maintains periodic consistency state for pipe-ingested data at two levels:

- HOP mode: compare this broker's stored view to its immediate parent for the same offset range. This is the primary, cheap, frequent check.
- DEEP mode: compare the same range by walking parent-by-parent to the root/cloud and identify the first divergent hop. This is infrequent and diagnostic-only.

PipeConsistency is diagnostic only. It never changes topology, parent assignment, or polling source.

Chosen defaults:

- Broker truth model: segments only; brokers do not keep a separate emission log.
- Cloud truth model: existing event table plus a small loopback_emission map for loopback virtual offsets only.
- Root composition: topic-level shortcut uses Merkle over segment hashes in offset order.
- Deep chain discovery: recursive parent walk. Each node already knows its current parent URL from registry/properties; DEEP mode walks hop-by-hop using the current upstream consistency endpoints.
- Rollout: disabled by default until cloud and all broker tiers expose the consistency contract.

## Key Changes

### 1. Hashing and consistency contract

- Add identical RecordHasher implementations in provider/common and ../cloud-server.
- Use:
    - CRC32C per record
    - Murmur3-128 for rolling combine and Merkle inner nodes
- Public contract:
    - recordCrc(offset, msgKey, eventTypeCode, data)
    - combine(prev16, recordCrc)
    - mergeNodes(left16, right16)
    - merkleRoot(leaves)
- Keep the byte format versioned and fixed. Any change must bump the version and force recomputation.

### 2. Broker-side segment truth and lineage

- Extend segment metadata with:
    - segment_hash
    - compaction_epoch
    - hash_record_count
    - hash_state
- Maintain a rolling 16-byte hash on append inside Segment, so sealed segments are O(1) to serve later.
- On seal(), persist the current rolling hash and record count; do not re-read payloads.
- Add SegmentHasher.reconstructFromScratch(...) only for:
    - old pre-feature segments
    - crash recovery when the active segment hash was never finalized
- Compaction recomputes the replacement segment rolling hash while writing survivors and persists compaction_epoch = max(input epochs) + 1.
- Add PipeLineageStore in the broker metadata DB:
    - append-only rows describing which parent URL produced which offset interval
    - close the old lineage row and open a new one whenever the upstream parent changes
    - resolve any [from,to] range into one or more (subrange, parentUrl) tuples
- Do not extend topology for v1. Use normalized parent_url as the durable lineage key. For metrics/reporting, capture upstream_node_id from the upstream consistency response when available; otherwise fall back to the URL.

### 3. PipeConsistency services and endpoints

- Add a broker-side PipeConsistency subsystem under the existing consistency area with:
    - PipeConsistencyChecker
    - PipeConsistencyScheduler
    - UpstreamConsistencyClient
    - PipeConsistencyReport
    - PipeConsistencyReportStore
    - PipeLineageStore
    - bounded HashCache
- PipeConsistencyChecker behavior:
    - discover topic-partitions from storage
    - load or backfill sealed segment hashes
    - snapshot the active segment rolling hash under the segment monitor
    - **clamp the audited range to min(local.maxOffset, upstream.maxOffset)** before requesting any hashes, where upstream.maxOffset comes from GET /pipe/consistency/max-offset; this prevents the perpetual false-mismatch at the head when the parent's ingest lags this broker's view (or vice versa)
    - resolve the requested range through PipeLineageStore
    - in HOP mode, compare against the direct parent for each lineage subrange
        - **fast short-circuit**: before fetching hash, compare local recordCount vs upstream recordCount for the range. If they differ, mark mismatch and go straight to drill-down without computing the upstream hash. Saves the upstream a Murmur fold for the common "obvious gap" case.
        - **stale-lineage handling**: if a lineage subrange's parent_url is unreachable (404 from /pipe/consistency, DNS failure, connection refused), do NOT mark the whole check ERROR. Mark that subrange with status=lineage-stale and continue with the remaining subranges. Emit a separate metric pipe_consistency_lineage_stale_total{topic} and surface the stale parent_url in the report so operators can decide whether to escalate via DEEP.
    - in DEEP mode, walk parent-by-parent to root/cloud and identify the first divergent hop
        - **cycle/loop protection**: maintain a visited-nodeId set during the walk. Cap traversal at pipe.consistency.deep.max-hops (default 8). If the cap is hit or a nodeId repeats, abort with status=deep-walk-aborted and emit pipe_consistency_deep_walk_aborted_total.
    - on mismatch, drill down by segment, then by record page, and classify:
        - missingOnBroker
        - extraOnBroker
        - dataMismatch
    - persist the latest PipeConsistencyReport per (topic, partition, mode)
- Add two endpoint surfaces:
    - Admin endpoints on broker HTTP:
        - GET /admin/consistency/pipe/{topic}/{partition}/root
        - GET /admin/consistency/pipe/{topic}/{partition}/segments
        - POST /admin/consistency/pipe/{topic}/{partition}/run
        - POST /admin/consistency/pipe/{topic}/{partition}/run-deep
        - GET /admin/consistency/pipe/{topic}/{partition}/latest-report?mode=hop|deep
        - GET /admin/consistency/pipe/lineage
    - Pipe endpoints on PipeServer, shared by brokers and cloud:
        - GET /pipe/consistency/hash?topic=&from=&to=&projection=raw|compacted
        - GET /pipe/consistency/range?topic=&from=&to=&page=
        - GET /pipe/consistency/max-offset?topic=
- /pipe/consistency/hash response should include:
    - hash
    - recordCount
    - projection
    - cached
    - source
    - nodeId
    - parentUrl when this node is not root
- PipeConsistencyReport should expose:
    - topic, partition, mode, checked_at, status
    - compared upstream node(s)
    - local root and upstream root(s)
    - mismatched segment summaries
    - first divergent hop for deep mode
    - bounded record-level mismatch details
    - truncated=true when capped

### 4. Broker, cloud, and load behavior

- Brokers do not maintain a separate emission log.
    - /pipe/consistency/* answers come from:
        - persisted segment hashes for sealed segments
        - live rolling hash for the active segment
        - record reads only for partial-range drill-down or lazy backfill
- Topic-level shortcut:
    - build a Merkle root over segment hashes in offset order
    - use only when all compared segments are compaction_epoch == 0
    - if any segment is compacted, compare per segment and require projection=compacted for those ranges
- Cloud-server adds only loopback_emission(virtual_offset, source_offset, emitted_at):
    - required because loopback virtual offsets cannot be reconstructed from the event table alone
    - direct non-loopback rows continue to come straight from event
- Cloud consistency responses are derived from:
    - event
    - loopback_emission join for virtual offsets
    - optional compacted projection over the requested range
- Load protection:
    - HashCache on every node keyed by (topic, from, to, projection)
    - **HashCache invalidation on compaction**: when SegmentManager.replaceSegments(...) runs, invalidate all HashCache entries whose [from, to] overlaps the compacted offset window for that topic. Without this, post-compaction reads can serve stale pre-compaction hashes from cache and produce phantom mismatches.
    - in-flight coalescing for identical hash requests
    - scheduler jitter
    - bounded drill-down
    - 429 handling with the same exponential-backoff style as HttpPipeConnector
- Metrics:
    - pipe_consistency_check_total{result,mode,parent_node_id}
    - pipe_consistency_check_seconds{topic,mode}
    - pipe_consistency_mismatch_segments_total{topic,mode}
    - pipe_consistency_missing_records_total{topic,mode}
    - pipe_consistency_extra_records_total{topic,mode}
    - pipe_consistency_segment_hash_compute_seconds{topic}
    - pipe_consistency_last_check_timestamp_seconds{topic,mode}
    - pipe_consistency_throttled_total{parent_node_id}
    - pipe_consistency_hash_cache_hit_total
    - pipe_consistency_hash_cache_miss_total
    - pipe_consistency_chain_first_divergent_hop{topic}
    - pipe_consistency_lineage_stale_total{topic}            (HOP subrange's parent_url unreachable)
    - pipe_consistency_deep_walk_aborted_total{topic}        (DEEP walk hit max-hops or cycle)
    - pipe_consistency_head_clamped_offsets{topic,mode}      (gauge of records the head-clamp excluded from this cycle)
- Config:
    - broker:
        - pipe.consistency.enabled=false
        - pipe.consistency.schedule.hop-interval=6h
        - pipe.consistency.schedule.deep-interval=24h
        - pipe.consistency.upstream.timeout=30s
        - pipe.consistency.deep.max-hops=8                  (cycle/loop cap for DEEP walk)
        - pipe.consistency.hash-cache.max-entries
        - pipe.consistency.drill-down.batch-size
        - pipe.consistency.drill-down.max-mismatch-report
        - optional endpoint kill switch on PipeServer
    - cloud:
        - pipe.consistency.endpoint.enabled=true
        - pipe.consistency.hash-cache.max-entries
        - pipe.loopback-emission.enabled
        - pipe.loopback-emission.retention-days=30

## Documentation

Add `provider/docs/PIPE_CONSISTENCY.md` (create the `docs/` folder if absent) and link it from the existing `provider/CLAUDE.md` "Important Implementation Notes" section. Also add a brief stanza to `cloud-server/README.md` (or create it) covering the cloud-side changes.

The README must explain how the feature works end-to-end. Required sections:

### 1. Overview
- One-paragraph problem statement: detect whether the broker's pipe-ingested data matches what its upstream actually emitted.
- Diagram (ASCII) of the cloud → ROOT → L2 → LOCAL chain with arrows showing both pipe flow (downward) and consistency checks (HOP=parent only, DEEP=walk to root).
- Diagnostic-only contract: PipeConsistency never reassigns parents or alters polling.

### 2. Concepts
- **Rolling hash**: 16-byte Murmur3-128 maintained on every Segment.append(); persisted at seal(); never re-read.
- **CRC32C per record**: 32-bit signature; safety from offset uniqueness + 128-bit combine.
- **Topic Merkle root**: combine sealed segment hashes in offset order; only valid when all segments are compaction_epoch=0.
- **Compaction epoch**: bumped when a segment is rewritten by compaction; cloud must use `projection=compacted` for those ranges.
- **Pipe lineage**: append-only log of (offset_range, parent_url); resolves an audit range into per-parent subranges.

### 3. Modes
- **HOP** (default, every 6h): cheap, talks only to immediate parent. Uses lineage to route per-subrange. Includes recordCount short-circuit, head-clamp, and lineage-stale fallback.
- **DEEP** (every 24h, or on-demand): walks the chain hop-by-hop using each node's reported `parentUrl`. Identifies the first divergent hop. Bounded by `deep.max-hops` and a visited-nodeId set.

### 4. Data flow walkthrough
- Step through a single HOP audit: discover topics → snapshot active rolling hash → load sealed hashes → clamp to upstream max-offset → split by lineage → fetch upstream hash → compare → drill down on mismatch → persist PipeConsistencyReport.
- Step through a DEEP audit: same setup, then recursive walk via `/pipe/consistency/hash` carrying nodeId+parentUrl in each response.
- Step through compaction: segments merged → epoch bumped → HashCache entries invalidated → cloud serves `projection=compacted` for the new range.

### 5. Endpoint reference
Tables of the new admin and pipe-side endpoints with request/response examples in JSON.

### 6. Operating runbook
- How to enable: flip `pipe.consistency.enabled=true` once cloud and all broker tiers are upgraded.
- How to trigger an on-demand check: `curl -XPOST .../admin/consistency/pipe/{topic}/0/run`
- How to read a report: field-by-field walkthrough of PipeConsistencyReport, with example PASS and MISMATCH bodies.
- How to interpret each metric, especially the new ones: `pipe_consistency_chain_first_divergent_hop`, `pipe_consistency_lineage_stale_total`, `pipe_consistency_deep_walk_aborted_total`, `pipe_consistency_head_clamped_offsets`.
- How to handle each error state: `lineage-stale`, `deep-walk-aborted`, `upstream-unsupported`, generic `error`.

### 7. Configuration reference
Every `pipe.consistency.*` and `pipe.loopback-emission.*` key in one table with default, type, and effect.

### 8. Failure modes and limits
- Pre-feature segments use lazy backfill once (slow first audit).
- Pre-feature lineage is best-effort (single seed row).
- HOP can mark whole ranges stale after rebalancing; DEEP catches the missed truth.
- DEEP traversal cost is per-hop on cold caches; mitigated by HashCache + coalescing.
- Active head is excluded from each cycle (snapshot bound).

### 9. Glossary
HOP, DEEP, lineage subrange, compaction epoch, rolling hash, topic root, projection, drill-down, head-clamp, first-divergent-hop.

## Test Plan

- Shared hasher golden-vector tests in broker/common and cloud.
- Rolling-hash tests proving append-time rolling hash equals standalone folding over the same records.
- Recovery/backfill test for pre-feature segments and crash-before-seal segments.
- Segment metadata migration test for old DBs without hash or lineage columns.
- Pipe lineage tests:
    - parent switch closes the old row and opens the new one
    - range resolution across multiple parent eras
    - cold-start seeding
- Cloud consistency tests:
    - raw event-only range
    - raw loopback virtual-offset range
    - compacted projection with duplicate keys and tombstones
- Broker HOP-mode integration:
    - aligned parent/child returns CONSISTENT
    - missing record on child returns MISMATCH with correct offset/key
    - compacted segment still matches with compacted projection
    - upstream unavailable returns ERROR
    - 429 increments throttled metric and backs off
    - rebalance splits a checked range across multiple lineage rows and routes subranges correctly
    - **head-clamp**: parent lags child by N offsets → check audits only up to parent.maxOffset, surplus reflected in pipe_consistency_head_clamped_offsets
    - **recordCount short-circuit**: counts differ → mismatch reported without an upstream hash request (assert upstream hash endpoint was NOT called)
    - **stale lineage row**: lineage points at a parent_url that returns 404 → that subrange reports lineage-stale, other subranges complete normally, pipe_consistency_lineage_stale_total increments
    - **HashCache invalidation after compaction**: pre-compaction hash served from cache → trigger compaction → next hash request must be a cache miss for the overlapping range
- Broker DEEP-mode integration with at least three in-process tiers:
    - all tiers aligned returns CONSISTENT
    - corrupt parent only: child HOP stays consistent, child DEEP returns mismatch and identifies the first divergent hop
    - **DEEP cycle protection**: misconfigured chain that forms a cycle → walk aborts at max-hops, status=deep-walk-aborted, pipe_consistency_deep_walk_aborted_total increments
    - **DEEP visited-set protection**: same nodeId appears twice in the walk → abort with the same status
- Manual validation:
    - trigger run and run-deep
    - confirm metrics increment
    - confirm persisted reports and lineage view
    - inject mid-tier corruption and verify DEEP classification without any parent switch

## Assumptions and Defaults

- ../cloud-server is a separate deployable repo and must ship together with broker tiers before enabling PipeConsistency.
- Current storage remains partition 0 only; the plan does not generalize routing beyond current behavior.
- Latest PipeConsistencyReport is persisted in broker SQLite per (topic, partition, mode).
- Active segment consistency uses an O(1) rolling-hash snapshot under the segment monitor; appends after the snapshot belong to the next check.
- Sealed segments are O(1) to compare after the first finalized hash; payload reads happen only for lazy backfill or drill-down.
- HOP mode is the default operational signal; DEEP mode is a slower safety net for waterfall corruption.
- Deep checks and lineage over pre-feature historical ranges are best-effort if the broker had data before lineage tracking existed; those ranges fall back to root/cloud comparison when exact parent history is unavailable.
- PipeConsistency is diagnostic only. It never changes topology, parent assignment, or polling source.
