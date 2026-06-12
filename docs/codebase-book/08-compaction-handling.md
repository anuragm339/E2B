# Compaction Handling

Related: [Data model](05-data-model.md), [Events](06-event-kafka-flow.md#compaction-event-semantics), [POS state](07-pos-machine-state.md), [Errors/recovery](09-error-retry-recovery.md), [Risks](13-risk-and-edge-cases.md).

## Purpose

Compaction keeps the latest record for a topic/key and eventually removes superseded records from sealed segments. DELETE tombstones are retained for a configured period, then may also be removed when they are still the latest key state.

Sources: `broker/src/main/java/com/messaging/broker/compaction/CompactionIndex.java`, `CompactionRewriter.java`.

## Ingress Indexing

Both ingress paths synchronously call:

```text
CompactionIndex.updateKey(topic, msgKey, offset, timestamp)
```

Sources:

- TCP producer: `broker/src/main/java/com/messaging/broker/handler/DataHandler.java`
- Pipe: `broker/src/main/java/com/messaging/broker/core/BrokerService.java`

Backends:

- RocksDB default: `RocksDbCompactionIndex.java`
- Memory: `InMemoryCompactionIndex.java`

Selected by `compaction-index.backend` in `broker/src/main/resources/application.yml`.

Null keys are not safely compactable and are retained.

## Delivery-Time Filtering

Physical compaction is delayed, so `BatchDeliveryService.applyCompactionFilter` prevents consumers from seeing known stale versions:

1. Fast-path keeps zero-copy if batch starts after the highest stale offset.
2. Otherwise decode records through `StorageEngine.read`.
3. Drop records where `CompactionIndex.isSuperseded` is true.
4. If unchanged, preserve original zero-copy batch.
5. If changed, build a heap-backed `ByteArrayDeliveryBatch`.
6. If all records are filtered, advance/persist the original batch's last offset plus one without a network send.

Source: `broker/src/main/java/com/messaging/broker/consumer/BatchDeliveryService.java`.

The filter fails open if decoding fails or returns fewer records than the file-backed batch advertises. This favors availability but can expose stale versions.

## Scheduled Compaction

`CompactionScheduler.java` is scheduled from:

- `compaction.schedule.initial-delay`
- `compaction.schedule.interval`

It uses:

- an atomic single-flight guard;
- one compaction executor;
- sorted topics;
- `max-topics-per-run`;
- `min-segments-per-topic`;
- process CPU and heap guards.

The default initial delay is 5 minutes and interval 24 hours in `application.yml`.

## Planning

`CompactionPlanner.java` selects a window of sealed segments:

- base offset above the stored checkpoint;
- at most configured window size;
- active segment excluded.

The scheduler skips topics without enough sealed segments.

## Rewrite

`CompactionRewriter.java`:

1. Reads selected segments with `CompactionSegmentReader`.
2. Writes staging output with `CompactionSegmentWriter`.
3. Keeps original offsets.
4. Removes superseded records.
5. Removes expired latest DELETE tombstones.
6. Forces output.
7. Atomically moves staging names to `.compacted.log/.index`.
8. Calls `SegmentManager.replaceSegments`.
9. Updates checkpoint and stale watermark as allowed.

Reader/writer code uses a 256 KB streaming buffer rather than loading an entire segment into memory.

If no records survive, selected segments can be removed without replacement. The code logs that consumers may stall; delivery must rely on gap-aware index/offset behavior.

## Persistent Index And Checkpoints

`SharedRocksDb.java` provides the `compaction` column family.

- Latest key state: `RocksDbCompactionIndex.java`
- Checkpoint: `CompactionCheckpointStore.java`

Checkpoint advancement is withheld when an unexpired tombstone remains in the rewritten window so it can be reconsidered after retention expires.

## Manual Trigger

`POST /admin/compaction/trigger` calls `CompactionScheduler.triggerAsync` with a preparation callback that force-rolls active segments for every topic. Source: `broker/src/main/java/com/messaging/broker/http/CompactionController.java`.

This makes data immediately eligible, but adds:

- filesystem IO;
- more segment files;
- lock interaction with pipe append;
- possible delivery/file-region interaction.

## Recovery Safety

- Staging `.compacting.*` files are not treated as normal segments.
- Final `.compacted.*` pairs are discoverable by recovery.
- Atomic move is used for publication.
- Segment map replacement is protected by a write lock; cleanup occurs after the map swap.

Sources: `DefaultStorageRecoveryService.java`, `SegmentManager.java`, compaction writer/rewriter.

## POS And Refresh Interaction

Compaction is not automatically paused during refresh. Delivery filtering and storage locks are expected to keep replay correct. Relevant journey tests:

- `CompactionDuringRefreshJourneySpec.groovy`
- `RefreshWithCompactedAndActiveSegmentsJourneySpec.groovy`
- `CompactionRaceWindowLegacyDeliveryJourneySpec.groovy`
- `CompactionDeliveryRecoveryJourneySpec.groovy`

The scheduled default may fire during a long initial replay. Existing production incident notes in `BUGS.md` describe this as risky; code currently relies on CPU/heap guards and operator configuration, not replay awareness.

## Performance Impact

- **Memory:** streaming rewrite is bounded, but each decoded record allocates key/data strings; delivery filtering creates heap batches when records are removed.
- **CPU:** full record decode and RocksDB lookups; guarded by process CPU threshold.
- **IO:** reads and rewrites all selected segment bytes, even if little is removed.
- **Network:** no direct network output, but storage locks/read failures can delay delivery.
- **Latency:** write-lock replacement and page-cache churn can increase delivery latency.
- **Compatibility:** offsets are preserved; consumers must tolerate gaps.

Sources: compaction reader/rewriter/scheduler and `BatchDeliveryService.java`.

## Confirmed Gaps And Risks

1. No-op rewrites are not skipped; unique-key segments can be rewritten for zero reclaim.
2. Force-roll can race active pipe writes.
3. Compaction may run during startup replay/refresh.
4. If all records are removed, earliest/head behavior across a missing range is delicate.
5. Delivery filter fail-open may temporarily expose stale data.
6. `StorageEngine.compact()` itself is a no-op; callers must use broker `CompactionScheduler`.

Tests cover many races at small scale, but not production segment sizes or real POS storage pressure. See [Risks](13-risk-and-edge-cases.md#compaction).
