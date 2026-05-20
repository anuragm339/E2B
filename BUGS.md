# Compaction Bugs — Production Incident Analysis

Observed during Docker run on 2026-05-19. All evidence from broker logs timestamped
18:06–18:36 UTC (23:36–01:06 IST).

---

## Bug 1 — `forceRollActiveSegment()` races with pipe mid-batch write

**Severity:** Critical  
**File:** `broker/src/main/java/com/messaging/broker/http/CompactionController.java`

### What happens

`POST /admin/compaction/trigger` calls `sm.forceRollActiveSegment()` for all 24 topics
sequentially on the Netty I/O thread. The pipe connector runs on its own `HttpPipeConnector`
thread and is concurrently streaming a batch record-by-record inside `streamAndHandle()`.
There is no coordination between the two threads. If `forceRollActiveSegment()` seals the
active segment for topic X between two `dataHandler.apply(record)` calls inside the same
batch, the next write attempt on that sealed segment throws `STORAGE_WRITE_FAILED`:
`Segment is not active`. The pipe stops the batch and never writes another record.

### Evidence

```
18:14:12 [default-nioEventLoopGroup-1-2] Force-rolling active segment: deposit baseOffset=1125920
18:14:12 [HttpPipeConnector] ERROR Segment is not active, topic=deposit, baseOffset=1125920
18:14:12 [HttpPipeConnector] ERROR CRITICAL: Failed to store pipe message at offset 1155426, stopping batch
```

Last ever `HttpPipeConnector` log. The pipe wrote no record after offset 1,155,425.

### Why it passes in system tests

Tests call `scheduler.compact()` directly with 512-byte segments containing 6–10 records.
The pipe finishes its tiny batch in milliseconds; `forceRollActiveSegment()` never races
with an active write because the batch is already complete. No test calls
`POST /admin/compaction/trigger` while the pipe is actively streaming a large batch.

### Fix needed

Pause the pipe connector before calling `forceRollActiveSegment()`, resume it after all
topics are sealed. The same `pausePipeCalls` flag already used by the data-refresh workflow
can be reused. Alternatively, `forceRollActiveSegment()` should be a no-op (return early)
if the segment is already sealed, and the pipe's error path should auto-create a new active
segment instead of stopping permanently.

---

## Bug 2 — Compaction permanently kills delivery for all consumers

**Severity:** Critical  
**Files:** `storage/src/main/java/com/messaging/storage/segment/SegmentManager.java`,
`broker/src/main/java/com/messaging/broker/consumer/BatchDeliveryService.java`

### What happens — two stages

**Stage 1: during compaction (47 seconds)**  
`replaceSegments()` holds a write lock for the full duration of segment installation
(close old FileChannels → put new segment → remove old entries). Any `readMessages()` call
on that topic blocks until the write lock releases. With 24 topics compacted sequentially
(~3–4 seconds each) and only 2 `TopicFairScheduler` threads, both scheduler threads are
repeatedly starved. Delivery threads that were mid-read against old segment FileChannels
receive `ClosedChannelException` when the channel is closed under them. These in-flight
batches are silently abandoned — no retry is triggered.

**Stage 2: after compaction (permanent stall)**  
Consumers that resume delivery after the write lock releases send new batches but never
receive `BATCH_ACK` within 60 seconds (`error=null` — TCP send succeeded, consumer went
silent). `BatchDeliveryService` fires `transient_failure` with `pendingOffsetRetained=true`.
With `maxInFlightPerTopic=1` the broker cannot send any new batch for that topic while a
pending offset is held. The retry after `transient_failure` **never fires** — no
`consecutiveFailures=2` is ever logged. Every affected topic is permanently locked.

### Evidence

```
# AckReconciliationScheduler detects in-flight batches from compaction window that never got ACKed:
18:19:52 WARN AckReconciliationScheduler — topic=loss-prevention-product missing=13 offsetRange=[1148488,1148695]
18:19:52 WARN AckReconciliationScheduler — topic=search-product       missing=13 offsetRange=[1154174,1154385]

# Wave 1 — first ACK timeout, retry never fires (no consecutiveFailures=2 ever logged):
18:24:51 ERROR [DELIVERY] Batch failed offset=1154335 topic=location               consecutiveFailures=0→1
18:24:51 ERROR [DELIVERY] Batch failed offset=1151537 topic=loss-prevention-store-configuration  consecutiveFailures=0→1
18:24:51 WARN  batch_delivery.transient_failure pendingOffsetRetained=true ackTimeoutMs=60000

# Wave 2 — different consumers hit the same fate 10 minutes later:
18:34:51 ERROR [DELIVERY] Batch failed offset=1154210 topic=restriction-rules      consecutiveFailures=0→1
18:34:51 ERROR [DELIVERY] Batch failed offset=1151545 topic=loss-prevention-configuration consecutiveFailures=0→1

# Last broker event (excluding topology polls):
18:36:58 compaction_run_finish — then complete silence
```

Delivery does not recover. After 18:36:58 no message is delivered to any consumer again.

### Why it passes in system tests

All compaction journey tests (e.g. `CompactionConcurrentPipeSameKeyJourneySpec`,
`CompactionRaceWindowLegacyDeliveryJourneySpec`) call `scheduler.compact()` **after**
PollingConditions confirms all expected records have been received and ACKed. There are
never any in-flight delivery batches when compaction's write lock fires. The
`TestRecordCollector` auto-ACKs immediately, so ACK timeouts never trigger. No test
verifies that delivery **continues and fully completes** across a compaction boundary when
batches are in-flight at the moment `replaceSegments()` runs.

The `FlakyConsumerJourneySpec` tests transient ACK failures but does not combine them with
concurrent compaction.

### Fix needed

Two independent fixes required:

1. **Lock scope**: `replaceSegments()` should minimise write-lock hold time — close old
   FileChannels outside the lock (they are already removed from the map so no reader can
   reach them). Or use a generation/epoch stamp so delivery threads detect the segment was
   replaced and retry the read against the new segment rather than surfacing an exception.

2. **Retry on `transient_failure`**: After `pendingOffsetRetained=true` the retry must be
   scheduled unconditionally. Currently the retry never fires when the failure path is
   entered from an ACK timeout with a held pending offset under `maxInFlightPerTopic=1`.

---

## Bug 3 — Scheduled compaction initial delay fires during initial replay

**Severity:** High  
**File:** `broker/src/main/resources/application.yml`

### What happens

```yaml
compaction:
  schedule:
    initial-delay: ${COMPACTION_INITIAL_DELAY:5m}
```

On a clean Docker start (fresh volume) the broker begins replaying ~1.155M records from
the cloud server. This replay takes ~29 minutes. The scheduled compaction fires 5 minutes
after boot — exactly when segments are fully loaded with replay data, the pipe is writing
at maximum throughput, and delivery is catching up at full speed. This is the worst possible
timing: it directly triggers Bugs 1 and 2 at peak load.

### Evidence

```
18:07:09 — broker starts, pipe begins replay from offset 0
18:12:47 — first segments hit 100 MB, natural rollover (5.5 min into replay)
18:14:12 — forceRollActiveSegment() fires (7 min into replay, replay 93% complete)
18:14:13 — compaction starts. Replay is still ongoing.
```

### Why it passes in system tests

System tests inject exactly the records they need (6–10 per test), so "replay" completes
in under a second. There is no concept of a long initial replay in the test harness. The
`initial-delay` has no test coverage.

### Fix needed

Increase `COMPACTION_INITIAL_DELAY` to a value well beyond the expected full replay
duration (e.g. `2h`), or gate the scheduled run on a "replay complete" signal from the
pipe connector (e.g. when the pipe receives consecutive empty polls indicating it has caught
up with the cloud server).

---

## Bug 4 — Compaction rewrites 3.1 GB for 0 bytes reclaimed

**Severity:** Medium  
**File:** `broker/src/main/java/com/messaging/broker/compaction/CompactionPlanner.java`

### What happens

The cloud server sends each message key exactly once per cycle — no duplicate keys, no
tombstones. Every record in every segment is already the latest for its key.
`isEligibleForDeletion()` returns false for every record across all 24 topics. Compaction
reads all ~3.1 GB and writes all ~3.1 GB back unchanged. Storage reclaimed: 0 bytes.

### Evidence

```
event=compaction_topic_finish topic=minimum-price  removed=0  reclaimed=0B  bytesRead=130,139,211  bytesWritten=130,139,199
event=compaction_topic_finish topic=prices-v1      removed=0  reclaimed=0B  bytesRead=127,894,395  bytesWritten=127,894,383
... (all 24 topics identical)
```

### Why it passes in system tests

Tests with duplicate keys (`CompactionConcurrentPipeSameKeyJourneySpec`) do validate that
superseded records are removed. However, no test verifies that compaction is **skipped or
short-circuited** when no records are eligible for deletion. The no-op rewrite path is
exercised silently without any assertion.

### Fix needed

`CompactionPlanner.selectDirtyWindow()` or `CompactionRewriter.rewrite()` should check
whether the RocksDB index contains any superseded or expired-tombstone entries for the
candidate segments before starting I/O. If the index shows no eligible records, skip the
rewrite entirely and advance the checkpoint without touching segment files.

---

## Bug 5 — Per-topic compaction causes repeated GC pressure and CPU spikes

**Severity:** Medium  
**File:** `broker/src/main/java/com/messaging/broker/compaction/CompactionRewriter.java`

### What happens

For each of the 24 topics, `CompactionSegmentReader` decodes every record from binary into
a `MessageRecord` with `String key` and `String data`. Even though records are streamed one
at a time, the JVM young generation fills rapidly with short-lived String objects (~20 KB
each for location/product data). G1GC fires repeatedly throughout the 47-second run, each
major collection consuming a full CPU core. Combined with the compaction I/O thread
(Thread-28) doing sequential reads and writes, container CPU reaches 109%.

After compaction, 24 new `Segment` objects plus their open `FileChannel`s and index arrays
remain in heap. Combined with Netty direct buffers for 24 active consumer TCP connections,
container RSS stays elevated at 528 MB even after GC.

### Evidence — heap oscillations across sequential topics

| Topic | Heap start | Heap end | Direction |
|-------|-----------|---------|-----------|
| minimum-price      | 97 MB  | 185 MB | +88 MB  |
| prices-v1          | 52 MB  | 141 MB | +89 MB  (GC ran between) |
| selling-restrictions | 108 MB | 169 MB | +61 MB |
| colleague-card-pin-v2 | 177 MB | 47 MB | −130 MB (major GC) |
| loss-prevention-configuration | 48 MB | **190 MB** | +142 MB ← peak |

Container CPU: 109%. Container RSS: 528 MB / 700 MB limit.

### Why it passes in system tests

Tests use 512-byte segments with 6–10 records. Total GC pressure per compaction run is
negligible — no heap oscillation, no GC events, no CPU spike. The test environment has no
memory or CPU assertions.

### Fix needed

Two independent improvements:

1. **Avoid String allocation for ineligible records**: `CompactionSegmentReader` should
   expose a `skipNext()` path that reads only the header (key length, data length, offset)
   and checks the compaction index before decoding the full payload into Strings. Records
   that are not eligible for deletion can be copied directly from the read `ByteBuffer` to
   the write `ByteBuffer` without any heap allocation.

2. **Explicit GC hint between topics**: Call `System.gc()` (advisory) after each topic's
   compaction completes and before the next begins, to give G1GC a chance to reclaim young
   generation objects during the brief inter-topic gap rather than forcing a major
   collection mid-topic.

---

## System test coverage gaps summary

| Bug | Closest existing test | Why it still passes |
|-----|-----------------------|---------------------|
| 1 — pipe race with forceRoll | `CompactionConcurrentPipeSameKeyJourneySpec` | 512-byte segments; pipe batch finishes in <1 ms; forceRoll never races a live write |
| 2 — delivery permanent stall | `CompactionRaceWindowLegacyDeliveryJourneySpec` | Compact runs after delivery is done; TestRecordCollector auto-ACKs; no in-flight batches at lock time |
| 3 — initial-delay fires mid-replay | None | Test "replay" completes in <1 s; initial-delay never elapses during a test |
| 4 — no-op rewrite not skipped | `CompactionConcurrentPipeSameKeyJourneySpec` | Only tests that duplicates ARE removed; no assertion that compaction is skipped when nothing is eligible |
| 5 — GC pressure | None | 6–10 records per segment; no memory or CPU assertions anywhere |

### Tests needed

- **Bug 1**: `CompactionForceRollPipeRaceJourneySpec` — start pipe streaming a large
  batch (>1000 records), fire `/admin/compaction/trigger` mid-batch, assert pipe
  continues writing all records without a gap.

- **Bug 2**: `CompactionDeliveryRecoveryJourneySpec` — write enough records to fill a
  sealed segment, start delivery, fire compaction while batches are in-flight (do not
  ACK immediately), assert delivery completes to head offset after compaction finishes.

- **Bug 3**: No new test needed — covered by fixing the initial-delay config and adding
  an assertion in `BrokerRestartJourneySpec` that delivery completes before any
  scheduled compaction fires.

- **Bug 4**: `CompactionNoOpSkipJourneySpec` — write unique-key records, run compaction,
  assert that segment files are not rewritten (file modification timestamps unchanged).

- **Bug 5**: No assertion-level test feasible; mitigated by the fix in Bug 4 (skip no-op
  rewrites) and the `skipNext()` path in Bug 5 fix.
