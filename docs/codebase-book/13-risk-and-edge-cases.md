# Risk And Edge Cases

Related: [Tests](10-test-map.md), [Runtime config](11-runtime-config.md), [Debugging](12-debugging-guide.md), [Open questions](15-open-questions.md).

## Critical/High

### Security

- No inbound HTTP authentication implementation was found for admin, diagnostics, compaction, refresh, or `/test` APIs.
- `TestDataController` accepts a filesystem SQLite path and concatenates `tableName` into SQL.
- Outbound auth config names do not match: YAML `broker.registry.auth.bearer-token`, filter `broker.http.auth.token`.

Sources: controllers, `AuthTokenClientFilter.java`, `application.yml`.

Impact: unauthorized refresh/compaction, local file access, SQL identifier injection, operational data exposure.

### Legacy Service Name Contract

RESOLVED 2026-06-11: broker specs now use `price-quote` matching the broker YAML and the
deployed consumer fleet; `:broker:integrationTest` and `:broker:journeyTest` pass. The
underlying rule stands: the `legacy-clients.service-topics` key must exactly equal the
serviceName legacy clients send in RegisterEvent — renaming either side silently disables
registration (client connects but never receives topics).

### Compaction Selecting The Active Segment After Restart (FIXED 2026-06-12)

Recovery used to leave the re-activated last segment in BOTH the sealed-segments map and
`activeSegment`. The first compaction run after a restart (5m initial delay) selected the
LIVE write target into its rewrite window, sealed and deleted it, and every append for the
topic failed forever with `Segment is not active` — which also stalled the entire pipe
stream (single global cursor stops at the first failed record). Fixed three ways:
recovery removes the active segment from the map (invariant: active never in map),
`getInactiveSegments()` filters the active segment defensively, and `replaceSegments()`
refuses the active segment exactly like `removeSegments()` always did. Regression spec:
`SegmentManagerSpec."restart never exposes the re-activated segment to compaction"`.
Operational signature when running an unfixed build: repeating
`CRITICAL: Failed to store pipe message at offset N ... Segment is not active` with the
same offset, starting minutes after a broker restart.

### Compaction Single-Flight Guard Leak On Rejected Offload (FIXED 2026-06-16)

`CompactionScheduler.compactScheduled` claims the `compactionRunning` single-flight guard
(`compareAndSet(false,true)`) and then offloads the run with `compactionExecutor.execute(...)`. The
guard is normally released by `runClaimedCompaction`'s `finally`, but if `execute()` threw
`RejectedExecutionException` the offloaded task never ran, the `finally` never executed, and the
guard stayed `true` — every later run would skip as `already_running` forever, so compaction would
halt and segments/tombstones accumulate (disk fills on a POS device). `triggerAsync` already guarded
this; `compactScheduled` did not. Fixed by wrapping the offload in `try/catch
(RejectedExecutionException)` that resets the guard. Low real-world likelihood — the
`compactionExecutor` is a single-thread executor with an unbounded queue, so `execute()` only
rejects after shutdown — but the guard keeps the flag honest. Regression spec:
`CompactionSchedulerSpec."F2: a rejected scheduled-compaction offload resets the single-flight guard"`.

### Refresh Scheduled-Task Guards (FIXED 2026-06-16)

`RefreshCoordinator` drives four refresh-lifecycle timers on a shared `ScheduledThreadPool(2)`. On a
`ScheduledThreadPoolExecutor`, an uncaught throw from a task silently cancels that task's schedule
(periodic) or skips its re-arm (one-shot) while the pool keeps running — work stops with no crash.
Only `scheduleReplayCheck` guarded its body; the other three did not:

- `retryResetBroadcast` (RESET retry, `scheduleWithFixedDelay`) — a throw cancelled the whole
  periodic schedule → RESET never retried → refresh could stall in `RESET_SENT`.
- `checkReadyAckTimeout` (READY timeout, one-shot self-reschedule) — a throw skipped the
  self-reschedule → stuck `READY_SENT`, no further READY re-sends.
- `abortRefreshIfStuck` (abort watchdog, one-shot + re-arm) — a throw lost the **last-resort abort
  itself** → a stuck refresh never reached a terminal state.

Losing any of these could leave refresh state active until a broker restart. Fixed by guarding each: `retryResetBroadcast`
and `checkReadyAckTimeout` swallow `Exception` around their risky call (the periodic re-run /
self-reschedule then survives), and a new `runAbortWatchdog` wrapper catches a throwing
`abortRefreshIfStuck` and **re-arms** the watchdog for another window (re-arm is refreshId-guarded,
so a completed/replaced refresh does not loop). Regression specs in `RefreshCoordinatorSpec`
(`F1: ...`) assert each timer survives a throwing collaborator and still reschedules/re-arms.

### Consumer ACK Before Processing (FIXED 2026-06-16)

The client acknowledged batches **before** the application processed them, breaking at-least-once.
The network `BatchAckHandler` flushed `BATCH_ACK(topic, group)` the instant a zero-copy batch was
decoded — i.e. before `ClientMessageHandler` forwarded the records and before any handler's
`handleBatch` ran. The broker treats that ack as proof of delivery and commits the next offset, so a
handler that threw (or a consumer that crashed) between decode and processing **silently lost** data
the broker would never resend. The same hazard applied to the refresh control acks: `RESET_ACK`
and `READY_ACK` were sent regardless of whether `onReset`/`onReady` actually succeeded, so a
half-reset or half-activated consumer could let a refresh complete against
state it never applied.

Fixed by moving every client ack to *after* successful processing:

- `network/.../codec/BatchAckHandler` now only unwraps `BatchDecodedEvent` into its record list and
  forwards it — it sends nothing on the wire.
- `ClientConsumerManager.handleDataMessage` sends `BATCH_ACK` (via `sendBatchAck`) only if **every**
  handler's `handleBatch` succeeded; on any failure, or when no handler is registered, it withholds
  the ack and the broker's ack-timeout reverts the offset and redelivers.
- `handleResetMessage`/`handleReadyMessage` withhold `RESET_ACK`/`READY_ACK` when any
  `onReset`/`onReady` throws, so a failed refresh times out and aborts instead of proceeding
  against un-reset consumers.

Regression specs: `ClientConsumerManagerRoutingSpec` (ack sent on success, withheld on handler
failure), `network/.../codec/BatchAckHandlerIntegrationSpec` (unwrap, no outbound ack),
`network/.../tcp/NettyTcpIntegrationSpec` (raw client sends no automatic BATCH_ACK). Operational
signature on an unfixed build: data marked delivered/committed on the broker that the consumer
never actually applied, with no redelivery — i.e. silent gaps after a handler exception or consumer
crash.

### Segment Lifecycle Audit (2026-06-12) — five more state-machine bugs FIXED

Found by a systematic walk of every active/sealed/compact/replace/recover transition after
the active-segment-compaction bug:

1. **Same-base re-compaction data loss (CRITICAL):** an unadvanced checkpoint (unexpired
   tombstones) re-selects the already-compacted segment; the rewrite ATOMIC_MOVEs onto the
   SAME `.compacted.log` path, and cleanup then unlinked the just-installed file and erased
   its metadata row. Fixed: `cleanupDetachedSegment(deleteMetadata, deleteFiles)` skips
   exactly the artifacts the replacement shares (base ⇒ metadata, path ⇒ files).
2. **Empty force-roll same-file twin (HIGH):** the `size==0` skip never fired (fresh
   segments are 6 header bytes), so force-rolling an idle topic sealed the active segment
   into the map AND reopened the SAME file as the new active — compacting the twin unlinked
   the live file. Fixed: skip by `recordCount==0` + a hard same-base/empty guard in
   `rollSegment`.
3. **In-flight zero-copy vs close (HIGH operational):** compaction closed the segment's
   channel while Netty was still sendfile-ing from it (read lock covers batch construction
   only) → `ClosedChannelException`, consumer connection resets every compaction. Fixed:
   batch leases — `Segment.close()` defers the channel close until the last
   `BatchFileRegion.close()` (Netty always deallocates); unlink-while-open is POSIX-safe.
4. **Recovery double-load (MEDIUM):** with both `X.log` and `X.compacted.log` on disk
   (crash between finalise and cleanup), both were loaded and the second silently shadowed
   the first in the map, leaking channels. Fixed: dedupe by base preferring `.compacted.log`
   with a loud warning.
5. **Lock-discipline + JMM hygiene (LOW):** `forceRollActiveSegment` rolled without the
   segment write lock (readers could observe half-rolled state) — the lock now lives inside
   `rollSegment` (reentrant); `Segment.nextOffset/logPosition/recordCount/active` are now
   volatile (they are read lock-free by delivery threads).

Regression specs: `SegmentManagerSpec` (restart/compaction exposure, empty force-roll,
same-base replacement), `SegmentSpec` (in-flight transfer survives close),
`DefaultStorageRecoveryServiceSpec` (duplicate-base dedupe).

### Full Concurrency Sweep (2026-06-12) — mode-combination matrix

Systematic pass over normal × refresh × compaction × consistency interactions, after the
segment-lifecycle fixes. Verified safe by design (with the load-bearing mechanism):

| Combination | Why safe |
|---|---|
| consistency scan × concurrent ingest | RocksDB iterators are snapshot views; child digests BEFORE calling parent so races degrade to benign drill-down classifications |
| consistency × compaction (either side) | digest input (key→latestOffset) is compaction-invariant; classify reads go through the segment read lock + batch leases |
| consistency × refresh | disjoint state — consistency reads the compaction index only; refresh wipes ACK store and pauses reconciliation, neither used by checks |
| consistency × consistency (many children → one parent) | server-side scan semaphore (429 above 2), single-flight on each child |
| compaction × manual trigger × consistency check | all three serialize on the single-threaded `compactionExecutor` |
| refresh replay × compaction swap | replay reads under the segment read lock; in-flight zero-copy protected by batch leases |
| pipe ingest × producer ingest same topic | `CompactionIndex.updateLock` serializes read-modify-write; appendLock serializes segment writes |
| shutdown × periodic flush | `flush()` and `close()` synchronize on the Segment; seal flips `active` before channel close so a racing flush no-ops |
| delivery × ACK × timeout | generation-gated `InMemoryInFlightDeliveryStore` (striped locks; claims validate generation AND pending offset) — hardened earlier, re-verified |

**Fixed this sweep:** `CompactionScheduler.compact()` and `AckReconciliationScheduler`
ran their minutes-long bodies INLINE on Micronaut's shared 4-thread scheduled pool — the
same pool as the 1-second segment-fsync flusher. On a small POS (pool = 2×cores) a
compaction sweep plus a first-run reconciliation could starve the flusher, silently growing
the power-cut data-loss window from 1s to minutes. Both now offload to the
single-threaded `compactionExecutor` (which also serializes them with consistency checks).

Known residual (accepted, documented): a parent-side compaction racing a `classify`
physical-presence read can transiently answer "absent" → one-off zombie false positive;
the next scheduled check self-corrects. Legacy delivery reads index FILES directly and can
lose a topic for one merged batch during a compaction swap (retries next poll).

### Compaction Index Is Derived And Has No Rebuild

The pipe-consistency audit (and delivery-time superseded filtering) trust the RocksDB
compaction index. There is no rebuild-from-segments mechanism: a wiped or corrupted index
with intact segments makes the node deliver superseded records and report falsely
inconsistent against any peer. Failure mode is loud, not silent. Follow-up: an offline
rebuild/spot-check admin tool reusing `CompactionSegmentReader`.

Segment↔index divergence windows on the pipe ingest path (`handlePipeMessage` writes
segments then the index then returns true → pipe offset advances) — two of three closed:

| Window | Status |
| --- | --- |
| `RocksDbCompactionIndex.updateKey` swallowed `RocksDBException` → ingest "succeeded", record in segments but never indexed | CLOSED — updateKey throws a structured `StorageException` (`STORAGE_METADATA_ERROR`) via `ExceptionLogger.logAndThrow`; ingest returns false, pipe retries the record (`CompactionIndex.updateKey` now declares `throws StorageException`; implementations MUST throw on backend failure) |
| Crash (or lost RocksDB WAL tail on power cut) between `append` and `updateKey` → parent re-sends, dedupe branch skipped the index write forever | CLOSED — the duplicate-skip branch now heals the index (`updateKey` is idempotent/monotone, no-op for genuine duplicates) |
| At-rest RocksDB loss/corruption with intact segments | OPEN — fails loud via the consistency check; rebuild tool backlogged (above) |

Producer `DataHandler` path with throwing updateKey is safe: no ACK → producer retries →
the retried append gets a new offset that supersedes the orphan.

### POS Refresh And Reconnect

One full journey run retained a reconnected group's pending ACK beyond timeout and never reached READY; isolated rerun passed.

Sources: `ConsumerCrashDuringReplayJourneySpec.groovy`, delivery state classes.

Impact: the refreshed topic remains active/non-green and all refresh participants wait; upstream pipe polling is not paused by the local consumer refresh itself.

### Compaction

- Scheduled compaction can run during long replay/refresh.
- Manual force-roll can race pipe append.
- Rewriter can rewrite large unique-key windows for zero reclaimed bytes.
- Segment replacement interacts with in-flight file regions.
- All-records-deleted windows create large offset gaps.

Sources: compaction package, `CompactionController.java`, `SegmentManager.java`, `BUGS.md`.

## Medium

### Pipe And Topology

- Connector omits `topic`; local PipeServer defaults `price-topic`.
- Only the first parent is selected.
- Node ID query URL encoding was not observed.
- `PipeMessageForwarder.getCurrentOffset()` returns `0`.
- Probe requires `/health`; journey mock returned 404 and retained old/no parent.

Sources: `HttpPipeConnector.java`, `PipeServer.java`, `TopologyManager.java`, `PipeMessageForwarder.java`.

### Offset Semantics

- Modern offsets are next-to-deliver; legacy offsets are last-acknowledged. Full table and per-site analysis: [Data model — Offset conventions](05-data-model.md#offset-conventions-legacy-vs-modern--dual-convention).
- `allConsumersCaughtUp` reads one property representation for both. It is written for the **legacy** convention (`offset == head` ⇒ caught up), which is correct for the deployed fleet. **Verdict (reviewed 2026-06-16): do NOT "consistency-fix" it toward next-to-deliver (`<= head`).** A legacy offset caps at `head`, so that would make caught-up unreachable and **stall refresh completion forever**. Same reasoning blocks clamping registration to `head + 1`.
- Captured refresh-window checks are consumer-type aware: legacy waits for `target`, modern waits for `target + 1`. If a consumer is disconnected and cannot be typed, the code defaults to legacy semantics to avoid wedging the deployed fleet.
- `CommitOffsetHandler` clamps to storage head, not head plus one.
- Registration clamps a corrupt value above head+1 back to head, intentionally allowing replay — the at-least-once-safe floor (legacy: exactly caught-up; modern: at most one duplicate, never a skip).

Sources: `BatchDeliveryService.java`, `LegacyConsumerDeliveryManager.java`, `ConsumerRegistry.java`, `CommitOffsetHandler.java`, `ConsumerRegistrationManager.java`.

Impact: off-by-one mistakes can skip or redeliver records. The modern path is off-by-one only at the single boundary `offset == head` (metrics undercount by 1; caught-up declared one record early) — cosmetic today because the fleet runs legacy. A true unification is a behavioral change to legacy ACK persistence/delivery/recovery, not a comment cleanup.

### Storage Integrity

- Current record format has no per-record CRC.
- Non-monotonic externally supplied offsets log a warning but append.
- `StorageEngine.compact()` and `validateStorage()` are placeholders.
- Real filesystem atomicity/power-loss behavior is not verified.

Sources: storage segment and engine classes.

### Shutdown

Integration logs showed a second `NettyTcpServer.shutdown` call during bean disposal after explicit broker shutdown, causing `RejectedExecutionException`.

Impact: noisy shutdown and possible incomplete cleanup in other failure orderings.

### Delivery Filter Fail-Open

Compaction filtering returns the unfiltered batch on decode/index errors.

Impact: stale keyed values may be delivered temporarily.

### HTTP Error Semantics

Most controllers return error maps with HTTP 200 rather than typed 4xx/5xx responses.

Impact: automation must inspect response body and may treat failures as success.

## Lower/Operational

- `GET /admin/refresh-current` reports only one arbitrary active topic.
- `GET /test/stats` is placeholder data.
- Client invokes all handlers for a topic, including multiple groups, for each decoded connection.
- READY retry stops after three attempts; registration remains but delivery stays blocked.
- Reconciliation scans active registered groups only.
- Properties files rewrite the full map; large group/topic counts increase IO.
- Metrics cleanup keys may differ from ephemeral client IDs; code attempts group/topic cleanup.
- Gradle uses deprecated features incompatible with Gradle 9.
- Qodana requests JDK 21 while project source target is 17.
- Docker build skips tests.

## Performance Guardrail Matrix

| Area | Existing guard | Residual risk |
|---|---|---|
| Memory | 256 KB batch default, streaming pipe, chunked ACK writes, streaming compaction | Heap replacement batches and decoded compaction strings |
| CPU | adaptive polling, fairness, compaction CPU threshold | Large topic count and full compaction scans |
| IO | 1s group commit, page-cache advice, atomic properties, bounded windows | No-op rewrites and SQLite/property rewrites |
| Network | send timeout, channel writability, one connection/topic/group | Many groups create many sockets |
| Latency | storage-read timeout, ACK timeout, scheduler backoff | Refresh and compaction lock interactions |
| Compatibility | legacy adapter, old index recovery | Service-name and offset-semantic drift |
| Compaction safety | staging/atomic move, offsets preserved, delivery filter | In-flight region lifetime and all-deleted ranges |
| POS safety | READY/RESET gates, persisted refresh, reconnect | Flaky stale pending ACK and abort policy |

## Change Hotspots

Changes in these files require broad tests:

- `Segment.java`, `SegmentManager.java`
- `NettyTcpServer.java`, `ZeroCopyBatchDecoder.java`
- `BatchDeliveryService.java`, `BatchAckService.java`
- `ConsumerRegistry.java`, `InMemoryInFlightDeliveryStore.java`
- `RefreshCoordinator.java`, phase services
- `CompactionRewriter.java`, `CompactionScheduler.java`
- `HttpPipeConnector.java`, `TopologyManager.java`
- `ClientConsumerManager.java`

Use the feature-to-test links in [Test map](10-test-map.md).
