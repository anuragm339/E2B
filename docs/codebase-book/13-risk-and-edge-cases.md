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

### POS Refresh And Reconnect

One full journey run retained a reconnected group's pending ACK beyond timeout and never reached READY; isolated rerun passed.

Sources: `ConsumerCrashDuringReplayJourneySpec.groovy`, delivery state classes.

Impact: pipe remains paused and all refresh participants wait.

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

- Modern offsets are next-to-deliver; legacy offsets are last-acknowledged.
- `allConsumersCaughtUp` reads one property representation for both.
- `CommitOffsetHandler` clamps to storage head, not head plus one.
- Registration clamps a corrupt value above head+1 back to head, intentionally allowing replay.

Sources: `BatchDeliveryService.java`, `LegacyConsumerDeliveryManager.java`, `ConsumerRegistry.java`, `CommitOffsetHandler.java`, `ConsumerRegistrationManager.java`.

Impact: off-by-one mistakes can skip or redeliver records.

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
