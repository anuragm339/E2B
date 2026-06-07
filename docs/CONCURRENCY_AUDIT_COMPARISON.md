# Concurrency Audit Comparison

**Reviewed:** 2026-06-07
**Branch:** `feature/pipe-consistency`

## Post-Comparison Remediation

The comparison findings were implemented and verified on 2026-06-07. Both reports are now
historical baselines rather than descriptions of the current implementation.

Resolved items include the segment append race, pipe reconnect lifecycle, compaction overlap,
unbounded executor queues, network/storage deadlines, modern and legacy ACK generations,
delivery/reconnect duplicate scheduling, repository error propagation, Netty ACK registration,
metrics cleanup, and stale integration/journey contracts.

Verification completed:

- full multi-module unit suite passed;
- all integration modules passed;
- all 37 journey tests passed;
- both separate-process system tests passed.

The broader audit remains the authoritative record because it covers the full repository and
records the residual non-blocking-pipeline, soak-test, fatal-policy, and Gradle 9 recommendations.

Compared:

- `readme/CONCURRENCY_ANALYSIS.md`
- `docs/CONCURRENCY_AND_TEST_COMPLETENESS_AUDIT.md`
- current production code, generated Micronaut bean definitions, test sources, executed suites,
  and JaCoCo output

## Executive Verdict

The `readme` analysis is a useful focused review of broker executor ownership and delivery
scheduling. It discovered two important issues that have now been incorporated into the broader
audit:

1. `ConsumerContext.consecutiveFailures` uses non-atomic `volatile int` increment/reset.
2. The modern delivery ACK-timeout sequence can reopen delivery before the timeout has finished
   reverting the previous offset.

The broader audit remains the stronger release assessment because it:

- covers storage, pipe, topology, client, network, repositories, process lifecycle, and test tiers;
- identifies the highest data-integrity and workflow defects;
- executes and reports the actual unit, integration, journey, system, and coverage tasks;
- distinguishes current behavior from proposed acceptance behavior;
- provides a saturation and fault-injection matrix.

The `readme` analysis should not be used as the sole remediation plan. It misses the critical
append race, broken pipe reconnect, overlapping compaction, unbounded queues, unbounded network
waits, topology overlap, repository compound-state races, and Netty ACK-registration race.

## Source Status

`readme/` is ignored by `.gitignore`. `readme/CONCURRENCY_ANALYSIS.md` is therefore not currently
version-controlled. The comparison and broader audit under `docs/` are visible to Git.

## Scope Comparison

| Area | `readme` analysis | Broader audit |
|---|---|---|
| Executor factory/lifecycle | Detailed | Detailed |
| Broker delivery workers | Detailed | Detailed |
| Storage append/rollover | Not analyzed | Detailed |
| Pipe reconnect/topology switch | Misdiagnosed | Detailed |
| Compaction concurrency | Not analyzed | Detailed |
| Network client/server races | Not analyzed | Detailed |
| Client reconnect scheduling | Not analyzed | Detailed |
| Shared state/repositories | Minimal | Detailed |
| Test inventory | Static and stale | Static plus executed results |
| JaCoCo | None | Module and critical-class figures |
| Journey/system verification | Suggested | Executed |
| Stress/fault matrix | Limited | Detailed |

The original brief requested every command/service/worker/future/repository tier. The `readme`
scope names 12 classes and does not satisfy that breadth by itself.

## Valid Findings From the Readme Analysis

### R1. Duplicate Executor Ownership During Shutdown

Status: **Confirmed**

`BrokerService` and `ShutdownCoordinator` both shut down `ackExecutor`. The more serious issue is
ordering: broker ingress remains active while `BrokerService` shuts the ACK pool first.

Correction to the `readme` explanation:

- `AdaptiveBatchDeliveryManager` and `ConsumerDeliveryManager` do not directly submit work to
  `ackExecutor`.
- ACK submissions originate from active network message handling.
- The required order is stop ingress, stop producers/schedulers, drain accepted work, then shut
  down executors.

Severity: **Medium-High**

### R2. `ConsumerContext` Lost Updates

Status: **Confirmed, absent from the broader audit**

`ConsumerContext.incrementFailures()` performs `++consecutiveFailures` on a `volatile int`.
`resetFailures()` also races with increment and with reads of `lastFailureTime`.

The current primary remote-delivery path uses `RemoteConsumer.AtomicInteger`, but
`ConsumerDeliveryManager` still uses `ConsumerContext`. The normal fixed-delay task is serialized,
which limits the race, but duplicate `startConsumerDelivery` calls or concurrent reset/control
operations break that confinement.

Severity: **Medium**, not the repository's first remediation priority.

### R3. `ConsumerDeliveryManager` Check-Then-Schedule

Status: **Confirmed**

`containsKey` followed by scheduling and `put` can create multiple periodic tasks for one
consumer. This also makes the `ConsumerContext` counter race more reachable.

Use an atomic registration operation, but avoid performing scheduler side effects inside a map
`compute` callback without carefully handling rejection and cancellation.

Severity: **Medium**

### R4. Ten-Minute Blocking Storage Wait

Status: **Confirmed and under-emphasized in the broader audit**

`BatchDeliveryService` blocks a `TopicFairScheduler` worker for up to ten minutes while waiting
for a storage executor future. With the effective two-thread defaults, a small number of blocked
deliveries can stop unrelated topic progress.

The fix is not only to reduce the timeout. Required behavior:

- include queue wait in a bounded end-to-end deadline;
- cancel timed-out storage work;
- use bounded queues;
- avoid waiting on one executor from another scarce scheduler where possible;
- expose timeout and queue metrics.

Severity: **High**

### R5. ACK Timeout Can Revert a Newer Delivery

Status: **Confirmed, absent from the broader audit**

The atomic `removePendingOffset` correctly chooses one ACK/timeout winner, but it does not make the
whole state transition atomic.

Valid interleaving:

1. Timeout removes the pending offset and becomes owner.
2. A late ACK observes no pending offset and clears `inFlight`.
3. The delivery scheduler starts the next batch and advances `RemoteConsumer.currentOffset`.
4. The old timeout writes `originalOffset` and clears `inFlight`, clobbering the newer delivery.

A CAS on offset alone is insufficient because pending offset, in-flight state, timeout, trace ID,
and generation belong to one logical delivery. Use an immutable delivery-state generation and
atomic compare/transition.

Severity: **High**

### R6. `TopicFairScheduler` Retry Publication Race

Status: **Confirmed**

The retry is scheduled before it is stored, and running tasks remove entries without generation
matching. The broader audit additionally identified zero-delay execution-before-publication,
newer-entry removal, and unbounded map cardinality.

Severity: **Medium**

### R7. `FlushingPropertiesStore.start()` Is Not Idempotent

Status: **Confirmed**

Multiple starts create multiple periodic flush tasks. No direct spec exists.

Severity: **Low-Medium**

### R8. Deprecated `Thread.getId()`

Status: **Confirmed, low priority**

The workspace runs JDK 21, where `Thread.getId()` is deprecated. The project compiles for Java 17.
Replace it with a dedicated atomic thread-name counter or `threadId()` when the runtime baseline
allows it.

Severity: **Low**

## Incorrect or Overstated Readme Findings

### D1. `execute()` Failures Are Not "Silently Lost"

The `readme` repeatedly states that uncaught failures from `ExecutorService.execute()` disappear.
For an ordinary `ThreadPoolExecutor`, an uncaught runtime failure reaches the worker thread's
uncaught-exception path; the default normally reports it to stderr.

The silent-failure trap is primarily:

- `submit()` when its returned `Future` is ignored;
- `ScheduledThreadPoolExecutor` scheduled tasks, whose failure is retained in the future and
  suppresses later periodic executions;
- task bodies that catch/log and intentionally do not propagate failure state.

A custom `UncaughtExceptionHandler` is useful but is not a complete async-failure strategy and
does not solve scheduled-future failures.

### D2. Catching `Throwable`/OOM Is Not a Recovery Strategy

The `readme` treats not catching `Error`/OOM as a defect and proposes stress-testing continued ACK
operation after OOM.

This is unsafe framing:

- an `OutOfMemoryError` does not "disappear";
- continuing normal service after an arbitrary `Error` may be unsafe;
- logging an OOM can itself allocate and fail;
- fatal JVM errors need a minimal fatal policy, health transition, and usually process
  termination/restart.

Catch expected operational exceptions. Handle fatal errors separately and deliberately.

### D3. `RefreshCoordinator` Injection Is Not Currently Undefined

The generated Micronaut bean definition explicitly invokes the current ten-argument constructor
including `DataRefreshMetrics`. DI is deterministic in the compiled application.

The deprecated public constructor is still maintainability debt and an explicit `@Inject`
annotation would make intent clearer, but this is not a demonstrated runtime concurrency defect.

The proposed reflection test also passes today, so it does not prove the stated failure.

Severity: **Low maintainability**

### D4. The Pipe Scheduler Is Not the Main Pipe Defect

A dedicated single thread running one poll loop is a legitimate executor design, although
`ScheduledExecutorService` is a misleading type for it.

The critical defect is missed:

- `disconnect()` permanently calls `shutdownNow()` on the final scheduler;
- `reconnect()` and topology parent switching reuse the same connector;
- the next `scheduler.execute()` is rejected after state has already been changed.

This is a broken production workflow, not a low-severity thread-utilization issue.

### D5. The Offset Persistence Race Is Not Demonstrated

`currentOffset` is volatile and `persistOffset()` is synchronized. Both callers serialize file
writes and read the current value inside the monitor. A final persist after poll-loop termination
would improve shutdown semantics, but the report does not demonstrate an older write overwriting
a newer persisted value.

Treat this as lifecycle hardening unless a failing interleaving is reproduced.

### D6. ACK Replay Loop Is Not Unbounded Because of `toOffset` Alone

The loop advances to the last returned offset plus one and exits on an empty read. A very large
range can be expensive, but a "misconfigured `toOffset`" does not by itself cause an infinite
loop. Infinite iteration requires the storage implementation to violate its forward-progress
contract.

The actual repository problem is stronger: `RocksDbAckStore.putBatch()` catches
`RocksDBException` and returns success to the caller.

### D7. Effective Pool Defaults Are Wrong

The `readme` uses the factory fallback of four storage threads. `application.yml` supplies two
threads for ACK, storage, and consumer scheduling in the normal runtime configuration.

### D8. "Five Orphan Executors" Is Incomplete

Also missing:

- `TopologyManager` scheduler;
- `ClientConsumerManager` reconnect and health schedulers;
- Netty client/server event-loop groups;
- `LegacyConsumerService` non-daemon event-loop thread.

Centralizing every raw executor in `ShutdownCoordinator` is not automatically correct.
`FlushingPropertiesStore`, for example, must perform its final flush as part of owner-specific
shutdown. Prefer one lifecycle owner per resource and coordinate owner shutdown order.

### D9. One Listed Journey Gap Already Has Coverage

`ConsumerCrashDuringReplayJourneySpec` and `LateConsumerDuringRefreshJourneySpec` cover consumer
loss/rejoin and late joining during refresh. Coverage can still be strengthened, but the scenario
is not wholly missing.

## Test Inventory Accuracy

The `readme` says 210 specs, then lists source-set file counts that sum to 215. The filesystem
does contain 215 Java/Groovy test-source files. Of those, 200 are executable `*Test` or `*Spec`
classes and 15 are fixtures/support files:

| Tier | Test-source files | Executable test/spec classes |
|---|---:|---:|
| Unit | 102 | 102 |
| Integration | 75 | 67 |
| Journey | 33 | 29 |
| Black-box system | 5 | 2 |
| Total | 215 | 200 |

The readme's category counts are therefore valid as source-file counts; only its stated grand
total and its use of "specs" are inaccurate.

Observed suite results from the broader audit:

| Tier | Result |
|---|---|
| Unit | 737 passed |
| Integration | 290 executed, 1 stale-signature failure |
| Journey | 37 passed |
| Black-box system | 2 passed |

The readme performs no JaCoCo analysis. The broader audit records module coverage and shows
especially weak branch coverage in `NettyTcpClient`, `ClientConsumerManager`,
`ConsumerReadinessManager`, and `SegmentManager`.

## Draft Test Quality

The readme labels its tests executable, but most are sketches or invalid regression tests.

| Draft | Assessment |
|---|---|
| `ConsumerContextConcurrencyTest` | Does not compile: `ConsumerContext.builder()` does not exist. The scenario is valid after constructing the real annotation-based object. |
| `ExecutorFactoryUncaughtHandlerTest` | Installs the handler manually on the worker thread, so it does not assert that the factory installed one and should pass today. |
| `BrokerShutdownIntegrationTest` | Asserts only final shutdown state. It cannot prove exactly-once shutdown or ordering; `shutdown()` is idempotent. |
| Storage-starvation test | Contains undefined helpers/classes, uses common-pool callers rather than the real delivery scheduler, and sleeps 35 seconds. It demonstrates blocking only after substantial completion work. |
| Constructor test | Passes today and does not prove Micronaut DI ambiguity. Generated bean code already shows the selected constructor. |
| Fair-scheduler test | Does not create the claimed race. One scheduler thread is blocked, so queued attempts cannot observe a held semaphore; after release, original submissions run legitimately. |
| ACK replay test | Incomplete snippet with undeclared fixtures. It tests a mock throwing, not the real repository swallowing `RocksDBException`. |
| Refresh journey | Pseudocode; equivalent scenarios already exist. |
| Exit-code stress test | Depends on nonexistent helpers and test hooks. Storage saturation should not automatically imply process termination without a defined fatal policy. |

The broader audit's drafts are more complete, but not perfect:

- they require JUnit/Mockito/Awaitility additions to custom unit-test configurations;
- the compaction controller draft mocks the component where the single-flight guard should live,
  so the final test should target a real scheduler/service-level guard;
- the fire-and-forget wrapper should not blindly catch all `Throwable`;
- the process-fatal test requires the proposed failure coordinator before it can be implemented.

## Corrections Applied to the Broader Audit

1. Added `ConsumerContext` increment/reset atomicity to the component matrix.
2. Added the timeout-winner/late-ACK/new-delivery generation race as a high-severity finding.
3. Raised the ten-minute storage wait from medium to high because it occupies scarce fairness
   scheduler threads.
4. Added `Thread.getId()` migration as low-priority JDK 21 cleanup.
5. Corrected `InMemoryInFlightStore` naming to `InMemoryInFlightDeliveryStore`.
6. Narrowed the example async wrapper from blanket `catch (Throwable)` to workflow exceptions plus
   an explicit fatal-error path.
7. Clarified that the mocked compaction-controller draft is not an acceptance test; the final
   regression must exercise a real single-flight scheduler/service guard.

## Findings Missing From the Readme Analysis

| Finding | Severity |
|---|---|
| `SegmentManager.append` can duplicate offsets or race segment rollover | Critical/High |
| `HttpPipeConnector` cannot reconnect after disconnect | High |
| Manual and scheduled compaction can overlap | High |
| Fixed executor queues are unbounded | High |
| Consumer READY/RESET sends block without deadlines | High |
| READY retry can reschedule after ACK cancellation | High/Medium |
| `DeliveryStateStore` compound updates can lose fields | Medium |
| Pending ACK state is split across non-atomic maps | Medium |
| Topology async calls overlap and can complete after shutdown | High/Medium |
| `ClientConsumerManager` schedules duplicate per-topic reconnects | Medium |
| `NettyTcpClient.waitForAck` has an ACK registration race | Medium |
| Netty event-loop shutdown futures are not awaited | Medium |
| `RefreshContext.recordReplayProgress` can regress offsets | Medium |
| `HashCache` has unbounded coalesced waits/self-deadlock risk | Medium |
| RocksDB and property persistence failures are swallowed | Medium/High |
| No performance tests, exit-code failure tests, or thread-leak assertions | Coverage gap |

## Corrected Combined Priority

### P0: Data and Workflow Integrity

1. Make `SegmentManager` active-segment selection, offset allocation, rollover, and append atomic.
2. Split pipe disconnect from bean destruction so reconnect and parent switching work.
3. Make delivery ACK/timeout transitions generation-based and atomic.
4. Add a compaction single-flight guard shared by manual and scheduled entry points.
5. Propagate or health-report RocksDB persistence failure.

### P1: Bounded Concurrency and Lifecycle

1. Replace unbounded fixed-pool queues with bounded queues and workflow-specific rejection.
2. Remove unbounded network `Future.get()` calls from shared scheduler/caller threads.
3. Bound/cancel the ten-minute storage wait or redesign it non-blockingly.
4. Stop ingress before draining and shutting shared executors; assign one owner per executor.
5. Fix READY retry cancellation/rescheduling and topology callback-after-shutdown behavior.
6. Make per-consumer delivery start and reconnect scheduling single-flight.

### P2: Compound State and Secondary Races

1. Make `DeliveryStateStore`, pending ACK, timeout, trace, and from-offset state atomic per key.
2. Fix `NettyTcpClient.waitForAck` registration and early-ACK retention.
3. Fix `ConsumerContext` atomic failure state.
4. Make `RefreshContext.recordReplayProgress` monotonic with `compute`/`merge`.
5. Make `TopicFairScheduler` retries generation-aware and prune inactive keys.
6. Make `FlushingPropertiesStore.start()` idempotent.

### P3: Test and Maintenance

1. Repair the stale `DefaultSegmentFactoryIntegrationSpec`.
2. Add deterministic concurrency regression tests using barriers, controllable executors, and
   fake clocks rather than sleeps.
3. Add process shutdown/exit-code and thread-leak assertions.
4. Add saturation, disk-failure, network-blackhole, and topology-churn performance tests.
5. Add JDK 21 thread naming cleanup and explicit constructor injection for maintainability.

## Relative Scoring

| Criterion | `readme` analysis | Broader audit |
|---|---:|---:|
| Breadth against requested scope | 4/10 | 9/10 |
| Valid concurrency findings | 6/10 | 9/10 |
| Severity prioritization | 4/10 | 8/10 |
| Test inventory/evidence | 3/10 | 9/10 |
| Draft test correctness | 3/10 | 7/10 |
| Readability/actionability | 8/10 | 8/10 |
| Self-contained release decision | 4/10 | 9/10 |

## Recommended Document Strategy

1. Keep `docs/CONCURRENCY_AND_TEST_COMPLETENESS_AUDIT.md` as the evidence-backed baseline.
2. The two confirmed missing findings from the readme are now merged into that audit.
3. Use this comparison as the correction record.
4. Do not retain two independent remediation orders; they will drift.
5. Move any retained `readme` analysis under version-controlled `docs/` or remove it after the
   validated findings are merged.
