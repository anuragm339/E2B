# Concurrency and Test Completeness Audit

**Audit date:** 2026-06-07
**Branch:** `feature/pipe-consistency`
**Java:** Java 17 target, audited on JDK 21.0.7; Micronaut 4.2.1, Gradle 8.5
**Scope:** `broker`, `client`, `common`, `network`, `pipe`, `storage`, and `test-consumer`

## Remediation Update

**Implementation completed:** 2026-06-07

All confirmed implementation defects identified by this audit and the compared `readme`
analysis were fixed. The detailed findings below are retained as the pre-fix baseline.

Key completed changes:

- serialized segment selection, offset assignment, rollover, and append without mutating caller records;
- separated pipe disconnect from bean destruction and made reconnect generation-safe;
- added generation-based atomic state for both modern and legacy ACK/timeout workflows;
- made compaction preparation and execution single-flight;
- replaced unbounded fixed-pool queues with configurable bounded executors and explicit rejection;
- added deadlines, cancellation, and interrupt restoration to storage and network waits;
- centralized broker executor shutdown after ingress and producer shutdown;
- made reconnect, delivery start, refresh progress, persistence updates, and lifecycle starts atomic;
- propagated RocksDB and properties persistence failures instead of reporting false success;
- bounded early ACK retention and per-key delivery synchronization state;
- deregistered per-consumer Micrometer meters instead of leaving `NaN` gauges/cardinality leaks;
- normalized JVM process CPU readings across fractional and percentage formats;
- added direct concurrency, rejection, timeout, reconnect, persistence, and stale-generation tests.

Post-remediation verification:

| Tier | Result |
|---|---|
| Unit | `./gradlew test` passed across all modules |
| Integration | All module integration suites passed; the stale `DefaultSegmentFactory` contract test was corrected |
| Journey | All 37 broker journey tests passed |
| System | Both separate-process broker system tests passed |
| Focused stress | Bounded queue rejection, concurrent append, ACK registration, stale timeout generation, and concurrent state-update tests passed |

Residual engineering recommendations are not confirmed correctness defects:

- replace the bounded cross-executor storage wait with a fully non-blocking delivery pipeline;
- add a dedicated long-running `performanceTest`/soak source set and disk/network fault injection;
- define an application-wide fatal worker policy if the deployment requires a non-zero process exit
  after an unrecoverable background failure;
- remove or bound per-topic fairness metadata if topic names can be attacker-controlled or unbounded;
- resolve Gradle deprecations before upgrading to Gradle 9.

## Executive Verdict

This repository has better-than-average journey coverage for refresh, replay, restart,
compaction, and consumer failure workflows. It also contains several deliberate concurrency
improvements: named executor beans, separate storage and ACK-storage pools, concurrent
collections, refresh generation checks, CAS-based transition claims, and shutdown hooks.

It is not yet safe to call the implementation explicitly thread-safe under saturation.
The highest-risk defects are:

1. `SegmentManager.append()` does not atomically select the active segment, assign the next
   offset, and append. Concurrent producers can select the same offset or append to a segment
   that another thread has just sealed.
2. `HttpPipeConnector.disconnect()` permanently shuts down its final scheduler. `reconnect()`
   and topology parent switching then try to reuse the terminated scheduler.
3. Manual and scheduled compaction can execute concurrently against the same segment windows.
4. The fixed executor pools have unbounded queues, so saturation becomes unbounded memory
   growth instead of controlled rejection or backpressure.
5. Several shared scheduler tasks call `Future.get()` without a timeout. A stalled network
   future can consume every consumer scheduler thread.
6. An ACK timeout can claim an old delivery, then a late ACK can reopen the gate for a new
   delivery before the old timeout rolls the consumer offset back. The rollback is not protected
   by an immutable delivery generation.
7. Background task failures are frequently logged and swallowed. There is no application-level
   fatal-error policy capable of producing a non-zero process exit after a critical worker fails.

**Overall concurrency risk: High**
**Overall readability: Medium**
**Unit coverage completeness: Medium-Low for concurrency contracts**
**Integration coverage completeness: Medium**
**Journey coverage completeness: High for domain workflows, Low for executor failure paths**
**System/stress coverage completeness: Low**

## Important Scope Clarification

This is a Micronaut broker/server process, not a Picocli command application. There are no
command classes or argument-to-worker command objects. The effective command boundaries are:

- `Application.main()` and Micronaut startup/shutdown
- HTTP controllers such as `CompactionController`
- TCP handlers such as `BatchAckHandler`
- the separate `test-consumer` process

Therefore, "CLI exit code" observations in this audit refer to the Java process exit status and
black-box process tests, not command-specific Picocli exit codes.

## Evidence Collected

- 215 test/spec source files were found.
- No `performanceTest` source files were found.
- The broker has 144 main source files and 128 test/spec files.
- `test-consumer` has 27 main source files and no direct tests.
- The suite contains two separate-process black-box specs.
- There are 31 `Thread.sleep` occurrences in test sources. Polling/latch-based coordination is
  common, but Awaitility is not currently a declared dependency.
- `./gradlew test` passed all 737 unit tests.
- `./gradlew integrationTest` ran 290 integration tests with one failure. The failing storage
  suite contained 34 tests and exposed one stale contract test:
  `DefaultSegmentFactoryIntegrationSpec` calls a removed five-argument `createSegment` overload.
  The current method requires `maxSegmentSize`.
- `./gradlew :broker:journeyTest` passed all 37 in-process journey tests in 3m 50s.
- `./gradlew :broker:systemTest` passed both separate-process black-box tests in 19s.
- `./gradlew jacocoReport` completed successfully.
- Gradle reports deprecated behavior that will be incompatible with Gradle 9.

Merged JaCoCo coverage from the test tasks executed in this workspace:

| Module | Line coverage | Branch coverage |
|---|---:|---:|
| broker | 79.6% | 61.3% |
| client | 63.0% | 42.1% |
| common | 75.8% | 50.4% |
| network | 65.9% | 46.0% |
| pipe | 88.9% | 66.7% |
| storage | 74.0% | 63.9% |
| test-consumer | No JaCoCo report | No JaCoCo report |

Concurrency-critical class coverage illustrates why aggregate line coverage is insufficient:

| Class | Line | Branch |
|---|---:|---:|
| `CompactionController` | 0.0% | 0.0% |
| `SegmentManager` | 57.9% | 52.9% |
| `ConsumerReadinessManager` | 63.6% | 50.0% |
| `ClientConsumerManager` | 62.5% | 42.1% |
| `NettyTcpClient` | 65.9% | 30.0% |
| `NettyTcpServer` | 75.5% | 46.0% |
| `HttpPipeConnector` | 86.0% | 65.2% |
| `TopologyManager` | 89.2% | 70.0% |

Coverage does not prove thread safety: the missing branches are disproportionately timeout,
rejection, shutdown, and race-resolution paths.

## Risk Scale

- **Critical:** plausible data corruption, duplicate identity, or irreversible workflow break.
- **High:** deadlock/starvation, unbounded resource growth, broken reconnect, or hidden critical failure.
- **Medium:** race with narrower timing/ownership assumptions, lifecycle ambiguity, or leak over time.
- **Low:** explicit invariant or cleanup improvement with limited current blast radius.

Coverage markers:

- **U:** unit
- **I:** Micronaut/integration
- **J:** in-process journey
- **S:** separate-process/system/stress

## Component Assessment

### Process and Command Boundaries

| Component | Risk assessment | Readability | Coverage gaps |
|---|---|---:|---|
| `Application` | Startup exceptions should fail the process, but no policy converts later critical worker failures into process termination/non-zero exit. | High | U: startup failure mapping. I: invalid configuration. J: worker failure propagation. S: assert exit codes and no leaked threads. |
| `CompactionController` | Uses `CompletableFuture.runAsync` on the common pool and permits overlapping trigger calls and overlap with `@Scheduled compact()`. The HTTP response says triggered before execution outcome is known. | High | U: concurrent triggers and async failure. I: HTTP 409/202 contract. J: trigger during scheduled run. S: sustained trigger storm. |
| TCP message handlers | Payload validation is generally clear. `BatchAckHandler` catches task-body exceptions, but executor rejection is only caught by a broad outer catch and is not reported to the client or health layer. | Medium | U: rejected executor, task crash, shutdown race. I: ACK while pool saturated. J: consumer-visible retry. S: exit/health behavior after critical failure. |
| `test-consumer` process | `LegacyConsumerService` starts a non-daemon event-loop thread and has a public `shutdown()` that is not annotated `@PreDestroy`. A normal Micronaut shutdown can leave the process alive. | Medium | U: event-loop exception and shutdown. I: context close invokes cleanup. J: broker disconnect. S: process terminates within deadline. |

### Executor Owners, Services, Workers, and Futures

| Component | Risk assessment | Readability | Coverage gaps |
|---|---|---:|---|
| `ExecutorFactory` | Named pools and daemon thread names are good. `newFixedThreadPool` creates unbounded queues; no rejection policy, queue metric, uncaught-exception handler, or configurable queue capacity exists. | High | U: pool sizing and thread naming. I: DI qualifiers. J: bounded saturation. S: memory/latency at capacity. |
| `ShutdownCoordinator` | Graceful then forced shutdown and interrupt restoration are correct. It shares executor ownership with `BrokerService`, waits sequentially up to roughly 90 seconds, and does not own private schedulers created by other beans. | High | U: timeout/interrupt/drop counts. I: single owner and shutdown order. J: shutdown under active load. S: process deadline/thread leak. |
| `BrokerService` | Shuts down `ackExecutor` before stopping ingress and delivery producers. It also duplicates `ShutdownCoordinator` ownership. ACK submissions can be rejected during the shutdown window. | Medium | U: shutdown ordering. I: Micronaut destroy ordering. J: active delivery plus shutdown. S: SIGTERM with traffic. |
| `BatchDeliveryService` | It blocks a scarce `TopicFairScheduler` worker for up to ten minutes while waiting on another executor. At the effective two-thread defaults, two stalled storage tasks can stop unrelated topic delivery. Its ACK timeout rollback is also not guarded by a delivery generation. | Low-Medium | U: timeout, cancellation, `ExecutionException`, interrupt restoration, timeout/late-ACK/new-delivery interleaving. I: storage stall. J: retry after timeout. S: saturated storage pool. |
| `BatchAckService` | Task body catches failures, but `execute()` rejection is not handled. RocksDB failures are swallowed by the repository, so the task may appear successful after data loss. | Medium | U: rejection and repository failure. I: RocksDB write failure. J: ACK replay persistence outage. S: queue saturation. |
| `BatchAckHandler` | Offloading protects the Netty event loop and MDC is propagated. There is no explicit response or fatal policy for executor rejection; task failures are log-only. | High | U: rejection and task exception. I: ACK under shutdown. J: consumer retry semantics. S: pool saturation. |
| `ConsumerReadinessManager` | The retry task blocks on unbounded `send(...).get()`. ACK cancellation can race with the running task, which then unconditionally creates the next retry. It does not re-check ready state before send/reschedule. | High | U: ACK-vs-reschedule race, timeout, send failure, max retry. I: delayed network future. J: duplicate READY prevention. S: all scheduler threads stalled. |
| `ConsumerRegistry` | Multiple READY/RESET/batch sends use unbounded `get()`. The legacy ACK timeout uses a millisecond timestamp as a generation token. Same-millisecond batches can make a stale timer clear newer state. | Medium | U: send timeout and stale timer. I: blocked TCP future. J: rapid ACK/batch turnover. S: consumer scheduler starvation. |
| `TopicFairScheduler` | Semaphore fairness is useful. Recursive retry scheduling publishes the future after scheduling; a zero-delay retry can run before publication. Unconditional removal can delete a newer generation. Topic/retry maps are not pruned. | Medium | U: zero-delay retry race, newer-generation removal, rejection, cleanup. I: fairness at capacity. J: many topics. S: high-cardinality soak. |
| `ConsumerDeliveryManager` | `containsKey` then schedule then `put` is not atomic; concurrent starts can create duplicate periodic tasks. It correctly cancels tasks and awaits its private scheduler. | Medium | U: concurrent start and shutdown. I: duplicate registration. J: reconnect storm. S: task cardinality soak. |
| `ConsumerContext` | `volatile` provides visibility but `++consecutiveFailures` and reset are compound, non-atomic transitions. Normal fixed-delay execution limits exposure, but duplicate starts and concurrent control operations make lost updates reachable. | High | U: concurrent increment, increment-vs-reset, and retry-delay consistency. I: duplicate delivery registration. J: reconnect/control race. S: repeated lifecycle churn. |
| `AdaptiveBatchDeliveryManager` | `volatile boolean running` gives visibility but start is check-then-set rather than atomic. Concurrent starts can double-schedule. | High | U: concurrent start/stop. I: lifecycle event duplication. J: restart/reconnect. S: repeated lifecycle calls. |
| `DeliveryScheduler` | Delegation keeps logic understandable, but error propagation inherits `TopicFairScheduler` log-and-swallow behavior. | High | U: scheduling rejection and failed delivery task. I: timing/backoff. J: delayed retry ordering. S: saturated fairness pool. |
| `RefreshCoordinator` | Strongest concurrency area: concurrent maps, task tracking, generation IDs, CAS transition guards, and explicit shutdown. It is large and has high cognitive load; private executor ownership and callback wiring obscure lifecycle boundaries. | Low-Medium | U: cleanup task after completion, rejected scheduling, concurrent shutdown. I: context destruction. J: already broad. S: prolonged stalled consumers. |
| `TopologyManager` | A single scheduler starts async registry calls that complete on common-pool threads, so polls can overlap. Callback ignores `running`, query errors are silently discarded, shutdown does not await, and parent switching reuses the broken connector. | High | U: overlap, callback after shutdown, query failure. I: slow registry and parent switch. J: topology churn. S: registry timeout storm. |
| `CloudRegistryClient` | Wraps blocking Micronaut HTTP in common-pool `supplyAsync`. This can starve unrelated common-pool work and has no dedicated bulkhead. | High | U: exception cause contract. I: read/connect timeout. J: registry outage. S: many concurrent brokers/queries. |
| `HttpPipeConnector` | `disconnect()` terminates the only scheduler permanently. `reconnect()` and parent switching then fail with `RejectedExecutionException`, after state has already been changed to running/new connection. | High | U: disconnect-connect reuse, concurrent connect, rejection state. I: parent switch. J: outage recovery. S: repeated topology churn and thread leak. |
| `ClientConsumerManager` | Per-topic reconnect has no single-flight guard. Several callbacks can schedule duplicate reconnects that each tear down the current connection. Global reconnect has a guard, showing the intended pattern. | Medium | U: duplicate callbacks. I: connection churn. J: simultaneous health/disconnect failure. S: broker flap soak. |
| `FlushingPropertiesStore` | Shutdown is sound, but `start()` is not idempotent and does not retain the scheduled future. Multiple starts create duplicate flush loops. | High | U: double start, task exception, stop-before-start. I: lifecycle callbacks. J: restart. S: long-running flush failure. |

### Shared State and Repositories

| Component | Risk assessment | Readability | Coverage gaps |
|---|---|---:|---|
| `DeliveryStateStore` | `ConcurrentHashMap` protects individual operations, not compound read-modify-write updates. Concurrent offset and in-flight updates can overwrite one another. Repository keys are also written separately. | High | U: concurrent field updates and torn persistence. I: restart after racing updates. J: ACK vs next delivery. S: repeated race stress. |
| `InMemoryPendingAckStore` | Three independent concurrent maps represent one logical pending-ACK record. Batch, timer, and send time transitions are not atomic and can expose mixed generations. | High | U: atomic replace/remove generation. I: timeout vs ACK. J: rapid batch turnover. S: many clients. |
| `InMemoryInFlightDeliveryStore` / `InMemoryReadyStateStore` | Concurrent collections are appropriate, but direct component tests are missing. Correctness is inferred through higher layers. | High | U: every atomic transition, cancel race, remove client. I: lifecycle. J: reconnect. S: high cardinality. |
| `RefreshContext` | Most collections and transition claims are thread-safe. `recordReplayProgress` is a get-then-put race and can regress a higher offset; `recordStartup` is a non-atomic check/add/clear sequence. | Medium | U: competing higher/lower progress and duplicate startup. I: concurrent callbacks. J: progress monotonicity. S: many consumers. |
| `HashCache` | Synchronized LRU plus in-flight coalescing is reasonable. Followers call unbounded `existing.get()`; stalled computation blocks all followers and recursive same-key computation self-deadlocks. Mutable hash arrays may escape. | Medium | U: coalescing, failure, timeout, recursion, invalidation race, defensive copy. I: slow hash view. J: consistency endpoint load. S: cache stampede. |
| `PropertiesFileStore` | Concurrent map and synchronized atomic-file replacement are good. The weakly consistent map snapshot can omit a racing update from the current flush. I/O errors are log-only. | High | U: mutation during flush and move failure. I: restart durability. J: shutdown flush. S: disk-full/slow-disk fault. |
| `RocksDbAckStore` | RocksDB itself is thread-safe, but `put`, `putBatch`, reads, and delete failures are swallowed. Callers cannot distinguish success from persistence loss. | High | U: all exception paths. I: closed DB/read-only/disk failure. J: replay persistence failure. S: write pressure and disk-full. |
| `SegmentMetadataStore` | Access to the shared SQLite connection is synchronized, which is appropriate. Verify that every connection use remains under the same monitor as methods evolve. | High | U: concurrent CRUD. I: database busy/corruption. J: recovery. S: append/roll metadata pressure. |
| `ConsumerOffsetTracker` | Thread-safe backing store is used, but persistence semantics inherit weak snapshot and swallowed I/O failures. `resetOffset` causes redundant flush behavior. | High | U: racing update/reset and persistence failure. I: restart. J: ACK/refresh race. S: many delivery keys. |

### Storage and Network

| Component | Risk assessment | Readability | Coverage gaps |
|---|---|---:|---|
| `Segment` | Synchronized append provides per-segment exclusion. Concurrent reads have a direct test. It cannot make manager-level offset selection and rollover atomic. | High | U: append vs seal/close. I: rollover boundary. J: producer concurrency. S: maximum writers. |
| `SegmentManager` | Critical race: active-segment read, capacity check, next-offset read, record mutation, and synchronized segment append do not share one manager lock. Roll-on-full can also seal a segment retained by another writer. | Medium | U: concurrent auto-offset append and append-vs-roll. I: recovery after concurrent writes. J: multiple producers plus pipe. S: sustained max writers. |
| Storage engines | FileChannel/MMap implementations have focused tests, but no maximum concurrent append/read/roll/compaction stress suite exists. | Medium | U: close races. I: engine parity. J: compaction plus write/read. S: disk latency and capacity. |
| `NettyTcpClient` | Connection fields are plain references used across caller and event-loop threads. `waitForAck` has a check-then-register race: an ACK can land after `completedAcks.remove` but before `pendingAcks.put`, causing false timeout. Early ACKs can accumulate indefinitely. | Medium | U: deterministic ACK registration race and visibility. I: early ACK/close. J: reconnect. S: ACK storm. |
| `NettyTcpServer` | Client map is concurrent and server channel close waits. Event-loop `shutdownGracefully()` futures are not awaited, so shutdown completion is logged before threads terminate. | High | U: shutdown idempotency. I: no remaining event loops. J: restart same port. S: process deadline. |

## Confirmed High-Severity Findings

### 1. Non-Atomic Offset Assignment and Append

`storage/src/main/java/com/messaging/storage/segment/SegmentManager.java:106`

The method reads `activeSegment`, checks capacity, obtains `current.getNextOffset()`, mutates the
record, and only then enters `Segment.append()`, which is synchronized at the segment level.
Two callers can observe the same next offset. A caller can also retain an old active segment
while another caller rolls and seals it.

Required change:

- Make active-segment selection, rollover decision, offset allocation, and append one atomic
  manager operation.
- Prefer a dedicated append lock or hold the manager write lock for the full append transaction.
- Do not mutate caller-owned `MessageRecord`; create an immutable/copy record with assigned offset.
- Add concurrent append and append-versus-roll tests before optimizing lock granularity.

### 2. Pipe Connector Cannot Reconnect

`pipe/src/main/java/com/messaging/pipe/HttpPipeConnector.java:79` and `:187`

The scheduler is created once and is final. `disconnect()` calls `shutdownNow()`. `reconnect()`
immediately invokes `connectToParent()` on the same object, and topology parent switching uses
the same sequence. The next `scheduler.execute()` is rejected.

Required change:

- Separate "disconnect current parent" from "destroy connector bean".
- Keep the scheduler alive for the bean lifetime; cancel a tracked poll future on disconnect.
- Add `@PreDestroy` for final scheduler shutdown.
- Make connect/disconnect transitions atomic and idempotent.
- Avoid the common pool: return an already-completed future after scheduling, or inject a
  dedicated executor.

### 3. Compaction Has No Single-Flight Guard

`broker/src/main/java/com/messaging/broker/http/CompactionController.java:82` and
`broker/src/main/java/com/messaging/broker/compaction/CompactionScheduler.java:90`

Manual triggers run on the common pool while Micronaut independently invokes the scheduled method.
Two runs can plan and rewrite the same sealed window concurrently.

Required change:

- Put an `AtomicBoolean.compareAndSet(false, true)` guard in `CompactionScheduler`, not only in
  the controller, so every entry point shares it.
- Return a result such as `STARTED`, `ALREADY_RUNNING`, or `FAILED_TO_SCHEDULE`.
- Use an injected bounded executor.
- Return HTTP 409 for already running, or HTTP 202 with a run ID and status endpoint.

### 4. Saturation Is Unbounded

`broker/src/main/java/com/messaging/broker/config/ExecutorFactory.java:32`, `:62`, and `:82`

`Executors.newFixedThreadPool` uses an unbounded `LinkedBlockingQueue`. At the configured
two-thread defaults, a slow downstream system can create an arbitrarily large task backlog.

Required change:

- Construct `ThreadPoolExecutor` directly with a configured bounded queue.
- Choose rejection by workflow:
  - network ACK path: reject and close/nack so the consumer retries;
  - delivery polling: coalesce/drop duplicate scheduling;
  - persistence: apply caller-runs only if blocking the submitting thread is acceptable.
- Export active count, queue depth, completed count, rejection count, and oldest task age.
- Validate thread and queue sizes at configuration binding time.

### 5. Unbounded Blocking on Shared Scheduler Threads

`ConsumerReadinessManager.java:93` and `ConsumerRegistry.java:278`, `:453`, `:474`, `:499`,
`:524`, `:546`, and `:574`

Each call can wait forever. With two configured consumer scheduler threads, two stalled sends can
stop READY retries and ACK-timeout work globally.

Required change:

- Use `orTimeout`/timed `get`, then handle `TimeoutException` explicitly.
- Prefer non-blocking completion chains for network sends.
- Do not perform broadcast sends serially; collect bounded futures and apply a total deadline.
- Separate timeout scheduling from potentially blocking I/O.

### 6. ACK Timeout Can Revert a Newer Delivery

`broker/src/main/java/com/messaging/broker/consumer/BatchDeliveryService.java:279` and
`broker/src/main/java/com/messaging/broker/consumer/BatchAckService.java:102`

`removePendingOffset()` atomically chooses whether the ACK or timeout owns the old pending
offset, but the remaining state transition is split across independent operations. This
interleaving is valid:

1. The timeout removes the old pending offset.
2. A late ACK sees no pending offset and clears `inFlight`.
3. A scheduler starts a new batch and advances `RemoteConsumer.currentOffset`.
4. The old timeout writes `originalOffset` and clears `inFlight`, reverting the newer delivery.

Required change:

- Represent pending offset, current/in-flight status, timeout, trace ID, and generation as one
  immutable per-delivery state.
- Make ACK, timeout, and new-delivery start compare and transition the expected generation
  atomically.
- Do not treat an ACK for an unowned generation as permission to clear the current in-flight
  delivery.
- Add a latch-controlled regression test for the exact timeout/late-ACK/new-delivery ordering.

## Readability and Architecture Improvements

1. Introduce explicit lifecycle interfaces for private executor owners:
   `start()`, `stopAccepting()`, `drain(Duration)`, and `forceStop()`.
2. Give each executor one owner. Consumers submit work but do not shut the executor down.
3. Replace anonymous recursive scheduling with named task classes:
   `ReadyRetryTask`, `DeliveryAttempt`, `PipePollTask`, and `AckPersistenceTask`.
4. Pass immutable task input snapshots. Do not close over mutable service state when a small
   command object can capture `clientId`, delivery key, generation, offsets, and trace ID.
5. Model compound repository state as one immutable value per key and update it with
   `ConcurrentHashMap.compute`.
6. Inject `Clock`, executors, and timeout configuration. This removes sleeps and wall-clock
   collisions from tests.
7. Use a shared `AsyncFailurePolicy`/`FatalErrorCoordinator` to classify:
   retryable, client-scoped, degraded-health, and process-fatal failures.
8. Replace broad `catch (Exception)` blocks with explicit `InterruptedException`,
   `TimeoutException`, `ExecutionException`, and `RejectedExecutionException` handling.
9. Restore interrupt status whenever interruption is caught.
10. Remove decorative Unicode from hot-path log messages; structured event fields are easier to
    query and keep source/log output consistent.

## Testing Tier Assessment

### Unit

Strengths:

- Broad service-level Spock coverage.
- Good direct checks for refresh transition claims and several repository behaviors.
- Network codec and storage primitives have focused tests.

Gaps:

- Tests disproportionately validate sequential outcomes, not happens-before relationships.
- No direct tests were found for `ConsumerReadinessManager`, `HashCache`,
  `CompactionController`, `FlushingPropertiesStore`, `InMemoryReadyStateStore`, or
  `InMemoryPendingAckStore`.
- `ConsumerContext` has no deterministic concurrent increment/reset test.
- No test pauses an ACK timeout after it claims the old pending offset, permits a late ACK and
  newer delivery, then proves the old timeout cannot mutate that newer generation.
- `SegmentConcurrentReadSpec` covers readers only, not concurrent writers.
- Executor rejection, cancellation, interruption, and task exceptions are not systematically tested.

### Integration (`@MicronautTest`)

Strengths:

- Real DI wiring exists across broker, network, pipe, storage, and client modules.
- Real RocksDB, SQLite, HTTP, TCP, compaction, and topology flows are exercised.

Gaps:

- The current suite is red because `DefaultSegmentFactoryIntegrationSpec` was not updated when
  the factory signature changed.
- There is no reconnect-after-disconnect test for the real `HttpPipeConnector`.
- There is no bounded-pool saturation test.
- Context shutdown does not assert that all application-owned threads terminate.
- Repository failures are not fault-injected.

### Journey

Strengths:

- Strong domain coverage: broker restart, consumer reconnect/crash, refresh/replay, pipe outage,
  segment rollover, compaction races, tombstones, duplicate keys, and multiple consumers.

Gaps:

- Most tests use an in-process harness and do not prove OS process termination or exit status.
- No journey forces every executor thread to block while continuing to submit work.
- No journey crashes a background task and checks whether the application becomes unhealthy or exits.
- There is no registry topology-churn journey using the real connector lifecycle.

### System and Stress

Strengths:

- Two black-box specs launch separate processes.

Gaps:

- No performance/stress source files exist.
- No queue-capacity, thread-capacity, memory-growth, disk-full, slow-disk, network-blackhole,
  or repeated SIGTERM test exists.
- No test asserts exit code 0 on clean shutdown and non-zero on startup/critical worker failure.
- No thread-dump or non-daemon-thread leak assertion exists.

## End-to-End User Flow Trace

Representative modern consumer flow:

1. `Application.main()` starts Micronaut.
2. `ExecutorFactory` creates named ACK, ACK-storage, storage, consumer, refresh, and flush pools.
3. `BrokerService` starts network and delivery components.
4. TCP subscription reaches the handler/consumer registry.
5. `AdaptiveBatchDeliveryManager` and `DeliveryScheduler` distribute delivery work through
   `TopicFairScheduler`.
6. Storage work is submitted to the storage executor; `BatchDeliveryService` collects the future
   with a timeout.
7. Netty sends the batch asynchronously.
8. A consumer BATCH_ACK reaches `BatchAckHandler`, which executes ACK processing on `ackExecutor`.
9. `BatchAckService` clears in-flight state and submits RocksDB replay/persistence to
   `ackStorageExecutor`.
10. Logs/metrics are the effective console output.

Failure behavior today:

- A task submitted with `execute()` usually logs and returns; no `Future` exposes failure.
- A task submitted with `submit()` is only safe where its future is collected.
- RocksDB persistence errors are swallowed.
- Pool saturation is queued without bound.
- A critical background crash does not produce a non-zero process exit.

## Draft Executable Regression Tests

These are intentionally red against the current implementation where they expose a confirmed
defect. Add JUnit 5, Mockito JUnit Jupiter, and Awaitility to the custom test configurations:

```groovy
unitTestImplementation 'org.junit.jupiter:junit-jupiter'
unitTestImplementation 'org.mockito:mockito-junit-jupiter'
unitTestImplementation 'org.awaitility:awaitility:4.2.2'
integrationTestImplementation 'org.awaitility:awaitility:4.2.2'
```

### 1. Concurrent Segment Append Must Allocate Unique Offsets

Target: `storage/src/unitTest/java/com/messaging/storage/segment/SegmentManagerConcurrentAppendTest.java`

```java
package com.messaging.storage.segment;

import com.messaging.common.model.EventType;
import com.messaging.common.model.MessageRecord;
import com.messaging.storage.metadata.SegmentMetadataStore;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;
import java.time.Instant;
import java.util.HashSet;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.stream.IntStream;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

class SegmentManagerConcurrentAppendTest {
    @TempDir Path tempDir;
    private ExecutorService executor;
    private SegmentManager manager;
    private SegmentMetadataStore metadata;

    @AfterEach
    void cleanup() throws Exception {
        if (executor != null) {
            executor.shutdownNow();
            assertTrue(executor.awaitTermination(5, TimeUnit.SECONDS));
        }
        if (manager != null) manager.close();
        if (metadata != null) metadata.close();
    }

    @Test
    void concurrentAutoOffsetAppendsAreUniqueAndContiguous() throws Exception {
        Path topicDir = tempDir.resolve("prices");
        metadata = new SegmentMetadataStore(topicDir);
        manager = new SegmentManager(
                "prices", 0, topicDir.resolve("partition-0"), 8 * 1024L, metadata);

        int writers = 16;
        int records = 512;
        executor = Executors.newFixedThreadPool(writers);
        CountDownLatch start = new CountDownLatch(1);

        List<Callable<Long>> tasks = IntStream.range(0, records)
                .mapToObj(i -> (Callable<Long>) () -> {
                    start.await();
                    MessageRecord record = new MessageRecord(
                            0L, "prices", 0, "key-" + i,
                            EventType.MESSAGE, "value-" + i, Instant.now());
                    return manager.append(record);
                })
                .toList();

        List<Future<Long>> futures = tasks.stream().map(executor::submit).toList();
        start.countDown();

        List<Long> offsets = futures.stream().map(future -> {
            try {
                return future.get(10, TimeUnit.SECONDS);
            } catch (Exception e) {
                throw new AssertionError("append failed", e);
            }
        }).sorted().toList();

        assertEquals(records, new HashSet<>(offsets).size(), "duplicate offsets");
        assertEquals(
                IntStream.range(0, records).mapToObj(Integer::longValue).toList(),
                offsets);
    }
}
```

### 2. Connector Must Be Reusable After Disconnect

Target: `pipe/src/unitTest/java/com/messaging/pipe/HttpPipeConnectorLifecycleTest.java`

```java
package com.messaging.pipe;

import com.messaging.pipe.metrics.PipeMetrics;
import io.micronaut.http.client.HttpClient;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.mockito.Mockito.mock;

class HttpPipeConnectorLifecycleTest {
    @TempDir Path tempDir;

    @Test
    void disconnectDoesNotDestroySchedulerNeededForReconnect() throws Exception {
        HttpPipeConnector connector = new HttpPipeConnector(
                mock(HttpClient.class), tempDir.toString(), 100, 1_000, 5,
                mock(PipeMetrics.class));

        connector.connectToParent("http://parent-a").get(2, TimeUnit.SECONDS);
        connector.disconnect();

        assertDoesNotThrow(() ->
                connector.connectToParent("http://parent-b").get(2, TimeUnit.SECONDS));

        connector.disconnect();
    }
}
```

After the lifecycle refactor, call the final `close()`/`@PreDestroy` method in cleanup rather than
using `disconnect()` as bean destruction.

### 3. READY ACK Must Prevent a Running Retry from Rescheduling

Target:
`broker/src/unitTest/java/com/messaging/broker/consumer/ConsumerReadinessManagerTest.java`

```java
package com.messaging.broker.consumer;

import com.messaging.common.api.NetworkServer;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.*;

class ConsumerReadinessManagerTest {
    @Test
    void acknowledgedConsumerIsNotSentOrRescheduledByCapturedRetry() throws Exception {
        ReadyStateStore store = mock(ReadyStateStore.class);
        NetworkServer server = mock(NetworkServer.class);
        ScheduledExecutorService scheduler = mock(ScheduledExecutorService.class);
        ScheduledFuture<?> scheduled = mock(ScheduledFuture.class);
        ArgumentCaptor<Runnable> task = ArgumentCaptor.forClass(Runnable.class);

        when(scheduler.schedule(task.capture(), anyLong(), eq(TimeUnit.MILLISECONDS)))
                .thenReturn(scheduled);
        when(server.send(any(), any())).thenReturn(CompletableFuture.completedFuture(null));

        ConsumerReadinessManager manager =
                new ConsumerReadinessManager(store, server, scheduler);
        manager.scheduleReadyRetry("client-1", "prices", "group-1", 0);

        when(store.isModernConsumerTopicReady(
                eq("client-1"), any())).thenReturn(true);
        manager.markModernConsumerTopicReady("client-1", "prices", "group-1");

        clearInvocations(scheduler, server);
        task.getValue().run();

        verifyNoInteractions(server);
        verify(scheduler, never()).schedule(any(Runnable.class), anyLong(), any());
    }
}
```

The implementation must re-check readiness immediately before send and before rescheduling.
Cancellation alone is insufficient because a running scheduled task cannot be recalled.

### 4. Compaction Must Be Single-Flight

Target: `broker/src/unitTest/java/com/messaging/broker/http/CompactionControllerTest.java`

```java
package com.messaging.broker.http;

import com.messaging.broker.compaction.CompactionScheduler;
import com.messaging.common.api.StorageEngine;
import com.messaging.storage.segment.SegmentAccess;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import static org.awaitility.Awaitility.await;
import static org.mockito.Mockito.*;

class CompactionControllerTest {
    @Test
    void concurrentTriggersStartOnlyOneCompaction() throws Exception {
        CompactionScheduler scheduler = mock(CompactionScheduler.class);
        StorageEngine storage = mock(StorageEngine.class);
        SegmentAccess segmentAccess = mock(SegmentAccess.class);
        when(storage.getTopicNames()).thenReturn(Set.of());

        CountDownLatch entered = new CountDownLatch(1);
        CountDownLatch release = new CountDownLatch(1);
        doAnswer(invocation -> {
            entered.countDown();
            release.await(5, TimeUnit.SECONDS);
            return null;
        }).when(scheduler).compact();

        CompactionController controller =
                new CompactionController(scheduler, storage, segmentAccess);
        controller.trigger();
        await().atMost(2, TimeUnit.SECONDS).until(() -> entered.getCount() == 0);

        try {
            controller.trigger();
            await().during(Duration.ofMillis(500))
                    .atMost(Duration.ofSeconds(1))
                    .untilAsserted(() -> verify(scheduler, times(1)).compact());
        } finally {
            release.countDown();
        }
    }
}
```

This controller-level draft only detects the current overlapping entry point. The acceptance
test must exercise a real single-flight method in `CompactionScheduler`, because mocking the
scheduler cannot prove that manual and scheduled entry points share the same guard.

### 5. Micronaut Executor Saturation Must Reject Predictably

Target:
`broker/src/integrationTest/java/com/messaging/broker/config/ExecutorSaturationIntegrationTest.java`

```java
package com.messaging.broker.config;

import io.micronaut.context.annotation.Property;
import io.micronaut.test.extensions.junit5.annotation.MicronautTest;
import jakarta.inject.Inject;
import jakarta.inject.Named;
import org.junit.jupiter.api.Test;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

@MicronautTest(startApplication = false)
@Property(name = "executor.ack.threads", value = "1")
@Property(name = "executor.ack.queue-capacity", value = "1")
class ExecutorSaturationIntegrationTest {
    @Inject @Named("ackExecutor") ExecutorService executor;

    @Test
    void queueCapacityAppliesBackpressure() throws Exception {
        CountDownLatch running = new CountDownLatch(1);
        CountDownLatch release = new CountDownLatch(1);

        executor.execute(() -> {
            running.countDown();
            try {
                release.await();
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        });
        assertTrue(running.await(2, TimeUnit.SECONDS));

        executor.execute(() -> { }); // fills the one-slot queue
        assertThrows(RejectedExecutionException.class,
                () -> executor.execute(() -> { }));

        release.countDown();
    }
}
```

Add a second test at the handler boundary to assert the selected rejection contract, such as
closing the client connection or returning a retryable error, rather than merely asserting the
executor exception.

### 6. Background Future Exceptions Must Be Observed

Use this pattern in every unit test for a `submit()`-based worker:

```java
Future<?> future = executor.submit(() -> {
    throw new IllegalStateException("simulated worker failure");
});

ExecutionException failure = assertThrows(
        ExecutionException.class,
        () -> future.get(2, TimeUnit.SECONDS));
assertInstanceOf(IllegalStateException.class, failure.getCause());
```

For production fire-and-forget work, prefer a wrapper that reports failures:

```java
executor.execute(() -> {
    try {
        task.run();
    } catch (RuntimeException failure) {
        failurePolicy.report("ack-persistence", failure);
        throw failure;
    }
});
```

Do not blanket-catch `Throwable` and continue. Fatal `Error` handling belongs in a deliberately
minimal fatal-process policy; it is not ordinary task recovery.

### 7. Separate Process Must Return Explicit Exit Codes

Target:
`broker/src/blackboxSystemTest/java/com/messaging/broker/systemtest/blackbox/BrokerExitCodeSystemTest.java`

```java
package com.messaging.broker.systemtest.blackbox;

import org.junit.jupiter.api.Test;

import java.nio.file.Path;
import java.time.Duration;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

class BrokerExitCodeSystemTest {
    @Test
    void invalidConfigurationExitsNonZeroWithoutHanging() throws Exception {
        String java = Path.of(System.getProperty("java.home"), "bin", "java").toString();
        ProcessBuilder builder = new ProcessBuilder(
                java,
                "-cp", System.getProperty("blackbox.broker.classpath"),
                "com.messaging.broker.Application");
        builder.environment().put("BROKER_PORT", "not-a-number");
        builder.redirectErrorStream(true);

        Process process = builder.start();
        assertTrue(process.waitFor(Duration.ofSeconds(20).toMillis(), TimeUnit.MILLISECONDS));
        assertNotEquals(0, process.exitValue());
    }
}
```

A second process test should inject a process-fatal background failure through a test-only bean
or endpoint and assert non-zero exit. The current architecture has no such failure coordinator,
so that acceptance test requires the recommended fatal-error policy first.

## Stress Test Matrix

| Scenario | Load/fault | Required assertions |
|---|---|---|
| ACK pool saturation | Block all ACK workers, fill queue, submit one more | Bounded queue, deterministic rejection contract, no Netty event-loop block, rejection metric increments |
| Storage pool starvation | Block every storage worker while deliveries continue | Delivery deadline fires, futures cancel, queue remains bounded, health degrades |
| Consumer scheduler starvation | Network sends never complete | Timeout frees scheduler threads; ACK timers and unrelated consumers still progress |
| Concurrent append | 2x CPU-count producers, tiny segments | Unique contiguous offsets, no append to sealed segment, restart recovery matches count |
| Append plus compaction | Continuous writers/readers with repeated compaction | No corruption, monotonic offsets, no missing latest key, bounded descriptors |
| Topology churn | Alternate parent every 100-500 ms | Exactly one poll loop, successful reconnect, no rejected scheduler tasks, no thread growth |
| Reconnect storm | Broker flap plus health-check/disconnect callbacks | One reconnect future per topic/group, no duplicate active connections |
| Disk full/read-only | Fail RocksDB and properties writes | Failure is observable, health/error status changes, no false ACK success |
| Shutdown under load | SIGTERM with full queues and active TCP sends | Stop ingress first, bounded drain, forced cancellation, process exits 0 before deadline |
| Critical worker crash | Throw from compaction/pipe/storage worker | Failure classification is explicit; fatal case exits non-zero |

Use repeated runs and fixed seeds. Capture:

- executor active count, queue depth, rejections, task latency, and task age;
- process RSS/heap, open file descriptors, and thread count;
- delivery/ACK latency percentiles and timeout counts;
- process exit code and shutdown duration.

## Priority Remediation Plan

### P0: Data and Workflow Correctness

1. Serialize the complete `SegmentManager.append` transaction.
2. Split `HttpPipeConnector.disconnect` from final destruction and test parent switching.
3. Add a global compaction single-flight guard.
4. Make delivery/pending-ACK compound state atomic and generation-guard timeout rollback.
5. Stop swallowing RocksDB persistence errors.

### P1: Bounded Concurrency

1. Replace unbounded fixed pools with bounded `ThreadPoolExecutor` beans.
2. Remove unbounded `Future.get()` from shared scheduler threads.
3. Remove the ten-minute cross-pool wait from `BatchDeliveryService`.
4. Add workflow-specific rejection and timeout behavior.
5. Add single-flight guards for per-topic reconnect and delivery start.
6. Give each executor exactly one lifecycle owner.

### P2: Test Reliability and Operations

1. Repair `DefaultSegmentFactoryIntegrationSpec`.
2. Add the red regression tests above.
3. Replace fixed sleeps with Awaitility, latches, fake clocks, and controllable executors.
4. Add real process exit-code and thread-leak checks.
5. Populate `performanceTest` with the stress matrix.
6. Add executor and async-failure metrics plus health indicators.
7. Replace `Thread.getId()` in executor thread naming when the runtime baseline permits.

## Release Gate Recommendation

Do not approve a high-throughput or multi-producer release until:

- concurrent append is proven unique and recoverable;
- real connector disconnect/reconnect passes;
- compaction is single-flight;
- all executor queues are bounded and saturation behavior is tested;
- network futures have deadlines;
- integration tests are green;
- clean shutdown and fatal failure exit codes are asserted in separate processes.
