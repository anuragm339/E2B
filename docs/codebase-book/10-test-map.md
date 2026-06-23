# Test Map

Related: [Features](03-feature-catalog.md), [POS state](07-pos-machine-state.md), [Compaction](08-compaction-handling.md), [Risks](13-risk-and-edge-cases.md), [Open questions](15-open-questions.md).

## Build Test Layout

The root `build.gradle` defines `integrationTest`, `systemTest`, and `performanceTest`. Each main library module redirects `test` to `src/unitTest/groovy`. `broker/build.gradle` additionally maps:

- `journeyTest` -> `broker/src/systemTest/{groovy,java}`
- `systemTest` -> `broker/src/blackboxSystemTest/{groovy,java}`

This naming is important when adding or running tests.

## Executed Results

Commands were run on 2026-06-11.

| Command | Result | Evidence/notes |
|---|---|---|
| `./gradlew test --no-daemon --console=plain` | PASS | 740 tests: broker 565, common 25, network 62, pipe 10, storage 78; client/test-consumer no unit source |
| `./gradlew integrationTest --no-daemon --console=plain` | FAIL | Broker reached 118 tests with 2 failures; aggregate stopped before other modules |
| Per-module integration command excluding broker | PASS | common 20, storage 34, network 69, pipe 33, client 11; test-consumer no source |
| Exact broker legacy integration rerun | FAIL | 2/2 failed again |
| `./gradlew :broker:journeyTest` | FAIL once | 36 tests, 1 reconnect/refresh timeout |
| Exact failed journey rerun | PASS | `ConsumerCrashDuringReplayJourneySpec` passed alone |
| `./gradlew :broker:systemTest` | PASS | 1 separate-process black-box test |
| `./gradlew performanceTest` | PASS/NO-SOURCE | All seven module performance source sets empty |
| `./gradlew build` | PASS | Compiles, packages, and runs unit tests; does not include integration/journey/system |

Unique full-suite inventory attempted: 740 unit + 285 integration + 36 journey + 1 black-box = 1,062 tests.

Qodana is configured in `qodana.yaml`, but `qodana` was not installed locally. No Checkstyle, SpotBugs, PMD, lint, or contract-test Gradle task was found by `./gradlew tasks --all`.

Java runtime used by the shell was OpenJDK 21.0.7; Gradle compiles source/target Java 17 from `build.gradle`.

## Observed Failures

### Repeatable Legacy Integration Failure

File: `broker/src/integrationTest/groovy/com/messaging/broker/core/BrokerLegacyConsumerIntegrationSpec.groovy`.

Failures:

1. `legacy consumer receives merged batch and ACK advances offsets`
2. `legacy refresh flow resolves RESET and READY through generic ACKs`

Both time out before registration/readiness. Logs state:

```text
Unknown legacy service: price-quote-service. No topics configured.
```

Cause is code/config related:

- test registers `price-quote-service`;
- `test-consumer/src/main/resources/application.yml` also defaults legacy service name to `price-quote-service`;
- broker `legacy-clients.service-topics` defines `price-quote`, not `price-quote-service`.

Source: `broker/src/main/resources/application.yml`, `BrokerLegacyConsumerIntegrationSpec.groovy`.

### Flaky Journey Failure

File: `broker/src/systemTest/groovy/com/messaging/broker/systemtest/journey/ConsumerCrashDuringReplayJourneySpec.groovy`.

Aggregate failure: consumer A never received refresh READY after consumer B reconnected. Logs showed group B blocked on a pending ACK for over 25 seconds despite `broker.consumer.ack-timeout=5000`.

The exact test passed on a clean rerun. Classification: concurrency/timing flake with a real stale-pending-state signal, not a confirmed deterministic failure.

### Shutdown Warning

Broker integration logs showed `RejectedExecutionException: event executor terminated` when Micronaut disposed `NettyTcpServer` after `BrokerService.shutdown` had already shut it down. Tests continued. Source: `network/src/main/java/com/messaging/network/tcp/NettyTcpServer.java`, `BrokerService.java`.

## Broker Unit Suites

ACK:

- `AckReconciliationSchedulerSpec`
- `AckRecordSpec`
- `InMemoryAckStoreSpec`
- `RocksDbAckStoreSpec`

Compaction:

- `CompactionCheckpointStoreSpec`
- `CompactionPlannerSpec`
- `CompactionRewriterSpec`
- `CompactionSchedulerSpec` (incl. `F2:` — a rejected offload resets the single-flight guard)
- `InMemoryCompactionIndexSpec`
- `RocksDbCompactionIndexSpec`

Consumer/delivery/refresh:

- `AdaptiveBackoffPolicySpec`
- `BatchAckServiceRocksDbSpec`
- `BatchDeliveryServiceSpec`
- `ConsumerAnnotationProcessorSpec`
- `ConsumerContextSpec`
- `ConsumerDeliveryManagerSpec`
- `ConsumerOffsetTrackerSpec`
- `ConsumerRegistrationManagerSpec`
- `ConsumerRegistrySpec`
- `DeliveryStateStoreSpec`
- `FlushingPropertiesStoreSpec`
- `InMemoryConsumerSessionStoreSpec`
- `InMemoryInFlightDeliveryStoreSpec`
- `InMemoryPendingAckStoreSpec`
- `PropertiesFileStoreSpec`
- `RefreshContextSpec`
- `RefreshCoordinatorSpec` (incl. `F1:` specs — each refresh timer survives a throwing collaborator and still reschedules/re-arms)
- `RefreshInitiatorSpec`
- `RefreshReadyServiceSpec`
- `RefreshRecoveryServiceSpec`
- `RefreshReplayWindowResolverSpec`
- `RefreshReplayServiceSpec`
- `RefreshResetServiceSpec`
- `RefreshResultSpec`
- `RefreshStateMachineSpec`
- `RefreshStateStoreSpec`
- `RemoteConsumerSpec`
- `TopicFairSchedulerSpec`

Core/handler/legacy/model/monitoring:

- `BrokerServiceSpec`, `ShutdownCoordinatorSpec`, `TopologyManagerSpec`, `TopologyPropertiesStoreSpec`
- `BatchAckHandlerSpec`, `MessageParserFactorySpec`, `ParseExceptionSpec`, `ReadyAckHandlerSpec`, `ResetAckHandlerSpec`, `SubscribeHandlerSpec`
- JSON/legacy parser and validation specs under `broker/src/unitTest/groovy/com/messaging/broker/handler/`
- `LegacyConsumerDeliveryManagerSpec`, `MergedBatchSpec`, `TopicCursorSpec`
- DTO specs under `broker/src/unitTest/groovy/com/messaging/broker/model/`
- `BrokerMetricsSpec`, `DataRefreshMetricsSpec`, `LogContextSpec`, `RefreshHealthIndicatorSpec`, `TraceIdsSpec`

## Broker Integration Suites

- ACK/compaction: `AckStoreIntegrationSpec`, `CompactionDeliveryFilterIntegrationSpec`, `CompactionIndexWiringIntegrationSpec`
- Wiring/executors: `ExecutorFactoryIntegrationSpec`, `HandlerRegistryIntegrationSpec`
- Consumer: `AdaptiveBatchDeliveryManagerIntegrationSpec`, `ConsumerOffsetTrackerIntegrationSpec`, `RefreshStateMachineIntegrationSpec`, `TopicFairSchedulerIntegrationSpec`, `WatermarkGatePolicyIntegrationSpec`
- Core: `BrokerLegacyConsumerIntegrationSpec`, `BrokerRegistryIntegrationSpec`, `BrokerServiceCriticalIntegrationSpec`, `BrokerServiceIntegrationSpec`, `BrokerSetupIntegrationSpec`, `CloudRegistryClientIntegrationSpec`, `TopologyManagerProbeIntegrationSpec`
- Handlers: `BatchAckHandlerIntegrationSpec`, `ClientDisconnectHandlerIntegrationSpec`, `CommitOffsetHandlerIntegrationSpec`, `DataHandlerIntegrationSpec`, `SubscribeHandlerIntegrationSpec`
- HTTP: `PipeMessageForwarderIntegrationSpec`, `RefreshControllerIntegrationSpec`, `TestDataControllerIntegrationSpec`, `ThreadDiagnosticsControllerIntegrationSpec`
- Monitoring: `ConsumerGroupMetricsIntegrationSpec`, `MemoryMonitorIntegrationSpec`, `ThreadMonitorIntegrationSpec`

All are under `broker/src/integrationTest/groovy/com/messaging/broker/`.

## Journey Suites

All under `broker/src/systemTest/groovy/com/messaging/broker/systemtest/journey/`:

- Startup/restart: `BrokerRestartJourneySpec`, `AckStoreSeederRestartJourneySpec`
- Delivery: `MessageDeliveryJourneySpec`, `MultiConsumerJourneySpec`, `LegacyConsumerJourneySpec`, `FlakyConsumerJourneySpec`, `ConsumerReconnectJourneySpec`, `GappedOffsetJourneySpec`, `DuplicateMsgKeyJourneySpec`, `LargeRecordRocksDbAckSpec`, `SegmentRolloverWithPipeDataJourneySpec`
- Pipe: `PipeOutageJourneySpec`
- Refresh: `DataRefreshJourneySpec`, `ConcurrentRefreshJourneySpec`, `ConsumerCrashDuringReplayJourneySpec`, `LateConsumerDuringRefreshJourneySpec`, `RefreshRestartRecoveryJourneySpec`, `RefreshWithPendingPipeDataJourneySpec`, `RefreshAwareReconciliationJourneySpec`
- Compaction: `CompactionConcurrentPipeDiffKeyJourneySpec`, `CompactionConcurrentPipeSameKeyJourneySpec`, `CompactionDeliveryRecoveryJourneySpec`, `CompactionDuringRefreshJourneySpec`, `CompactionRaceWindowLegacyDeliveryJourneySpec`, `CompactionRestartRecoveryJourneySpec`, `LateConsumerAfterCompactionJourneySpec`, `RefreshWithCompactedAndActiveSegmentsJourneySpec`, `TombstoneLifecycleJourneySpec`

These map directly to [Features](03-feature-catalog.md), [POS state](07-pos-machine-state.md), and [Compaction](08-compaction-handling.md).

## Black-Box System Suite

`broker/src/blackboxSystemTest/groovy/com/messaging/broker/systemtest/blackbox/ModernConsumerEndToEndSystemSpec.groovy` launches broker and test-consumer JVM processes using classpaths supplied by `broker/build.gradle`.

## Other Module Suites

### Common

Models, exceptions, no-op error handler, and auth filter under `common/src/unitTest/groovy/` and `common/src/integrationTest/groovy/`.

### Storage

Engine, segment, concurrent read, crash recovery, memory leak, metadata, recovery service, and watermarks under `storage/src/unitTest/groovy/` and `storage/src/integrationTest/groovy/`.

### Network

Binary/JSON/zero-copy codecs, handlers, protocol detection, legacy event codecs/state, metrics, and TCP integration under `network/src/unitTest/groovy/` and `network/src/integrationTest/groovy/`.

### Pipe

`HttpPipeConnectorSpec`, connector integration, `PipeServerIntegrationSpec`, and `PipeMetricsIntegrationSpec`.

### Client

`client/src/integrationTest/groovy/com/messaging/client/ClientConsumerManagerIntegrationSpec.groovy` verifies discovery, connection, subscribe, data/control routing, and lifecycle.

`ClientConsumerManagerRoutingSpec` covers per-topic:group DATA routing (#13) and #1 ACK ordering:
a `BATCH_ACK` is sent only after every handler's `handleBatch` succeeds, and is withheld when a
handler throws (so the broker redelivers).

Network-layer #1 coverage lives in `network/.../codec/BatchAckHandlerIntegrationSpec` (the handler
unwraps the batch and emits **no** outbound ack) and `network/.../tcp/NettyTcpIntegrationSpec`
(`sendBatch` delivers records to the application and a raw client sends no automatic BATCH_ACK).

## Coverage Gaps

- No contract-test framework or suite.
- No performance tests despite configured source sets.
- No local static-analysis execution; Qodana config only.
- `storage/src/test/java/com/messaging/storage/mmap/MMapStorageEngineTest.java` is orphaned from the custom `test` source set.
- No confirmed inbound HTTP security tests.
- No production-size compaction/load test.
- No real power-loss/filesystem durability test.
- No deterministic stress test for the stale reconnect pending-ACK signal.
- No test enforcing the legacy service-name mapping as a contract.

## Pipe Consistency (added 2026-06-11)

| Spec | Source set | Covers |
|---|---|---|
| `consistency/KeyspaceDigestSpec` | unitTest | digest order-independence, watermark filter, single-key bucket flip, count asymmetry, hash determinism |
| `consistency/PipeConsistencyServiceSpec` | unitTest | consistent path (1 network call), missing vs benign-dead-key, stale vs zombie, fabricated (authoritative ABSENT → INCONSISTENT, non-authoritative → extraKeys), lag classification, watermark clamp, UNREACHABLE/UNSUPPORTED_PARENT, single-flight, all-topics walk |
| `compaction/CompactionIndexForEachEntrySpec` | unitTest | streaming iteration contract on both index backends, meta-key exclusion |
| `http/PipeConsistencyEndpointsIntegrationSpec` | integrationTest | digest/bucket/classify against real storage+index, admin trigger + report polling, input validation |
| `http/PipeConsistencyDisabledIntegrationSpec` | integrationTest | fail-closed 404/503 contract with feature disabled |
| `http/StatusControllerStorageSpec` | unitTest | `/admin/status/storage` roll-up (offsets, durabilityLag, segment counts/bytes, no-disk-IO default) and `/storage/{topic}` segment inventory (sorted by baseOffset, graceful when no segment manager) |
| `monitoring/ErrorRecorderSpec` | unitTest | ERROR-default min-level filtering, top-unique grouping with sample traceId, chronological per-trace chain |
| `monitoring/FailedMessageRecorderSpec` | unitTest | bounded 20-entry LRU failed-record registry: attempt counting, STUCK poison threshold, distinct offsets, newest-first |
| `monitoring/ErrorExplainerSpec` | unitTest | what/how/why mapping from code/exception/message |
| `consistency/PipeConsistencySchedulerSpec` | unitTest | auto-run fires all-topics check; skip reasons (disabled/running/offline/memory-pressure); cloud target needs no parent; throwing check contained |

| `journey/PipeConsistencyEscalationJourneySpec` | journeyTest | real broker: clamp→pending→escalation, head-probe filtering of behind candidate, full verdict from qualified verifier, streak reset |
| `blackbox/PipeConsistencySystemSpec` | systemTest | cross-process: subprocess broker + real pipe ingest + admin HTTP trigger + CONSISTENT verdict vs stub parent |

Escalation unit coverage in `PipeConsistencyServiceSpec`: pending-first-clamp, dead/behind
candidate skip, escalated INCONSISTENT, all-behind stays pending, streak reset.
Cloud-server: `PipeConsistencyEndpointTest` (8 tests incl. head + verifierCandidates contract).

Pipe-ingest sync invariant (segments ↔ compaction index, added 2026-06-12):
`core/BrokerServiceSpec` (unitTest) — append+updateKey both fire on success; duplicate
re-send skips the storage write but heals the index; throwing `updateKey` fails the ingest
so the pipe offset is not advanced. `compaction/RocksDbCompactionIndexSpec` — `updateKey`
propagates a backend `RocksDBException` as `IllegalStateException` instead of swallowing.
`ConsistencyTamperTestController` has NO specs by design (throwaway fault-injection tool).

Coverage gaps: drill-down cap paths (413 / max-drilldown-buckets) only unit-covered.

Note: the two legacy integration failures listed above (and the `price-quote-service` naming) were
resolved on 2026-06-11 — specs now use the fleet's `price-quote` service name; `./gradlew
:broker:integrationTest` and `:broker:journeyTest` pass.
