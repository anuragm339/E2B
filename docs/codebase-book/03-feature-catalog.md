# Feature Catalog

Related: [APIs](04-api-catalog.md), [Data model](05-data-model.md), [Events](06-event-kafka-flow.md), [POS state](07-pos-machine-state.md), [Compaction](08-compaction-handling.md), [Tests](10-test-map.md).

## Producer Ingestion

- **Business purpose:** Accept keyed `MESSAGE` or `DELETE` records from TCP producers.
- **Entry point:** `broker/src/main/java/com/messaging/broker/handler/DataHandler.java`.
- **Main files:** `HandlerInitializer.java`, `DataHandler.java`, `StorageEngine.java`, `CompactionIndex.java`.
- **Flow:** Decode JSON, require `topic`, default missing key/type/data, append partition `0`, update metrics and compaction index, send `ACK`.
- **Data:** `MessageRecord`; segment log/index; SQLite `segment_metadata`.
- **Events:** inbound `DATA`, outbound `ACK`; domain `MESSAGE`/`DELETE`.
- **External systems:** TCP client only.
- **POS dependency:** None directly; produced records feed POS consumers.
- **Compaction:** Every append calls `CompactionIndex.updateKey`.
- **Failures:** Invalid JSON/topic or storage error closes the connection; ACK send failure is logged asynchronously.
- **Tests:** `broker/src/integrationTest/groovy/com/messaging/broker/handler/DataHandlerIntegrationSpec.groovy`; parser/model unit specs.
- **Coverage gaps:** No explicit authorization or payload-size business validation test at the handler boundary was confirmed.
- **Risks:** Generated default keys prevent useful compaction; `event_type` values other than exact `DELETE` become `MESSAGE`.

See [TCP API](04-api-catalog.md#tcp-data-plane) and [Storage](05-data-model.md#segment-storage).

## Topology And Pipe Replication

- **Business purpose:** Discover a parent node and replicate cloud records into local storage.
- **Entry point:** `broker/src/main/java/com/messaging/broker/core/TopologyManager.java`.
- **Main files:** `CloudRegistryClient.java`, `TopologyManager.java`, `TopologyPropertiesStore.java`, `HttpPipeConnector.java`, `BrokerService.java`.
- **Flow:** Poll registry, choose the first parent, probe `/health`, connect pipe, stream `/pipe/poll`, invoke broker handler per record, persist the last successful offset.
- **Data:** `TopologyResponse`, `MessageRecord`, `topology.properties`, `pipe-offset.properties`.
- **Events/topics:** HTTP JSON records with topic and parent offsets; no Kafka client.
- **External systems:** Cloud registry and parent HTTP broker.
- **POS dependency:** Pipe pauses only for destructive download-refresh sections that mutate `pipe-offset.properties` or topic folders; local POS refresh leaves pipe polling active.
- **Compaction:** Pipe records update the same compaction index as producer records.
- **Failures:** Registry/probe/poll failures retain or retry the previous state with adaptive delay; failed storage handling prevents offset advancement.
- **Tests:** `CloudRegistryClientIntegrationSpec`, `TopologyManagerProbeIntegrationSpec`, `HttpPipeConnectorIntegrationSpec`, `PipeOutageJourneySpec`.
- **Coverage gaps:** Multi-parent failover and topic selection across the pipe are not confirmed.
- **Risks:** `HttpPipeConnector` does not add a `topic` query parameter, while `PipeServer` defaults to `price-topic`; `PipeMessageForwarder.getCurrentOffset()` always returns `0`.

See [Pipe API](04-api-catalog.md#pipe-api), [Runtime config](11-runtime-config.md#topology-and-pipe), and [Risks](13-risk-and-edge-cases.md#pipe-and-topology).

## Segment Storage And Recovery

- **Business purpose:** Persist ordered topic records with bounded segment files and restart recovery.
- **Entry point:** `storage/src/main/java/com/messaging/storage/filechannel/FileChannelStorageEngine.java`.
- **Main files:** `Segment.java`, `SegmentManager.java`, `DefaultStorageRecoveryService.java`, `SegmentMetadataStore.java`.
- **Flow:** Create/recover topic manager, append binary record and dense index entry, roll full segments, periodically force active files, read by binary-searching index offsets.
- **Data:** `.log`, `.index`, `.compacted.log`, `.compacted.index`, SQLite `segment_metadata`.
- **Events/topics:** Topic and partition `0`; record offsets may be sparse when copied from a parent.
- **External systems:** Local filesystem, SQLite JDBC, Linux JNA cache advisory.
- **POS dependency:** All replay/readiness state ultimately depends on durable segment offsets.
- **Compaction:** Exposes `SegmentAccess` and atomic replacement APIs to the broker compactor.
- **Failures:** Recovery validates headers, truncates orphan log/index tails, ignores/deletes staging files, and fails startup for unrecoverable storage errors.
- **Tests:** Storage segment, crash recovery, concurrent read, memory-leak, engine, metadata, and recovery specs under `storage/src/`.
- **Coverage gaps:** Power-loss behavior on real POS filesystems and filesystems without atomic move is not confirmed.
- **Risks:** No per-record CRC in current v2 format; non-monotonic supplied offsets are warned but appended; `StorageEngine.compact()` and `validateStorage()` are placeholders.

See [Data model](05-data-model.md).

## Modern Consumer Subscription And Readiness

- **Business purpose:** Register one consumer group for one topic and block delivery until the client is ready.
- **Entry point:** `broker/src/main/java/com/messaging/broker/handler/SubscribeHandler.java`.
- **Main files:** `ConsumerRegistry.java`, `ConsumerRegistrationManager.java`, `ConsumerReadinessManager.java`, `ClientConsumerManager.java`.
- **Flow:** Parse `{topic,group}`, restore and validate `group:topic` offset, atomically register the socket session, ACK subscribe, send `READY`, wait for `READY_ACK`.
- **Data:** `RemoteConsumer`, `ConsumerKey`, `DeliveryKey`, `consumer-offsets.properties`, in-memory readiness state.
- **Events:** `SUBSCRIBE`, `ACK`, `READY`, `READY_ACK`.
- **External systems:** Modern TCP client.
- **POS dependency:** READY is the normal POS startup gate and is reused by refresh completion.
- **Compaction:** No direct state change; later delivery filters superseded records.
- **Failures:** Duplicate registration is ignored; malformed subscription closes the connection; READY retries three times.
- **Tests:** `SubscribeHandlerSpec`, `SubscribeHandlerIntegrationSpec`, `ConsumerRegistrationManagerSpec`, client integration tests.
- **Coverage gaps:** Multiple live sockets for the same group/topic and takeover semantics are not specified.
- **Risks:** Client IDs are remote socket addresses and change on reconnect; group/topic state must never be keyed only by client ID.

See [POS state](07-pos-machine-state.md#startup-readiness).

## Modern Zero-Copy Delivery And ACK

- **Business purpose:** Deliver batches efficiently and commit progress only after client processing.
- **Entry point:** `broker/src/main/java/com/messaging/broker/consumer/DeliveryScheduler.java`.
- **Main files:** `TopicFairScheduler.java`, `BatchDeliveryService.java`, `NettyTcpServer.java`, `ZeroCopyBatchDecoder.java`, `BatchAckService.java`.
- **Flow:** Apply gates, read `DeliveryBatch` on storage executor, filter stale compacted records, reserve generation state, send header plus file region, auto-ACK after decode, persist next offset.
- **Data:** In-memory `PendingDelivery`; `consumer-offsets.properties`; ACK RocksDB records.
- **Events:** `BATCH_HEADER` plus raw bytes, then `BATCH_ACK`.
- **External systems:** Modern TCP client.
- **POS dependency:** Delivery is blocked until startup/refresh readiness and paused in `RESET_SENT`.
- **Compaction:** Delivery-time filter prevents stale versions before physical compaction.
- **Failures:** Storage/send timeouts, ACK timeout offset rollback, exponential consumer backoff, unregister after ten consecutive failures.
- **Tests:** `BatchDeliveryServiceSpec`, `BatchAckServiceRocksDbSpec`, codec tests, modern delivery journey/system tests.
- **Coverage gaps:** Slow real networks and POS process pauses are simulated only through timeouts.
- **Risks:** A reconnect journey once retained a pending ACK beyond its configured timeout; see [Tests](10-test-map.md#observed-failures).

See [Modern event flow](06-event-kafka-flow.md#modern-delivery-flow).

## Legacy Consumer Delivery

- **Business purpose:** Support old service-name registration and merged multi-topic batches.
- **Entry point:** `network/src/main/java/com/messaging/network/legacy/ProtocolDetectionDecoder.java`.
- **Main files:** `LegacyEventDecoder.java`, `LegacyConnectionState.java`, `LegacyClientConfig.java`, `LegacyConsumerDeliveryManager.java`, `ConsumerRegistry.java`.
- **Flow:** Detect leading `REGISTER`, convert it to legacy `SUBSCRIBE`, map service name to topics, merge topic cursors by offset, send a JSON `BatchEvent`, interpret generic ACK by pending FIFO expectation.
- **Data:** Per-topic offsets, `MergedBatch`, `TopicCursor`, segment indexes.
- **Events:** Legacy `REGISTER`, `READY`, `RESET`, `BATCH`, generic `ACK`.
- **External systems:** Legacy POS/service client.
- **POS dependency:** This is the compatibility path for deployed POS services.
- **Compaction:** Legacy cursor reads compacted indexes; merged delivery must preserve offset advancement over gaps.
- **Failures:** Unknown service names do not register; pending batch timeout frees the slot; disconnect clears readiness and pending state.
- **Tests:** Legacy codec tests, `LegacyConsumerDeliveryManagerSpec`, legacy journey specs, `BrokerLegacyConsumerIntegrationSpec`.
- **Coverage gaps:** Production service-name aliases are not tested as a configuration contract.
- **Risks:** Current integration tests repeatedly fail because they register `price-quote-service`, while YAML maps only `price-quote`.

See [Legacy protocol](06-event-kafka-flow.md#legacy-wire-protocol) and [Observed failures](10-test-map.md#observed-failures).

## Offset And ACK Persistence

- **Business purpose:** Persist group progress and provide per-record ACK audit/reconciliation.
- **Entry point:** `broker/src/main/java/com/messaging/broker/consumer/BatchAckService.java`.
- **Main files:** `ConsumerOffsetTracker.java`, `AckStore.java`, `RocksDbAckStore.java`, `AckStoreSeeder.java`, `AckReconciliationScheduler.java`.
- **Flow:** Claim one pending generation, persist offset, update metrics, re-read acknowledged records in chunks, write ACK records asynchronously; seed/reconcile missing historical entries.
- **Data:** `consumer-offsets.properties`; RocksDB keys `{topic}|{group}|{offset}`.
- **Events:** `BATCH_ACK`; legacy generic ACK resolved to batch ACK.
- **External systems:** RocksDB JNI and local storage.
- **POS dependency:** Offset durability controls replay after POS restart.
- **Compaction:** ACK and compaction use separate column families in one `SharedRocksDb`.
- **Failures:** Async ACK-store write can fail after offset commit; seeding/reconciliation repair or report gaps.
- **Tests:** ACK store/unit/integration specs, seeder and reconciliation journey specs.
- **Coverage gaps:** Abrupt power loss between property flush and RocksDB write is not exercised on real hardware.
- **Risks:** Property-file synchronous flush is rate-limited to one second, so at-least-once replay after crash is expected.

See [Data model](05-data-model.md#rocksdb) and [Recovery](09-error-retry-recovery.md#ack-recovery).

## POS Data Refresh

- **Business purpose:** Reset consumers and replay local data while upstream pipe polling remains active unless a destructive download-refresh bootstrap is mutating local pipe/storage state.
- **Entry point:** `POST /admin/refresh-topic` in `broker/src/main/java/com/messaging/broker/http/RefreshController.java`.
- **Main files:** `RefreshCoordinator.java`, `RefreshInitiator.java`, `RefreshReplayWindowResolver.java`, `RefreshResetService.java`, `RefreshReplayService.java`, `RefreshReadyService.java`, `RefreshRecoveryService.java`.
- **Flow:** Persist `RESET_SENT`, resolve the replay window, clear ACK records, broadcast RESET, reset each ACKing group to the replay start offset, replay until the captured target is reached, broadcast READY, wait for READY ACKs, clear refresh state, and resume ACK reconciliation.
- **Data:** `RefreshContext`, `data-refresh-state.properties`, consumer offsets, ACK store.
- **Events:** `RESET`, `RESET_ACK`, batches, `READY`, `READY_ACK`.
- **External systems:** POS consumers and parent pipe.
- **POS dependency:** This feature is the POS state machine.
- **Compaction:** Replay reads compacted plus active segments and delivery-time filtering remains active.
- **Failures:** Retries RESET every 5s, READY every 10s, abort watchdog at 10 minutes, persisted recovery after restart.
- **Tests:** Refresh unit/integration specs and numerous journey tests.
- **Coverage gaps:** Operational behavior after `ABORTED` still requires manual policy definition.
- **Risks:** Late joiners and disconnect races are complex; one reconnect journey was flaky in the aggregate run.

See [POS state](07-pos-machine-state.md).

## Log Compaction

- **Business purpose:** Suppress superseded keyed records and reclaim sealed-segment disk space.
- **Entry point:** scheduled `CompactionScheduler.compact()` or `POST /admin/compaction/trigger`.
- **Main files:** all files under `broker/src/main/java/com/messaging/broker/compaction/` plus `SegmentManager.java`.
- **Flow:** Select sealed windows after checkpoint, stream records, drop stale versions and expired latest tombstones, atomically install compacted files, replace segment map, update checkpoint/index.
- **Data:** Segment files and RocksDB compaction column family.
- **Events:** Keyed `MESSAGE` and `DELETE`.
- **External systems:** Local filesystem and RocksDB.
- **POS dependency:** Delivery must remain safe while POS consumers replay or have batches in flight.
- **Compaction:** This is the compaction implementation; active segments are not rewritten.
- **Failures:** Single-flight guard, CPU/heap skip guards, staging recovery, per-topic error isolation.
- **Tests:** Compaction unit/integration and nine journey suites.
- **Coverage gaps:** No test confirms that a no-op compaction rewrite is skipped.
- **Risks:** Heavy IO/CPU/GC, force-roll race, in-flight file-region lifetime, and all-records-deleted offset gaps.

See [Compaction handling](08-compaction-handling.md).

## Embedded Annotation Consumers

- **Business purpose:** Run broker-local `@Consumer` handlers without the remote TCP registry.
- **Entry point:** `broker/src/main/java/com/messaging/broker/consumer/ConsumerAnnotationProcessor.java`.
- **Main files:** `ConsumerDeliveryManager.java`, `ConsumerContext.java`, shared `@Consumer` and `RetryPolicy`.
- **Flow:** Discover annotated `MessageHandler` beans, restore offsets, poll storage, invoke handler, apply retry policy, persist progress.
- **Data:** Consumer context and shared offset properties.
- **Events:** Internal `MessageRecord` to handler calls.
- **External systems:** None.
- **POS dependency:** None confirmed; test consumer uses the separate client module.
- **Compaction:** Local delivery checks `CompactionIndex` before invoking handlers.
- **Failures:** `EXPONENTIAL_THEN_FIXED`, `SKIP`, or `PAUSE` policy from annotation.
- **Tests:** `ConsumerAnnotationProcessorSpec`, `ConsumerDeliveryManagerSpec`, `ConsumerContextSpec`.
- **Coverage gaps:** No production local consumer implementation was found.
- **Risks:** It shares storage and offset resources with remote delivery but follows a separate execution path.

## Modern Client Library

- **Business purpose:** Make a Micronaut service a consumer by annotating a `MessageHandler`.
- **Entry point:** `client/src/main/java/com/messaging/client/ClientConsumerManager.java`.
- **Main files:** `NettyTcpClient.java`, `ZeroCopyBatchDecoder.java`, common `MessageHandler.java`.
- **Flow:** Discover beans, resolve properties, create one connection per topic/group, subscribe, route batches, call `onReset`/`onReady`, reconnect with 5s-to-60s backoff.
- **Data:** In-memory connection maps and decoded `ConsumerRecord` lists.
- **Events:** Modern subscribe/readiness/batch/control messages.
- **External systems:** Broker TCP endpoint.
- **POS dependency:** Implements the modern POS-side reset/ready callbacks.
- **Compaction:** Receives already-filtered records.
- **Failures:** Per-connection health check every 10s; reconnect on disconnect/dead channel.
- **Tests:** `client/src/integrationTest/groovy/com/messaging/client/ClientConsumerManagerIntegrationSpec.groovy`.
- **Coverage gaps:** Multi-handler reset semantics and long reconnect storms are not load tested.
- **Risks:** All handlers registered for a topic are invoked for data and control events, even when multiple groups share that topic.

## Monitoring And Administration

- **Business purpose:** Expose health, metrics, thread/resource diagnostics, runtime log levels, refresh status, and compaction controls.
- **Entry point:** controllers under `broker/src/main/java/com/messaging/broker/http/`.
- **Main files:** monitoring package, `application.yml`, controllers.
- **Flow:** Micronaut HTTP requests read process or service state and return maps/text; scheduled monitors update metrics/logs.
- **Data:** In-memory metrics and JVM MXBeans.
- **Events:** HTTP only.
- **External systems:** Prometheus scraper and operators.
- **POS dependency:** Used to diagnose deployed store nodes.
- **Compaction:** Exposes status/trigger and compaction metrics.
- **Failures:** Most endpoints return error maps rather than typed HTTP error statuses.
- **Tests:** controller and monitoring integration/unit specs.
- **Coverage gaps:** Authentication/authorization behavior is not confirmed.
- **Risks:** Diagnostics and test APIs expose powerful operations if reachable.

See [API catalog](04-api-catalog.md).

## Pipe Consistency Detection

- **Business purpose:** Detect (never repair) whether this POS holds everything its parent holds per topic — missed records, stale keys, zombie keys (deletes missed while offline past tombstone retention), and fabricated keys (entries an authoritative verifier — the cloud — never had: corrupted/injected index) — despite both sides compacting independently.
- **Entry point:** `broker/src/main/java/com/messaging/broker/consistency/PipeConsistencyService.java`.
- **Main files:** `KeyspaceDigest.java`, `ParentConsistencyClient.java`, `PipeConsistencyReport.java`, `http/PipeConsistencyController.java`, `http/PipeConsistencyAdminController.java`, `CompactionIndex.forEachEntry` (both backends).
- **Flow:** admin POST → resolve target (`TopologyManager.getCurrentParentUrl()` or registry URL) → per topic: child scans its compaction index into 64 XOR-bucket digests at its own watermark → one GET to the parent → equal digests ⇒ CONSISTENT; else fetch mismatched buckets (one request/scan) → classify differences via parent record physical-presence + index state → report + Micrometer gauges.
- **Data:** compaction index only (`key → latestOffset`, compaction-invariant); no new persistent state; bounded in-memory report ring (16).
- **Events:** HTTP + `PipeConsistencyScheduler` (`@Scheduled`, default 6h interval / 10m initial delay, bean exists only when `pipe.consistency.enabled=true`); skips with a logged reason when a check runs, the parent is unassigned (offline POS), or heap pressure is high. Admin API remains available regardless.
- **External systems:** parent broker; cloud-server must mirror the three served endpoints (pending, separate repo).
- **POS dependency:** offline → `UNREACHABLE` verdict, no retries; reshuffle → target re-resolved per call, watermark clamps to a lagging parent (`CONSISTENT_UP_TO`).
- **Compaction:** verdict is compaction-invariant by construction; expired-tombstone asymmetries resolved by the classify step's physical-presence check.
- **Failures:** parent 404 → `UNSUPPORTED_PARENT`; index/storage error → `ERROR`; oversized divergence capped by `max-drilldown-buckets`/`max-classify-entries`/`max-bucket-entries`.
- **Tests:** `KeyspaceDigestSpec`, `PipeConsistencyServiceSpec`, `CompactionIndexForEachEntrySpec` (unit); `PipeConsistencyEndpointsIntegrationSpec`, `PipeConsistencyDisabledIntegrationSpec` (integration).
- **Coverage gaps:** no two-broker journey yet; no cloud-side implementation; no index-rebuild tool (a wiped RocksDB reports falsely inconsistent).
- **Risks:** trusts the compaction index as source of truth (already load-bearing for delivery filtering); fail-closed default `pipe.consistency.enabled=false`.

See [Pipe Consistency API](04-api-catalog.md#pipe-consistency-api) and `readme/PIPE_CONSISTENCY_V2.md`.
