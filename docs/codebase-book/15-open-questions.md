# Open Questions

Related: [Risks](13-risk-and-edge-cases.md), [Tests](10-test-map.md), [Runtime config](11-runtime-config.md), [Debugging](12-debugging-guide.md).

Items below could not be confirmed from code or need an owner decision.

## Protocol And Integration

1. What is the canonical legacy service name: `price-quote` or `price-quote-service`?
   - Sources: `broker/src/main/resources/application.yml`, `test-consumer/src/main/resources/application.yml`, `BrokerLegacyConsumerIntegrationSpec.groovy`.
2. How does the external parent select topics when `HttpPipeConnector` does not send `topic`?
   - Sources: `HttpPipeConnector.java`, `PipeServer.java`.
3. Is multi-parent failover required, or is selecting the first parent intentional?
   - Source: `TopologyManager.java`.
4. Is `HEARTBEAT` planned or obsolete? It has a message code but no registered handler.
5. Is legacy producer ingestion supported? `LegacyEventDecoder` emits pipe-separated DATA payloads, while `DataHandler` expects JSON. Not confirmed from an end-to-end test.

## Security

1. What component enforces authentication for inbound admin/diagnostic/test HTTP endpoints?
2. Should `AUTH_BEARER_TOKEN` map to `broker.http.auth.token` instead of the currently documented registry key?
3. Should `/test/**` be disabled outside test/development environments?
4. Is arbitrary SQLite file loading an intended production capability?

## Offset And State Semantics

1. Should all persisted consumer offsets be standardized as next-to-deliver, including legacy?
2. Should `CommitOffsetHandler` accept `storageHead + 1` for a caught-up modern consumer?
3. Does `DeliveryStateStore` participate in active remote delivery recovery, or is it retained legacy infrastructure? Its direct integration into `BatchDeliveryService` was not confirmed.
4. Should replay-window reset semantics remain consumer-type aware, or should legacy/modern offset conventions be unified first?
5. Should reconciliation include disconnected historical groups?

## Refresh/POS

1. After an abort, who resumes the pipe and ACK reconciliation, and under which conditions?
2. What is the expected operator workflow for persisted `ABORTED` contexts?
3. Should compaction pause during refresh or initial replay?
4. What caused the aggregate-only stale pending ACK in `ConsumerCrashDuringReplayJourneySpec`?
5. Should a refresh with no registered consumers pause/clear any local POS state, or is immediate `COMPLETED` correct?

## Storage And Compaction

1. Is the `MMapStorageEngine` name intentional even though the shared segment implementation uses `FileChannel`?
2. Should non-monotonic external offsets be rejected rather than appended?
3. Is per-record checksum validation required for POS power-loss/corruption detection?
4. What are the intended implementations of `StorageEngine.compact()` and `validateStorage()`?
5. Should no-op compaction windows be detected before rewriting?
6. What should `getEarliestOffset` return when only an active segment exists with a nonzero base offset?
7. Is active/recovered segment double-close possible or explicitly idempotent? Not confirmed from tests.
8. What general retention policy exists for non-superseded messages? Not confirmed from code.

## Runtime And Build

1. Which port defaults are canonical: YAML `19092/8082` or Docker `9092/8081`?
2. Where is Qodana executed, and is JDK 21 intentional for a Java 17 target?
3. Should Docker image builds run tests rather than `-x test`?
4. Should integration, journey, and system tests be dependencies of a CI aggregate task?
5. Should `storage/src/test/java/com/messaging/storage/mmap/MMapStorageEngineTest.java` be moved into the custom unit source set or deleted?
6. Are Gradle 9 deprecation warnings tracked?

## Planned: Local message bus (`@Producer`)

Design decisions still open for the planned local pub/sub bus — full context in
[ch.18 Roadmap](18-roadmap-local-message-bus.md) (PLANNED, not implemented):

1. Default `acks`: fire-and-forget (lightest on POS) vs wait-for-persist (safest)?
2. Durability of locally-produced events across a POS reboot — segment store vs lighter transient path?
3. Resource isolation — share the broker budget vs quota'd so the bus cannot starve cloud→till?
4. Refresh gate scope — per-topic vs global; reject-and-retry vs block the caller?

## Documentation Maintenance

Update this book when any of these change:

- protocol message codes or record binary format;
- service/topic mappings;
- offset semantics;
- refresh transitions/timeouts;
- storage file/schema format;
- compaction retention/checkpoint behavior;
- test source-set wiring;
- runtime ports/authentication.

The starting point for updates is [the index](00-index.md).
