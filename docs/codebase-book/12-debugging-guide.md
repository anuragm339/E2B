# Debugging Guide

Related: [Index](00-index.md), [Runtime config](11-runtime-config.md), [Errors/recovery](09-error-retry-recovery.md), [Tests](10-test-map.md), [Risks](13-risk-and-edge-cases.md).

## Broker Does Not Start

1. Check storage recovery exceptions from `BrokerService.java`.
2. Inspect `<dataDir>/<topic>/partition-0` for mismatched `.log/.index` pairs and staging files.
3. Check TCP bind port from `broker.network.port`.
4. Check RocksDB lock/path messages; ACK seeding failure alone should not stop startup.
5. Confirm Java and Gradle compatibility: source target 17, current local runtime observed 21.

Relevant files: `BrokerService.java`, `DefaultStorageRecoveryService.java`, `NettyTcpServer.java`, `SharedRocksDb.java`.

## Consumer Receives No Data

Check in this order:

1. Registration exists in `ConsumerRegistry`.
2. READY_ACK opened readiness state.
3. Refresh is not `RESET_SENT`.
4. Storage head is at or above consumer offset.
5. No `DeliveryKey` is in-flight or waiting for ACK.
6. Failure backoff/max-failure gate is not active.
7. Topic fairness scheduler is still rescheduling.
8. Compaction filter did not advance an all-stale batch.

Files: `SubscribeHandler.java`, `ConsumerReadinessManager.java`, `WatermarkGatePolicy.java`, `RefreshGatePolicy.java`, `BatchDeliveryService.java`, `DeliveryScheduler.java`.

Useful logs:

- `event=consumer.registered`
- `event=ready_ack.processed`
- `event=batch_delivery.blocked`
- `event=batch_delivery.storage_read_slow`
- `event=delivery_scheduler.reschedule_failed`

## Pending ACK Never Clears

Inspect:

- `group:topic` pending offset and send time in `InMemoryInFlightDeliveryStore`;
- BATCH_ACK topic/group payload;
- ACK executor rejection;
- timeout scheduling generation;
- disconnect cleanup.

Sources: broker `BatchAckHandler.java`, `BatchAckService.java`, `InMemoryInFlightDeliveryStore.java`, `ConsumerRegistry.unregisterConsumer`.

Known signal: `ConsumerCrashDuringReplayJourneySpec` once logged pending ACK age above 25 seconds with a 5-second timeout; exact rerun passed.

## Refresh Is Stuck

Use:

- `GET /admin/refresh-status?topic=...`
- `GET /admin/refresh-current`
- refresh metrics and logs.

By state:

- `RESET_SENT`: compare expected consumers and RESET ACK set; check client `onReset`, connection, and service mapping.
- `REPLAYING`: inspect `consumer-offsets.properties`, storage head, pending ACKs, and replay gap metric.
- `READY_SENT`: inspect READY ACK set and client `onReady`; retry occurs every 10 seconds.
- `ABORTED`: automatic operational recovery policy is not confirmed; inspect pipe/reconciliation state.

Files: `RefreshCoordinator.java`, phase services, `RefreshStateStore.java`.

## Pipe Is Not Advancing

1. Confirm topology returned a parent.
2. Confirm parent `/health` returns success.
3. Inspect `pipe-offset.properties`.
4. Check `event=pipe_connector.connected`, paused/resumed logs, and poll failures.
5. Verify parent poll endpoint topic behavior.
6. Check duplicate guard against local topic head.
7. Check storage exception from `BrokerService.handlePipeMessage`.

Files: `TopologyManager.java`, `CloudRegistryClient.java`, `HttpPipeConnector.java`, `BrokerService.java`.

## Compaction Problems

Use `GET /admin/compaction/status`, logs, and segment directory.

Check:

- compaction enabled and schedule;
- CPU/heap guard skipped run;
- sealed segment count/minimum;
- checkpoint;
- `.compacting.*` leftovers;
- matching `.compacted.log/.index`;
- `replaceSegments` errors;
- delivery pending ACKs/file-region failures during replacement.

Files: `CompactionScheduler.java`, `CompactionPlanner.java`, `CompactionRewriter.java`, `SegmentManager.java`.

Do not call `StorageEngine.compact()` expecting broker compaction; current engine implementations leave it as a placeholder.

## Legacy Client Does Not Register

1. First byte must be legacy REGISTER ordinal `0`.
2. Inspect `Detected protocol: LEGACY`.
3. Compare exact service name with `legacy-clients.service-topics`.
4. Confirm legacy clients are enabled.
5. Check `Unknown legacy service` logs.

Files: `DefaultProtocolDetectionService.java`, `LegacyEventDecoder.java`, `SubscribeHandler.java`, `LegacyClientConfig.java`, `application.yml`.

Current confirmed mismatch: `price-quote-service` versus configured `price-quote`.

## Offset Problems

Identify semantics first:

- modern: next offset to deliver;
- legacy: last acknowledged offset;
- storage head: last stored offset;
- pipe: last successful parent offset.

Inspect `consumer-offsets.properties`, then trace writes in `BatchAckService`, `LegacyConsumerDeliveryManager`, `CommitOffsetHandler`, and refresh reset.

For gaps after compaction, verify reads seek first offset greater than or equal to requested in `Segment.java`.

## Resource Pressure

HTTP diagnostics:

- `/diagnostics/threads/top-cpu`
- `/diagnostics/threads/top-memory`
- `/diagnostics/threads/by-category`
- `/diagnostics/threads/deadlocks`

Metrics/logs:

- storage slow-read warnings;
- executor queue descriptions;
- compaction CPU/heap skips;
- pending ACK age;
- pipe streaming failures;
- page-cache advisory logs.

Files: monitoring package, `ExecutorFactory.java`, `FileChannelStorageEngine.java`.

## Test Reproduction

```bash
./gradlew test
./gradlew :broker:integrationTest --tests com.messaging.broker.core.BrokerLegacyConsumerIntegrationSpec --rerun-tasks
./gradlew :broker:journeyTest --tests com.messaging.broker.systemtest.journey.ConsumerCrashDuringReplayJourneySpec --rerun-tasks
./gradlew :broker:systemTest
```

Reports are written under `<module>/build/reports/tests/<task>/index.html`.

## Safe Change Checklist

Before changing delivery/storage/refresh/compaction:

- Memory: preserve streaming/chunking and batch limits.
- CPU: avoid per-record global scans or busy polling.
- IO: preserve group commit, atomic files, and bounded rewrites.
- Network: preserve header/payload ordering and one-topic/group connection assumption.
- Latency: preserve scheduler fairness and timeout ownership.
- Existing flow: test modern, legacy, pipe, restart, refresh, and compaction intersections.
- Compatibility: preserve legacy event ordinals and modern message codes.
- Compaction: preserve original offsets and staging publication.
- POS state: preserve RESET/READY ACK gates and reconnect cleanup.
