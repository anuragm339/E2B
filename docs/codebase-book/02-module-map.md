# Module Map

Related: [Overview](01-system-overview.md), [Features](03-feature-catalog.md), [Tests](10-test-map.md), [Runtime config](11-runtime-config.md).

## Build Topology

Modules are declared in `settings.gradle`:

```text
common <- storage
common <- network
common + storage <- pipe
common + storage + network + pipe <- broker
common + network <- client
common + network + client <- test-consumer
test-consumer <- broker journey/system test classpaths
```

Dependency declarations are in each module's `build.gradle`.

## `common`

Purpose: stable contracts, annotations, models, exceptions, and outbound HTTP auth.

- APIs: `common/src/main/java/com/messaging/common/api/`
  - `StorageEngine`, `BatchReadableStorage`, `NetworkServer`, `NetworkClient`, `PipeConnector`
  - `MessageHandler`, `ErrorHandler`, `NoOpErrorHandler`
- Models: `common/src/main/java/com/messaging/common/model/`
  - `BrokerMessage`, `MessageRecord`, `ConsumerRecord`, `DeliveryBatch`
  - `ByteArrayDeliveryBatch`, `EventType`, `HealthStatus`, `TopologyResponse`
- Consumer annotations: `common/src/main/java/com/messaging/common/annotation/`
- Exceptions: `common/src/main/java/com/messaging/common/exception/`
- Outbound token filter: `common/src/main/java/com/messaging/common/http/AuthTokenClientFilter.java`

Tests: `common/src/unitTest/groovy/`, `common/src/integrationTest/groovy/`. See [Test map](10-test-map.md#common).

## `storage`

Purpose: append-only topic storage, segment/index lifecycle, crash recovery, watermarks, and SQLite metadata.

- File-channel engine: `storage/src/main/java/com/messaging/storage/filechannel/FileChannelStorageEngine.java`
- Alternate DI engine: `storage/src/main/java/com/messaging/storage/mmap/MMapStorageEngine.java`
- Segment core:
  - `storage/src/main/java/com/messaging/storage/segment/Segment.java`
  - `storage/src/main/java/com/messaging/storage/segment/SegmentManager.java`
  - `storage/src/main/java/com/messaging/storage/segment/DefaultStorageRecoveryService.java`
  - `storage/src/main/java/com/messaging/storage/segment/SegmentAccess.java`
- Metadata:
  - `storage/src/main/java/com/messaging/storage/metadata/SegmentMetadata.java`
  - `storage/src/main/java/com/messaging/storage/metadata/SegmentMetadataStore.java`
  - `storage/src/main/java/com/messaging/storage/metadata/SegmentMetadataStoreFactory.java`
- Watermarks: `storage/src/main/java/com/messaging/storage/watermark/StorageWatermarkTracker.java`

`MMapStorageEngine` delegates to the same `SegmentManager`/`Segment` implementation; the active segment implementation uses `FileChannel`. The class name does not prove memory mapping.

Tests: `storage/src/unitTest/groovy/`, `storage/src/integrationTest/groovy/`. `storage/src/test/java/com/messaging/storage/mmap/MMapStorageEngineTest.java` is outside the custom unit source set and is not run by `:storage:test`. See [Open questions](15-open-questions.md).

## `network`

Purpose: TCP server/client, modern codecs, zero-copy batch framing, protocol detection, legacy codecs, and network metrics.

- TCP: `network/src/main/java/com/messaging/network/tcp/`
  - `NettyTcpServer`, `NettyTcpClient`
- Modern codecs: `network/src/main/java/com/messaging/network/codec/`
  - `BinaryMessageDecoder`, `BinaryMessageEncoder`
  - `ZeroCopyBatchDecoder`, `BatchDecodedEvent`, client `BatchAckHandler`
- Handlers: `network/src/main/java/com/messaging/network/handler/`
- Legacy compatibility: `network/src/main/java/com/messaging/network/legacy/`
  - `ProtocolDetectionDecoder`, `DefaultProtocolDetectionService`
  - `LegacyEventDecoder`, `LegacyEventEncoder`, `LegacyConnectionState`
- Legacy event model: `network/src/main/java/com/messaging/network/legacy/events/`
- Metrics: `network/src/main/java/com/messaging/network/metrics/`

Tests: `network/src/unitTest/groovy/`, `network/src/integrationTest/groovy/`.

## `pipe`

Purpose: parent-to-child replication over HTTP and server-side polling.

- Client connector: `pipe/src/main/java/com/messaging/pipe/HttpPipeConnector.java`
- Parent endpoint: `pipe/src/main/java/com/messaging/pipe/PipeServer.java`
- Metrics: `pipe/src/main/java/com/messaging/pipe/PipeMetrics.java`

Tests: `pipe/src/unitTest/groovy/`, `pipe/src/integrationTest/groovy/`.

## `broker`

Purpose: application composition and nearly all business workflows.

- Application and lifecycle:
  - `broker/src/main/java/com/messaging/broker/Application.java`
  - `broker/src/main/java/com/messaging/broker/core/BrokerService.java`
  - `broker/src/main/java/com/messaging/broker/core/ShutdownCoordinator.java`
- Topology and replication: `broker/src/main/java/com/messaging/broker/core/`
- Protocol handlers: `broker/src/main/java/com/messaging/broker/handler/`
- Consumer lifecycle/delivery/refresh/state: `broker/src/main/java/com/messaging/broker/consumer/`
- ACK store and reconciliation: `broker/src/main/java/com/messaging/broker/ack/`
- Compaction: `broker/src/main/java/com/messaging/broker/compaction/`
- Legacy merge delivery: `broker/src/main/java/com/messaging/broker/legacy/`
- HTTP controllers: `broker/src/main/java/com/messaging/broker/http/`
- DTO/value types: `broker/src/main/java/com/messaging/broker/model/`
- Metrics/logging/health: `broker/src/main/java/com/messaging/broker/monitoring/`
- DI/executors/handler setup: `broker/src/main/java/com/messaging/broker/config/`
- Runtime configuration: `broker/src/main/resources/application.yml`

Test source sets:

- Unit: `broker/src/unitTest/groovy/`
- Integration: `broker/src/integrationTest/groovy/`
- In-process journey: `broker/src/systemTest/groovy/`, mapped to `journeyTest`
- Separate-process black-box: `broker/src/blackboxSystemTest/groovy/`, mapped to `systemTest`

The unusual mapping is defined in `broker/build.gradle`.

## `client`

Purpose: embedded modern consumer framework.

Only production class: `client/src/main/java/com/messaging/client/ClientConsumerManager.java`.

It discovers `@Consumer` handlers, creates one TCP connection per `topic:group`, shares one Netty event loop, reconnects with exponential backoff, handles `RESET`/`READY`, and routes decoded batches to all handlers for a topic.

Tests: `client/src/integrationTest/groovy/com/messaging/client/ClientConsumerManagerIntegrationSpec.groovy`.

## `test-consumer`

Purpose: runnable consumer process and journey/system-test fixture.

- Application: `test-consumer/src/main/java/com/example/consumer/ConsumerApplication.java`
- Modern handler: `test-consumer/src/main/java/com/example/consumer/GenericConsumerHandler.java`
- Legacy direct connection/service: `test-consumer/src/main/java/com/example/consumer/service/LegacyConsumerService.java`
- Legacy protocol mirror: `test-consumer/src/main/java/com/example/consumer/legacy/`
- Consumer logging API: `test-consumer/src/main/java/com/example/consumer/http/LoggingController.java`
- Runtime config: `test-consumer/src/main/resources/application.yml`

No direct test source is present; it is exercised from broker journey and black-box tests.

## Repository-Level Assets

- Root build: `build.gradle`, `settings.gradle`, `gradle/`, `gradlew`
- Container: `Dockerfile`
- Static-analysis config: `qodana.yaml`
- Existing operating notes: `README.md`, `BUGS.md`, `TODO.md`, `CLAUDE.md`
- Audit documents: `docs/CLAUDE_MD_AUDIT_COMPARISON.md`, `docs/CONCURRENCY_AUDIT_COMPARISON.md`
- Monitoring dashboards: `html-dashboards/` is currently untracked in Git and was not modified.

Existing prose was used only as a navigation aid; behavior in this book is based on code and executed tests.
