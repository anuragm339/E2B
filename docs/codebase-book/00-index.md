# Codebase Book Index

Repository: `kafka-like-messaging`  
Verified against the working tree on 2026-06-11.

This book is a source-backed map of the repository. Start with the chapter that owns the behavior, then follow its links to the API, data, event, state, test, and debugging chapters.

## Chapters

1. [System overview](01-system-overview.md)
2. [Module map](02-module-map.md)
3. [Feature catalog](03-feature-catalog.md)
4. [API catalog](04-api-catalog.md)
5. [Data model](05-data-model.md)
6. [Event and Kafka-like flow](06-event-kafka-flow.md)
7. [POS machine state](07-pos-machine-state.md)
8. [Compaction handling](08-compaction-handling.md)
9. [Error, retry, and recovery](09-error-retry-recovery.md)
10. [Test map](10-test-map.md)
11. [Runtime configuration](11-runtime-config.md)
12. [Debugging guide](12-debugging-guide.md)
13. [Risks and edge cases](13-risk-and-edge-cases.md)
14. [Glossary](14-glossary.md)
15. [Open questions](15-open-questions.md)

## Features

| Feature | Primary chapter | Entry point |
|---|---|---|
| Broker startup and shutdown | [Overview](01-system-overview.md#startup-and-shutdown) | `broker/src/main/java/com/messaging/broker/core/BrokerService.java` |
| Producer ingestion | [Features](03-feature-catalog.md#producer-ingestion) | `broker/src/main/java/com/messaging/broker/handler/DataHandler.java` |
| Parent-to-child pipe replication | [Features](03-feature-catalog.md#topology-and-pipe-replication) | `broker/src/main/java/com/messaging/broker/core/TopologyManager.java` |
| Segment storage and recovery | [Data model](05-data-model.md#segment-storage) | `storage/src/main/java/com/messaging/storage/segment/SegmentManager.java` |
| Modern consumer subscription | [Features](03-feature-catalog.md#modern-consumer-subscription-and-readiness) | `broker/src/main/java/com/messaging/broker/handler/SubscribeHandler.java` |
| Zero-copy batch delivery | [Events](06-event-kafka-flow.md#modern-delivery-flow) | `broker/src/main/java/com/messaging/broker/consumer/BatchDeliveryService.java` |
| Legacy multi-topic delivery | [Features](03-feature-catalog.md#legacy-consumer-delivery) | `broker/src/main/java/com/messaging/broker/legacy/LegacyConsumerDeliveryManager.java` |
| Offset and per-record ACK persistence | [Features](03-feature-catalog.md#offset-and-ack-persistence) | `broker/src/main/java/com/messaging/broker/consumer/BatchAckService.java` |
| POS refresh/reset/replay/ready | [POS state](07-pos-machine-state.md) | `broker/src/main/java/com/messaging/broker/consumer/RefreshCoordinator.java` |
| Log compaction | [Compaction](08-compaction-handling.md) | `broker/src/main/java/com/messaging/broker/compaction/CompactionScheduler.java` |
| Embedded annotation consumers | [Features](03-feature-catalog.md#embedded-annotation-consumers) | `broker/src/main/java/com/messaging/broker/consumer/ConsumerAnnotationProcessor.java` |
| Client auto-discovery and reconnect | [Features](03-feature-catalog.md#modern-client-library) | `client/src/main/java/com/messaging/client/ClientConsumerManager.java` |
| Monitoring and diagnostics | [API catalog](04-api-catalog.md#diagnostics-and-management) | `broker/src/main/java/com/messaging/broker/http/ThreadDiagnosticsController.java` |
| Test-data ingestion | [API catalog](04-api-catalog.md#test-data-api) | `broker/src/main/java/com/messaging/broker/http/TestDataController.java` |

## APIs

### HTTP

- `GET /pipe/poll`: [Pipe API](04-api-catalog.md#pipe-api)
- `POST /admin/refresh-topic`: [Refresh API](04-api-catalog.md#refresh-api)
- `GET /admin/refresh-status`: [Refresh API](04-api-catalog.md#refresh-api)
- `GET /admin/refresh-current`: [Refresh API](04-api-catalog.md#refresh-api)
- `POST /admin/compaction/trigger`: [Compaction API](04-api-catalog.md#compaction-api)
- `GET /admin/compaction/status`: [Compaction API](04-api-catalog.md#compaction-api)
- `/admin/logging/**`: [Runtime logging API](04-api-catalog.md#runtime-logging-api)
- `/diagnostics/threads/**`: [Diagnostics API](04-api-catalog.md#diagnostics-and-management)
- `/test/**`: [Test-data API](04-api-catalog.md#test-data-api)
- `/health`, `/prometheus`, `/metrics`: [Management endpoints](04-api-catalog.md#diagnostics-and-management)

### TCP protocol

- `DATA`, `ACK`, `SUBSCRIBE`, `COMMIT_OFFSET`
- `RESET`, `READY`, `DISCONNECT`, `HEARTBEAT`
- `BATCH_HEADER`, `BATCH_ACK`, `RESET_ACK`, `READY_ACK`

See [Event and Kafka-like flow](06-event-kafka-flow.md#modern-wire-protocol). Definitions are in `common/src/main/java/com/messaging/common/model/BrokerMessage.java`.

## Topics And Events

This repository does **not** use Apache Kafka libraries or a Kafka broker. It implements Kafka-like topics, offsets, consumer groups, ACKs, and compaction itself. See [Event and Kafka-like flow](06-event-kafka-flow.md).

- Record events: `MESSAGE`, `DELETE` in `common/src/main/java/com/messaging/common/model/EventType.java`
- Legacy events: `REGISTER`, `MESSAGE`, `RESET`, `READY`, `ACK`, `EOF`, `DELETE`, `BATCH` in `network/src/main/java/com/messaging/network/legacy/events/EventType.java`
- Topic source: producer payload, pipe `MessageRecord.topic`, or legacy service mapping in `broker/src/main/resources/application.yml`
- Known configured POS topics: [Runtime configuration](11-runtime-config.md#legacy-service-topic-map)

## Database Tables And Persistent Entities

| Store/entity | Location | Details |
|---|---|---|
| `segment_metadata` SQLite table | Per topic: `<dataDir>/<topic>/segment_metadata.db` | [Data model](05-data-model.md#sqlite-segment-metadata) |
| Segment log/index files | `<dataDir>/<topic>/partition-0/` | [Data model](05-data-model.md#segment-storage) |
| ACK records | RocksDB default column family | [Data model](05-data-model.md#rocksdb) |
| Compaction latest-key/checkpoint data | RocksDB `compaction` column family | [Compaction](08-compaction-handling.md#persistent-index-and-checkpoints) |
| Consumer offsets | `consumer-offsets.properties` | [Data model](05-data-model.md#properties-state-files) |
| Refresh state | `data-refresh-state.properties` | [POS state](07-pos-machine-state.md#persisted-refresh-state) |
| Pipe offset | `pipe-offset.properties` | [Data model](05-data-model.md#properties-state-files) |
| Topology | `topology.properties` | [Runtime config](11-runtime-config.md#topology-and-pipe) |

No SQL migration files were found. The SQLite schema is created at runtime by `storage/src/main/java/com/messaging/storage/metadata/SegmentMetadataStore.java`.

## Important Classes

- Orchestration: `BrokerService`, `TopologyManager`, `RefreshCoordinator`, `ConsumerRegistry`
- Ingress: `DataHandler`, `HttpPipeConnector`, `PipeServer`
- Storage: `Segment`, `SegmentManager`, `FileChannelStorageEngine`, `DefaultStorageRecoveryService`
- Delivery: `DeliveryScheduler`, `TopicFairScheduler`, `BatchDeliveryService`, `BatchAckService`
- State: `ConsumerOffsetTracker`, `InMemoryInFlightDeliveryStore`, `RefreshStateStore`
- Network: `NettyTcpServer`, `NettyTcpClient`, `ZeroCopyBatchDecoder`, `ProtocolDetectionDecoder`
- Compaction: `CompactionIndex`, `CompactionPlanner`, `CompactionRewriter`, `CompactionScheduler`
- Client: `ClientConsumerManager`

Package ownership and paths are in [Module map](02-module-map.md).

## Test Suites

- Unit: `./gradlew test`
- Integration: `./gradlew integrationTest`
- Journey/end-to-end in process: `./gradlew :broker:journeyTest`
- Separate-process black-box: `./gradlew :broker:systemTest`
- Performance source sets: `./gradlew performanceTest` (currently no source)
- Build verification: `./gradlew build`
- Static analysis: Qodana configuration exists, but no local Qodana CLI or Gradle lint task was found.

Results and every suite are indexed in [Test map](10-test-map.md).

## Debugging Entries

- Broker does not start: [Startup failures](12-debugging-guide.md#broker-does-not-start)
- Consumer receives no data: [Delivery stalls](12-debugging-guide.md#consumer-receives-no-data)
- Pending ACK never clears: [ACK stalls](12-debugging-guide.md#pending-ack-never-clears)
- Refresh stuck: [Refresh debugging](12-debugging-guide.md#refresh-is-stuck)
- Pipe not advancing: [Pipe debugging](12-debugging-guide.md#pipe-is-not-advancing)
- Compaction stalls or loses delivery: [Compaction debugging](12-debugging-guide.md#compaction-problems)
- Legacy client not registering: [Legacy registration](12-debugging-guide.md#legacy-client-does-not-register)
- Disk/memory/CPU pressure: [Resource debugging](12-debugging-guide.md#resource-pressure)

Related: [Risks](13-risk-and-edge-cases.md), [Open questions](15-open-questions.md).
