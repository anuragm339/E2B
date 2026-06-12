# System Overview

Related: [Module map](02-module-map.md), [Features](03-feature-catalog.md), [Events](06-event-kafka-flow.md), [Runtime config](11-runtime-config.md), [Risks](13-risk-and-edge-cases.md).

## Purpose

The repository implements a store-oriented, Kafka-like messaging provider. It owns topic storage, offsets, consumer groups, binary delivery, legacy protocol compatibility, parent-cloud replication, refresh orchestration, ACK auditing, and log compaction. It does not depend on Apache Kafka. The module list is declared in `settings.gradle`; runtime wiring starts in `broker/src/main/java/com/messaging/broker/Application.java`.

The dominant deployment model is a broker running near POS consumers:

```text
Cloud registry/topology
        |
        v
Parent HTTP pipe ----> Local broker storage ----> POS/service consumers
                             ^                         |
                             |                         v
                      TCP producers             ACK / RESET / READY
```

Sources:

- Parent discovery: `broker/src/main/java/com/messaging/broker/core/CloudRegistryClient.java`
- Parent lifecycle: `broker/src/main/java/com/messaging/broker/core/TopologyManager.java`
- Pipe polling: `pipe/src/main/java/com/messaging/pipe/HttpPipeConnector.java`
- Local storage: `storage/src/main/java/com/messaging/storage/filechannel/FileChannelStorageEngine.java`
- Consumer orchestration: `broker/src/main/java/com/messaging/broker/consumer/ConsumerRegistry.java`
- Client library: `client/src/main/java/com/messaging/client/ClientConsumerManager.java`

## Architectural Style

- Java 17 source compatibility, Gradle multi-project build, Micronaut 4.2.1: `build.gradle`
- Netty TCP data plane with a custom binary protocol: `network/src/main/java/com/messaging/network/tcp/NettyTcpServer.java`
- Micronaut HTTP control and pipe plane: `broker/src/main/java/com/messaging/broker/http/`, `pipe/src/main/java/com/messaging/pipe/PipeServer.java`
- Append-only segmented local logs plus dense indexes: `storage/src/main/java/com/messaging/storage/segment/Segment.java`
- SQLite segment metadata: `storage/src/main/java/com/messaging/storage/metadata/SegmentMetadataStore.java`
- RocksDB ACK and compaction state: `broker/src/main/java/com/messaging/broker/compaction/SharedRocksDb.java`
- Properties files for small operational state: `broker/src/main/java/com/messaging/broker/consumer/PropertiesFileStore.java`
- Scheduled polling rather than push notification for delivery: `broker/src/main/java/com/messaging/broker/consumer/DeliveryScheduler.java`

## Core Data Flow

1. A producer sends `DATA`, or a parent returns `MessageRecord` objects from the pipe.
2. The broker appends each record to topic partition `0`.
3. The compaction index records the latest offset for non-null message keys.
4. Adaptive delivery gates on consumer readiness, refresh state, storage watermark, in-flight state, and pending ACK.
5. The storage layer returns a file-backed `DeliveryBatch`; the network layer sends a `BATCH_HEADER` followed by raw segment bytes.
6. The client decoder parses the batch and automatically emits `BATCH_ACK`.
7. The broker commits the next delivery offset and asynchronously writes per-record ACK entries.

Sources: `broker/src/main/java/com/messaging/broker/handler/DataHandler.java`, `broker/src/main/java/com/messaging/broker/core/BrokerService.java`, `broker/src/main/java/com/messaging/broker/consumer/BatchDeliveryService.java`, `network/src/main/java/com/messaging/network/codec/ZeroCopyBatchDecoder.java`, `broker/src/main/java/com/messaging/broker/consumer/BatchAckService.java`.

See [Event flow](06-event-kafka-flow.md) and [Data model](05-data-model.md).

## Startup And Shutdown

`BrokerService.onApplicationEvent` performs:

1. `storage.recover()`; recovery failure aborts startup.
2. Optional ACK-store seeding; failure is logged and startup continues.
3. Network message and disconnect handler registration.
4. TCP server startup.
5. Embedded annotation-consumer delivery startup.
6. Adaptive remote-consumer delivery startup.
7. Topology manager and parent pipe startup.

Source: `broker/src/main/java/com/messaging/broker/core/BrokerService.java`.

Handler registration is separately performed on `StartupEvent` by `broker/src/main/java/com/messaging/broker/config/HandlerInitializer.java`. This registers `DATA`, `SUBSCRIBE`, `COMMIT_OFFSET`, `RESET_ACK`, `READY_ACK`, and `BATCH_ACK`.

Shutdown order is network ingress, adaptive delivery, topology/pipe, embedded delivery, shared executors, and storage. Sources: `BrokerService.java` and `broker/src/main/java/com/messaging/broker/core/ShutdownCoordinator.java`.

## Consistency Model

- Segment append order is protected by a fair append lock in `storage/src/main/java/com/messaging/storage/segment/SegmentManager.java`.
- Consumer delivery state is keyed by `group:topic`, not ephemeral socket address: `broker/src/main/java/com/messaging/broker/model/DeliveryKey.java`.
- Modern offsets use next-to-deliver semantics in `broker/src/main/java/com/messaging/broker/consumer/BatchDeliveryService.java`.
- Legacy delivery stores the last acknowledged offset per topic in `broker/src/main/java/com/messaging/broker/legacy/LegacyConsumerDeliveryManager.java`.
- ACK processing is at-least-once oriented: an ACK timeout reverts the in-memory offset; a lost persisted offset causes replay, not omission.
- Pipe progress advances only after the broker handler returns success: `pipe/src/main/java/com/messaging/pipe/HttpPipeConnector.java`.
- Compaction preserves original offsets and filters stale records before physical rewrite: `broker/src/main/java/com/messaging/broker/compaction/CompactionRewriter.java`.

The mixed modern/legacy offset semantics are an important maintenance risk. See [Risks](13-risk-and-edge-cases.md#offset-semantics).

## Concurrency And Resource Model

- Netty boss/worker pools: `network/src/main/java/com/messaging/network/tcp/NettyTcpServer.java`
- Per-topic fair delivery scheduler: `broker/src/main/java/com/messaging/broker/consumer/TopicFairScheduler.java`
- Dedicated ACK, ACK-storage, storage-read, compaction, registry, refresh, consumer, and flush executors: `broker/src/main/java/com/messaging/broker/config/ExecutorFactory.java`
- One shared client-side Netty event loop, but one connection per `topic:group`: `client/src/main/java/com/messaging/client/ClientConsumerManager.java`
- Pipe and topology each own scheduler/executor lifecycle: `pipe/src/main/java/com/messaging/pipe/HttpPipeConnector.java`, `TopologyManager.java`

Performance guardrails are implemented through bounded executor queues, per-topic semaphores, batch byte limits, storage-read/send timeouts, compaction CPU/heap guards, chunked ACK replay, streaming pipe parsing, and page-cache advisory calls. Sources are detailed in [Runtime config](11-runtime-config.md) and [Compaction](08-compaction-handling.md).

## External Integrations

| Integration | Protocol | Source |
|---|---|---|
| Cloud registry | HTTP GET topology | `broker/src/main/java/com/messaging/broker/core/CloudRegistryClient.java` |
| Parent/cloud broker | HTTP streaming poll | `pipe/src/main/java/com/messaging/pipe/HttpPipeConnector.java` |
| POS/service consumers | Modern or legacy TCP | `network/src/main/java/com/messaging/network/tcp/NettyTcpServer.java` |
| Embedded modern consumers | TCP client library | `client/src/main/java/com/messaging/client/ClientConsumerManager.java` |
| SQLite test import | JDBC file URL | `broker/src/main/java/com/messaging/broker/http/TestDataController.java` |
| Prometheus | Micronaut management | `broker/src/main/resources/application.yml` |
| Linux page cache advice | JNA `posix_fadvise` | `storage/src/main/java/com/messaging/storage/filechannel/FileChannelStorageEngine.java` |

## Security Boundary

`common/src/main/java/com/messaging/common/http/AuthTokenClientFilter.java` only adds credentials to outbound Micronaut HTTP requests when `broker.http.auth.token` is configured. The main YAML instead defines `broker.registry.auth.bearer-token`; no code mapping between these keys was found.

No inbound HTTP authentication filter or `@Secured` controller annotation was found. The repository does configure Micronaut management endpoint sensitivity in `broker/src/main/resources/application.yml`, but enforcement is **not confirmed from code**. Treat admin, diagnostics, and test endpoints as requiring network-layer protection until verified.

See [Runtime config](11-runtime-config.md#security-and-authentication) and [Risks](13-risk-and-edge-cases.md#security).
