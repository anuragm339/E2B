# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

This is the **provider/broker** half of a Kafka-like distributed messaging system. The broker accepts messages from a parent broker (via HTTP pipe polling) or local producers, persists them to a segment-based storage layer, and delivers them to consumers over a binary TCP protocol. The neighbouring repos (`../consumer-app`, `../monitoring`, `../cloud-server`) make up the rest of the system but are out of scope for this directory.

## Module Layout

```
common/   shared APIs, models, exceptions, annotations (no dependencies)
storage/  segment-based storage engines (mmap & file-channel), metadata
network/  Netty TCP server/client, binary + legacy codecs, protocol handlers
pipe/     parent-broker HTTP polling + pipe server for child brokers
client/   embedded broker-as-library helper (used by tests/tools)
broker/   orchestration: delivery, refresh, topology, ACK, compaction, metrics
```

`broker/` depends on every other module. `settings.gradle` also includes `test-consumer` which is used by journey tests only.

## Pluggable Boundaries

The broker module is deliberately decoupled from concrete storage and transport implementations. New backends do **not** require changes to broker code:

- `StorageEngine` (`common/api`) — append/read/offset contract
- `BatchReadableStorage` — optional capability for storage engines that can produce a `DeliveryBatch` (used for the zero-copy delivery path)
- `NetworkServer` — transport contract; the broker calls `sendBatch(...)` and the transport owns batch encoding + framing
- `DeliveryBatch` — storage-to-transport handoff unit (replaces the older direct exposure of Netty `FileRegion` through the broker API)

Broker code does not branch on concrete storage or transport types.

## Build Commands

Java 17 is the source/target baseline (`build.gradle` subprojects block).

```bash
./gradlew build                  # compile + run unit tests (Spock) for all modules
./gradlew :broker:build          # build a single module
./gradlew build -x test          # skip tests
./gradlew clean build
```

## Testing

This project uses **Spock 2.3 + Groovy 4.0** as the primary test framework (`micronaut-test-spock` for `@MicronautTest`). **Mockito and Awaitility are not on the classpath.** There is no standard `src/test/` directory — every module has up to four custom source sets.

| Gradle task                  | Source root                       | Approx specs | Runtime |
|------------------------------|-----------------------------------|--------------|---------|
| `./gradlew test`             | `src/unitTest/groovy`             | 102          | Pure unit, Spock `Mock()` |
| `./gradlew integrationTest`  | `src/integrationTest/groovy`      | 75           | `@MicronautTest` with Micronaut context |
| `./gradlew journeyTest`      | `src/systemTest/{java,groovy}`    | 33           | In-process broker + test-consumer harness |
| `./gradlew systemTest`       | `src/blackboxSystemTest/{java,groovy}` | 5       | Separate-process black-box (broker + consumer launched as subprocesses) |
| `./gradlew performanceTest`  | `src/performanceTest/java`        | —            | Defined in root `build.gradle`; opt-in |

> ⚠️ **Source-root naming inversion:** the `journeyTest` task reads from `src/systemTest/`, and the `systemTest` task reads from `src/blackboxSystemTest/`. This is defined in `broker/build.gradle` lines 22–33. Always verify the task-to-directory mapping before placing new specs.

Running a single spec:

```bash
./gradlew :broker:test --tests "BatchDeliveryServiceSpec"
./gradlew :broker:integrationTest --tests "*RefreshIntegrationSpec*"
./gradlew :broker:journeyTest --tests "RefreshJourneySpec"
```

Run only one module's tests: `./gradlew :storage:test`.

## Running the Broker

Local development (uses `application.yml` defaults: `HTTP_PORT=8082`, `BROKER_PORT=19092`):

```bash
NODE_ID=local-001 REGISTRY_URL=http://localhost:8080 \
DATA_DIR=./data-local \
./gradlew :broker:run
```

Override ports via env vars: `NODE_ID`, `BROKER_PORT`, `HTTP_PORT`, `DATA_DIR`, `REGISTRY_URL`. All env vars map to `application.yml` keys.

## Architecture

### Message Flow

**Parent → broker (replication):**
```
CloudRegistry → TopologyManager (assigns parent)
HttpPipeConnector (HTTP poll, streaming JSON) → BrokerService.handlePipeMessage()
→ StorageEngine.append() → compactionIndex.updateKey()
```
Discovery of new messages is via **adaptive watermark-driven polling**, not producer→consumer signalling. The pipe is fully decoupled from delivery.

**Producer → broker → consumer:**
```
Producer/Client → NettyTcpServer → MessageHandlerRegistry → handler
→ StorageEngine.append()
ConsumerDeliveryManager (scheduleWithFixedDelay 200ms) → batch read
AdaptiveBatchDeliveryManager (watermark polling) → BatchDeliveryService.deliverBatch()
→ NetworkServer.sendBatch() → consumer
```

### Storage Layer

Kafka-style segments managed by `SegmentManager`:
- **Active segments** use sequential scan (no index)
- **Sealed segments** use sparse index (every 4 KB) for O(log n) lookups
- Segments roll over at configurable size (default 1 GB), CRC32 on read/write
- SQLite (`SegmentMetadataStore`) tracks segment boundaries
- Two implementations: `MMapStorageEngine` (memory-mapped) and `FileChannelStorageEngine`

### Network Protocol

Binary frame: `[Type:1B][MessageId:8B][PayloadLength:4B][Payload:var]`

Message types: `DATA` (0x01), `ACK` (0x02), `SUBSCRIBE` (0x03), `COMMIT_OFFSET` (0x04), `RESET` (0x05), `READY` (0x06), `BATCH_ACK` (0x07), `HEARTBEAT` (0x08).

A separate **legacy** wire protocol (`network/legacy/`) is multiplexed onto the same port via `ProtocolDetectionDecoder`.

### Data Refresh (RESET/READY) Workflow

Coordinated by `RefreshCoordinator` (`broker/consumer/`). State machine: `RESET_SENT → REPLAYING → READY_SENT → COMPLETED` (or `ABORTED`). Late-joining consumers are folded into the broadcast set via `registerLateJoiningConsumer`. The coordinator owns its own `ScheduledExecutorService` (2 threads) for watchdogs, RESET retries, replay checks, and READY timeouts. Expected-consumer list is in `application.yml` under `data-refresh.expected-consumers`.

### ACK + Compaction

- `BatchAckService` handles modern + legacy batch ACKs. Modern path uses `ackStorageExecutor` (separate pool) for async RocksDB writes of per-offset ACK records to avoid blocking delivery.
- `CompactionScheduler` + `CompactionRewriter` perform Kafka-style log compaction; `RocksDbCompactionIndex` tracks the highest-known stale offset per key for delivery-time filtering.

## Executor Inventory

`ExecutorFactory` (`broker/config`) defines six named pools registered with `ShutdownCoordinator`:

```
ackExecutor          fixed pool, default 4 threads — ACK processing
ackStorageExecutor   fixed pool, default 2 threads — async RocksDB writes after ACK
storageExecutor      fixed pool, default 4 threads — blocking storage reads
consumerScheduler    scheduled pool, default 4 threads — READY retries, ACK timeouts
dataRefreshScheduler scheduled pool, 2 threads — refresh coordinator (legacy bean)
flushScheduler       scheduled pool, 1 thread — periodic flushes
```

**However**, several classes construct their **own** schedulers outside the factory and manage their own `@PreDestroy` lifecycle — these are not registered with `ShutdownCoordinator`:

- `ConsumerDeliveryManager` (own scheduled pool, sized to CPU count)
- `RefreshCoordinator` (own 2-thread scheduled pool)
- `TopicFairScheduler` (own scheduled pool, default 2 threads)
- `FlushingPropertiesStore` (own single-thread scheduled executor per instance)
- `HttpPipeConnector` (own single-thread scheduled executor for the poll loop)

When adding new background work, **prefer injecting an `@Named(...)` pool from `ExecutorFactory`** instead of creating a new one — that gives you central lifecycle, naming, and (once added) a single hook for uncaught-exception handling.

## Configuration

Main file: `broker/src/main/resources/application.yml`.

Key paths:
- `broker.nodeId`, `broker.network.port` (default `19092`)
- `micronaut.server.port` (default `8082` — HTTP admin / metrics)
- `broker.storage.dataDir`, `broker.storage.segment-size`
- `broker.registry.url` (Cloud Registry endpoint)
- `data-refresh.expected-consumers`
- `executor.*.threads` — per-pool sizing (see Executor Inventory)

Env-var → YAML mapping is standard Micronaut: `NODE_ID`, `BROKER_PORT`, `HTTP_PORT`, `DATA_DIR`, `REGISTRY_URL`.

## Consumer Offset Tracking

Offsets are persisted to property files via `FlushingPropertiesStore` (periodic flush + final flush on stop). File: `<DATA_DIR>/consumer-offsets.properties`, format `<group>:<topic>=<offset>`. Always go through `ConsumerOffsetTracker.updateOffset()` so the metric + the property file stay in sync.

## Development Guidelines

### Storage layer
- Active segments scan sequentially; sealed segments must keep sparse-index consistency.
- Always validate CRC32 on reads. Test segment boundaries (end of segment, cross-segment).
- Recovery (`storage.recover()`) is called from `BrokerService.onApplicationEvent` — failures there abort startup.

### Network layer
- All I/O is `CompletableFuture`-based. Netty handlers must be stateless or scoped per channel.
- Binary protocol changes require version compatibility — the legacy decoder shares a port via `ProtocolDetectionDecoder`.

### Broker core
- `BrokerService` is the orchestrator — keep it focused; new responsibilities go in dedicated services.
- Consumer registration uses stable `group:topic` identifiers, not socket addresses.
- After `storage.append()`, do not signal consumers directly — the watermark poll picks them up. Producer→consumer push has been removed.

### Concurrency
- `ConsumerContext.consecutiveFailures` is a `volatile int` mutated by `++`; treat as racy until migrated to `AtomicInteger` (see `RemoteConsumer` for the precedent).
- No `UncaughtExceptionHandler` is installed on broker thread factories. Exceptions thrown by `execute()` (no `Future.get()` consumer) are silently swallowed. When adding async work, wrap the body in `try/catch (Throwable)` and log.
- Avoid blocking a scheduler thread on `Future.get(longTimeout)` against another pool. There is a known long-blocking pattern in `BatchDeliveryService` (`storageExecutor.submit(...).get(10, TimeUnit.MINUTES)`) — do not copy it for new code.
- Full audit (findings F1–F10, per-component risks, coverage gaps, drafted JUnit/Mockito/Awaitility tests, prioritised remediation): [`readme/CONCURRENCY_ANALYSIS.md`](readme/CONCURRENCY_ANALYSIS.md). Consult before changing executor lifecycle, refresh state, or batch delivery flow.

## Related Repositories

- **`../consumer-app`** — remote TCP consumer apps. Has its own CLAUDE.md and build.
- **`../monitoring`** — Prometheus scrape config + Grafana dashboards.
- **`../cloud-server`** — Micronaut mock registry used in dev/test.
- **`../docker-compose.yml`** — orchestrates broker + consumers + monitoring for full-system local runs.

Refer to those repos' own docs when working there; do not duplicate their contents here.
