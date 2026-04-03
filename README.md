# Messaging Provider

A modular messaging broker with pluggable storage, transport, topology, and refresh workflows.

This repository is the broker/provider side of the system. The current design separates broker orchestration from storage and network implementations so new backends can be added without changing broker code.

## Current Design

- `broker` owns orchestration, consumer delivery, refresh, topology, and metrics.
- `storage` owns persistence and batch creation.
- `network` owns wire protocol, TCP transport, client/server codecs, and batch transfer.
- `pipe` owns parent-broker polling and upstream replication.
- `common` owns the shared contracts and models used across all modules.

The main pluggable boundaries are:

- `StorageEngine`: generic append/read/offset storage contract
- `BatchReadableStorage`: optional capability for storage engines that can produce a `DeliveryBatch`
- `NetworkServer`: transport contract used by the broker to send messages and batches
- `DeliveryBatch`: storage-produced delivery unit consumed by the transport

That means:

- a new storage backend such as SQLite/Postgres can implement `StorageEngine`, and if it supports remote batch delivery, `BatchReadableStorage`
- a new transport such as gRPC/RSocket can implement `NetworkServer`
- the broker module does not need transport-specific or storage-specific branching

## Latest Design Changes

- Removed transport leakage from broker/common APIs. Batch delivery no longer exposes Netty `FileRegion`.
- Removed concrete storage branching from broker delivery logic. The broker now uses `BatchReadableStorage` instead of `instanceof` dispatch.
- Introduced `DeliveryBatch` as the storage-to-transport handoff object.
- Kept `StorageEngine` generic so non-file-backed storage remains possible.
- Moved the batch-header responsibility into the transport via `NetworkServer.sendBatch(...)`.
- Updated the architecture docs in [`readme/C4_ARCHITECTURE.md`](readme/C4_ARCHITECTURE.md) to reflect both structural C4 views and dynamic runtime flows.

## Module Layout

```text
provider/
├── common/    Shared APIs, models, exceptions, annotations
├── storage/   File-backed storage engines, segments, metadata, batch production
├── network/   Netty TCP server/client, codecs, protocol handlers
├── pipe/      Parent-broker polling, pipe server, replication transport
├── broker/    Broker orchestration, delivery, refresh, topology, metrics
├── readme/    Architecture and design docs
└── scripts/   Monitoring and support tooling
```

## Runtime Architecture

### Producer ingress

1. Producer sends broker protocol messages to the broker TCP port.
2. `NettyTcpServer` decodes the message and forwards it to broker handlers.
3. `BrokerService` routes data to the configured `StorageEngine`.
4. Consumer delivery is scheduled from broker-side delivery services.

### Parent-broker replication

1. `TopologyManager` queries the cloud registry for the current topology.
2. If the node has a parent, `HttpPipeConnector` polls the parent broker's `/pipe/poll` endpoint.
3. Pipe messages are converted into local `MessageRecord`s and stored through broker services.

### Remote consumer delivery

1. `BatchDeliveryService` asks `BatchReadableStorage` for a `DeliveryBatch`.
2. `NetworkServer.sendBatch(clientId, group, batch)` hands the batch to the transport.
3. The transport writes the batch header and streams payload bytes.
4. The client decodes the batch and sends `BATCH_ACK`.
5. The broker advances offsets only after the ACK path completes.

### Refresh workflow

The broker supports coordinated refresh via `RESET`, replay, and `READY`:

1. broker starts refresh for a topic
2. subscribed consumers receive `RESET`
3. broker waits for `RESET_ACK`
4. broker replays historical data
5. broker sends `READY`
6. broker waits for `READY_ACK`
7. normal forward delivery resumes

For the exact class and method flow, including modern and legacy client behavior, see [`readme/C4_ARCHITECTURE.md`](readme/C4_ARCHITECTURE.md).

## Default Runtime Configuration

Current defaults are defined in `broker/src/main/resources/application.yml`.

```yaml
micronaut:
  server:
    port: ${HTTP_PORT:8082}

broker:
  nodeId: ${NODE_ID:local-001}

  storage:
    type: filechannel
    dataDir: ${DATA_DIR:./data}
    segment-size: 1073741824

  network:
    type: tcp
    port: ${BROKER_PORT:19092}

  consumer:
    ack-timeout: ${ACK_TIMEOUT_MS:120000}
    max-message-size-per-consumer: ${MAX_MESSAGE_SIZE_PER_CONSUMER:2097152}
    adaptive-polling:
      min-delay-ms: 200
      max-delay-ms: 5000

  pipe:
    min-poll-interval-ms: 500
    max-poll-interval-ms: 20000
    poll-limit: 5
```

Notes:

- default storage is `filechannel`
- default broker TCP port is `19092`
- default HTTP/admin port is `8082`
- the current config is tuned for lower CPU usage rather than maximum throughput

## Supported Extension Points

### Add a new storage implementation

Implement:

- `StorageEngine`
- optionally `BatchReadableStorage` if the backend supports remote consumer batch delivery

Examples:

- file-backed zero-copy storage can return a file-backed `DeliveryBatch`
- DB-backed storage can return `ByteArrayDeliveryBatch`

### Add a new transport implementation

Implement:

- `NetworkServer`

The transport is responsible for:

- accepting broker messages
- sending point-to-point broker messages
- sending `DeliveryBatch` payloads
- encoding the batch header
- closing the batch on success, failure, cancellation, or rejection

## Running The Broker

### Local run

```bash
./gradlew :broker:run
```

### With environment variables

```bash
export NODE_ID=broker-001
export REGISTRY_URL=http://localhost:8080
export DATA_DIR=./data-local
export BROKER_PORT=19092
export HTTP_PORT=8082

./gradlew :broker:run
```

### Docker

```bash
docker build -t messaging-broker:latest -f broker/Dockerfile .

docker run -d \
  --name messaging-broker \
  -e NODE_ID=broker-001 \
  -e REGISTRY_URL=http://host.docker.internal:8080 \
  -e BROKER_PORT=19092 \
  -e HTTP_PORT=8082 \
  -e DATA_DIR=/app/data \
  -p 19092:19092 \
  -p 8082:8082 \
  -v broker-data:/app/data \
  messaging-broker:latest
```

## Monitoring

Grafana dashboards and support-facing monitoring assets live outside this module under the sibling `monitoring/` directory.

Useful starting points:

- broker and per-consumer dashboards
- thread monitoring and bottleneck dashboards
- pipe performance dashboards
- refresh and ACK reconciliation dashboards

The architecture/design document for flows and drilldowns is here:

- [`readme/C4_ARCHITECTURE.md`](readme/C4_ARCHITECTURE.md)

## Build And Test

```bash
./gradlew build
./gradlew test
./gradlew :broker:build
./gradlew :network:build
./gradlew :storage:build
```

## Technology Stack

- Java 17
- Micronaut 4
- Netty TCP transport
- FileChannel-based segment storage
- SQLite metadata store
- Prometheus metrics
- Grafana dashboards
- Gradle multi-module build

## Notes

- The broker supports both modern clients and legacy clients.
- `BATCH_ACK` is automatic on the modern client path after batch decode.
- `RESET_ACK` and `READY_ACK` are part of the refresh workflow and are handled separately from normal batch ACKs.
- The current detailed flow documentation is intentionally kept in `readme/C4_ARCHITECTURE.md` instead of duplicating it here.
