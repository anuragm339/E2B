# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

This is a **Kafka-like distributed messaging system** comprising three main components:

1. **Provider (Broker)** - The messaging broker with modular architecture (this directory)
2. **Consumer-app** - Standalone consumer applications (../consumer-app)
3. **Monitoring** - Prometheus + Grafana observability stack (../monitoring)

### Complete System Features:
- Segment-based storage with memory-mapped file I/O (similar to Kafka)
- Netty-based non-blocking network layer
- Hierarchical broker topology with parent-child relationships
- 13 remote TCP consumer services handling 24 topics
- Push-based message delivery (event-driven, <5ms latency)
- Data refresh workflow (RESET/READY) for consumer synchronization
- Prometheus metrics with Grafana dashboards
- Docker Compose orchestration for full-stack deployment

## Repository Structure

This CLAUDE.md file is located in the **provider/** directory. The complete system spans three directories:

```
messaging/                          (parent directory)
├── provider/                       ← You are here (broker code)
│   ├── common/
│   ├── storage/
│   ├── network/
│   ├── pipe/
│   ├── broker/
│   ├── build.gradle
│   ├── Dockerfile
│   └── CLAUDE.md                   ← This file
│
├── consumer-app/                   ← Consumer applications
│   ├── src/
│   ├── libs/                       (JARs from provider)
│   ├── build.gradle
│   └── Dockerfile
│
├── monitoring/                     ← Observability stack
│   ├── prometheus/
│   │   └── prometheus.yml
│   └── grafana/
│       ├── provisioning/
│       └── dashboard-files/
│
├── cloud-server/                   ← Mock registry (Micronaut)
│   └── Dockerfile
│
├── docker-compose.yml              ← Orchestrates all 16 containers
├── data/                           ← Shared data directory
└── scripts/                        ← Deployment scripts (in provider/)
```

**Navigation tips:**
- To work on consumer-app: `cd ../consumer-app`
- To access monitoring configs: `cd ../monitoring`
- To run docker-compose: `cd ..` (parent directory)
- To return to provider: `cd provider`

## Build Commands

### Building the Provider (Broker)
```bash
# Build all modules
./gradlew build

# Build specific module
./gradlew :broker:build
./gradlew :storage:build

# Skip tests
./gradlew build -x test

# Clean build
./gradlew clean build
```

### Running Tests
```bash
# Run all tests
./gradlew test

# Run tests for specific module
./gradlew :broker:test
./gradlew :network:test
```

### Running the Broker

**Standalone (local development):**
```bash
NODE_ID=local-001 REGISTRY_URL=http://localhost:8080 \
BROKER_PORT=9092 DATA_DIR=./data-local HTTP_PORT=8081 \
./gradlew :broker:run
```

**With production settings:**
```bash
NODE_ID=broker-001 REGISTRY_URL=http://registry.example.com \
BROKER_PORT=9092 DATA_DIR=/var/lib/broker HTTP_PORT=8081 \
JAVA_OPTS="-Xms512m -Xmx2g" \
./gradlew :broker:run
```

### Docker Build & Run
```bash
# Build Docker image
docker build -t messaging-broker:latest -f Dockerfile .

# Run container
docker run -d --name broker \
  -e NODE_ID=broker-001 \
  -e BROKER_PORT=9092 \
  -e HTTP_PORT=8081 \
  -e DATA_DIR=/app/data \
  -p 9092:9092 -p 8081:8081 \
  -v broker-data:/app/data \
  messaging-broker:latest

# View logs
docker logs -f broker
```

### Complete System Setup
```bash
# Using provided setup scripts (from scripts/ directory)
cd scripts
./setup.sh                    # Quick automated setup
./setup-interactive.sh        # Interactive with path configuration

# Manual Docker Compose
docker compose up -d
docker compose logs -f broker
docker compose ps
```

## Architecture

### Module Structure
The project is organized as a multi-module Gradle build:

```
common/        - Core interfaces & models (no dependencies)
├── api/       - StorageEngine, NetworkServer, NetworkClient, PipeConnector
├── model/     - MessageRecord, BrokerMessage, EventType, ConsumerRecord
└── annotation/ - @Consumer, @MessageHandler, @ErrorHandler

storage/       - Storage implementations (depends: common)
├── mmap/      - MMapStorageEngine (memory-mapped file I/O)
├── segment/   - Segment, SegmentManager (Kafka-style segments)
└── metadata/  - SegmentMetadata, SQLite-based tracking

network/       - Network layer (depends: common)
├── tcp/       - NettyTcpServer, NettyTcpClient (Netty-based)
├── codec/     - BinaryMessageEncoder/Decoder
└── handler/   - ServerMessageHandler, ClientMessageHandler

pipe/          - Parent broker connection (depends: common)
├── HttpPipeConnector - HTTP polling from parent
└── PipeServer        - Serves messages to child brokers

broker/        - Main broker service (depends: all modules)
├── core/      - BrokerService (main orchestrator)
├── consumer/  - RemoteConsumerRegistry, ConsumerDeliveryManager
├── registry/  - TopologyManager, CloudRegistryClient
├── refresh/   - DataRefreshManager (RESET/READY workflow)
├── metrics/   - BrokerMetrics (Prometheus integration)
└── http/      - DataRefreshController, REST endpoints
```

### Message Flow Architecture

**Parent → Broker:**
```
CloudRegistry (assigns parent) → TopologyManager
Parent Broker → PipeConnector (HTTP poll) → BrokerService.handlePipeMessage()
→ StorageEngine.append() → RemoteConsumerRegistry.notifyNewMessage() (push)
```

**Producer → Broker → Consumer:**
```
Producer/Client → NettyTcpServer → BrokerService.handleMessage()
→ StorageEngine.append() → RemoteConsumerRegistry.notifyNewMessage()
→ ConsumerDeliveryManager → TCP send to consumer
```

**Push-based delivery:** Messages are immediately pushed to consumers when they arrive, eliminating polling delays. Typical end-to-end latency is <5ms.

### Storage Layer Details

**Kafka-style segment approach:**
- **Active segments** use sequential scan (no index) for reads
- **Sealed segments** use sparse index (every 4KB) for O(log n) lookups
- Segments roll over at configurable size (default: 1GB)
- CRC32 validation on read/write
- SQLite metadata store tracks segment boundaries
- Recovery from disk on restart

**Key classes:**
- `StorageEngine` (interface) - Storage abstraction
- `MMapStorageEngine` - Memory-mapped implementation
- `Segment` - Individual segment file
- `SegmentManager` - Manages active/sealed segments per topic-partition

### Network Protocol

**Binary message format:**
```
[Type:1B][MessageId:8B][PayloadLength:4B][Payload:variable]
```

**Message types:**
- `DATA` (0x01) - Message delivery
- `ACK` (0x02) - Acknowledgment
- `SUBSCRIBE` (0x03) - Consumer subscription
- `COMMIT_OFFSET` (0x04) - Offset commit
- `RESET` (0x05) - Data refresh trigger
- `READY` (0x06) - Data refresh complete
- `BATCH_ACK` (0x07) - Batch acknowledgment
- `HEARTBEAT` (0x08) - Keep-alive

### Data Refresh Workflow

The broker implements a RESET/READY workflow for consumer data synchronization:

1. Broker sends `RESET` to consumers
2. Consumers clear local data and send `RESET_ACK`
3. Broker waits for all expected consumers (configured in `application.yml`)
4. Broker triggers replay from earliest offset
5. Consumers rebuild data and send `READY`
6. Normal message delivery resumes

**Implementation:** `DataRefreshManager`, `DataRefreshController`

**Configuration:** See `data-refresh.expected-consumers` in `broker/src/main/resources/application.yml`

## Configuration

Main configuration file: `broker/src/main/resources/application.yml`

**Key settings:**
- `broker.nodeId` - Unique broker identifier
- `broker.network.port` - TCP port for consumers (default: 9092)
- `micronaut.server.port` - HTTP admin port (default: 8082)
- `broker.storage.dataDir` - Data directory path
- `broker.storage.segment-size` - Segment rollover size (bytes)
- `broker.registry.url` - Cloud registry URL for topology
- `data-refresh.expected-consumers` - List of topics for data refresh

**Environment variable overrides:**
- `NODE_ID` → `broker.nodeId`
- `BROKER_PORT` → `broker.network.port`
- `HTTP_PORT` → `micronaut.server.port`
- `DATA_DIR` → `broker.storage.dataDir`
- `REGISTRY_URL` → `broker.registry.url`

## Development Guidelines

### When Modifying Storage Layer
- Storage operations are low-level and performance-critical
- Active segments use sequential scan (no index needed)
- Sealed segments must maintain sparse index consistency
- Always validate CRC32 on reads
- Test segment boundary conditions (end of segment, cross-segment reads)
- Storage recovery on startup must handle corrupted segments

### When Modifying Network Layer
- All I/O is async (CompletableFuture-based)
- Netty handlers must be stateless or properly managed
- Binary protocol changes require version compatibility
- TCP connections are long-lived; handle disconnect gracefully
- Message encoding/decoding happens in codec layer

### When Modifying Broker Core
- `BrokerService` is the main orchestrator; keep it focused
- Consumer registration uses stable `group:topic` identifiers (not socket addresses)
- Offset commits persist to property files (single source of truth)
- Push model: Always call `notifyNewMessage()` after storage.append()
- Data refresh requires careful state management

### Testing Strategy
- Use `cloud-test-server.py` for integration testing
- Start broker with local settings (see "Running the Broker")
- Consumer-app is in separate repository; coordinate testing
- Monitor metrics at `http://localhost:8081/metrics`
- Check health at `http://localhost:8081/health`

### Monitoring & Observability

**Prometheus metrics available:**
- `broker_messages_received_total` - Total messages received
- `broker_messages_stored_total` - Total messages stored
- `broker_consumer_offset{consumer_id, topic, group}` - Consumer offsets
- `broker_consumer_lag{consumer_id, topic, group}` - Consumer lag
- `broker_consumer_messages_sent_total` - Messages delivered
- `broker_consumer_delivery_latency_seconds` - Delivery latency (p50/p95/p99)
- `broker_storage_write_seconds` - Storage write duration
- `broker_e2e_latency_seconds` - End-to-end message latency

**Grafana dashboards:**
- Location: `scripts/monitoring/grafana/dashboards/`
- Multi-consumer dashboard with filtering
- Per-consumer metrics tracking

## Important Implementation Notes

### CA Certificate Handling
The Dockerfile imports `mvnrepository.com.pem` into the JDK trust store for Maven Central access during Gradle builds. This is required for secure dependency downloads.

### Memory Management
- Default JVM settings: `-Xms512m -Xmx2g -XX:+UseG1GC`
- Docker memory limits configured in `docker-compose.yml`
- Broker: 300MB, Consumers: 200MB each

### Consumer Offset Tracking
- Offsets are persisted to property files (NOT in-memory)
- File location: `<DATA_DIR>/consumer-offsets.properties`
- Format: `<group>:<topic>=<offset>`
- Always use `ConsumerOffsetTracker.updateOffset()` to persist

### Recent Bug Fixes (Dec 2024)
1. Fixed infinite loop in active segment reads
2. Fixed segment boundary traversal
3. Implemented Kafka-style active segment scanning
4. Switched to push-based message delivery (eliminated polling delay)

## Consumer Application (../consumer-app)

The consumer-app is a separate Gradle project that implements remote TCP consumers.

### Consumer App Architecture
```
consumer-app/
├── src/main/java/com/example/consumer/
│   ├── ConsumerApplication.java        - Main application
│   ├── GenericConsumerHandler.java     - Message processing
│   ├── service/
│   │   ├── ConsumerConnectionService   - TCP connection to broker
│   │   └── ControlMessageHandler       - RESET/READY workflow
│   └── api/
│       └── QueryController             - REST API endpoints
├── libs/                               - Provider JARs (common, network)
├── build.gradle                        - Uses Shadow plugin for fat JAR
└── Dockerfile                          - Multi-stage build
```

### Building Consumer App
```bash
# From consumer-app directory
cd ../consumer-app

# Build fat JAR
./gradlew shadowJar

# Build Docker image
docker build -t consumer-app:latest .

# Or use provided script (copies JARs from provider first)
./scripts/build-and-push.sh
```

### Running Individual Consumers

**Standalone (local development):**
```bash
cd ../consumer-app

CONSUMER_TYPE=price-quote \
CONSUMER_TOPICS="prices-v1,reference-data-v5" \
CONSUMER_GROUP=price-quote-group \
CONSUMER_PORT=8090 \
BROKER_HOST=localhost \
BROKER_PORT=9092 \
./gradlew run
```

**Docker:**
```bash
docker run -d --name my-consumer \
  -e CONSUMER_TYPE=price-quote \
  -e CONSUMER_TOPICS="prices-v1,reference-data-v5" \
  -e CONSUMER_GROUP=price-quote-group \
  -e CONSUMER_PORT=8090 \
  -e BROKER_HOST=broker \
  -e BROKER_PORT=9092 \
  -p 8090:8090 \
  -v consumer-data:/app/consumer-data \
  consumer-app:latest
```

### Consumer Environment Variables
- `CONSUMER_TYPE` - Consumer service identifier (e.g., price-quote, product-svc-lite)
- `CONSUMER_TOPICS` - Comma-separated list of topics to subscribe to
- `CONSUMER_GROUP` - Consumer group name (used for offset tracking)
- `CONSUMER_PORT` - HTTP port for REST API and health checks
- `BROKER_HOST` - Broker hostname (use `broker` in Docker, `localhost` for local)
- `BROKER_PORT` - Broker TCP port (default: 9092)
- `STORAGE_DATA_DIR` - Directory for consumer data (default: /app/consumer-data)

### Consumer REST API
Each consumer exposes a REST API on its configured port:

**Health check:**
```bash
curl http://localhost:8090/health
```

**Query by offset (if consumer stores data locally):**
```bash
curl http://localhost:8090/api/query/offset/12345
```

### The 13 Consumer Services

The system deploys 13 consumer services handling 24 topics:

1. **consumer-price-quote** (port 8090) - 6 topics:
   - prices-v1, reference-data-v5, non-promotable-products, prices-v4, minimum-price, deposit

2. **consumer-product-svc-lite** (port 8091) - 1 topic:
   - product-base-document

3. **consumer-search-enterprise** (port 8092) - 1 topic:
   - search-product

4. **consumer-tesco-location-service** (port 8093) - 2 topics:
   - location, location-clusters

5. **consumer-customer-order-on-till** (port 8094) - 1 topic:
   - selling-restrictions

6. **consumer-colleague-facts** (port 8095) - 2 topics:
   - colleague-facts-jobs, colleague-facts-legacy

7. **consumer-loss-prevention-api** (port 8096) - 4 topics:
   - loss-prevention-configuration, loss-prevention-store-configuration, loss-prevention-product, loss-prevention-rule-config

8. **consumer-stored-value-services** (port 8097) - 2 topics:
   - stored-value-services-banned-promotion, stored-value-services-active-promotion

9. **consumer-colleague-identity** (port 8098) - 1 topic:
   - colleague-card-pin

10. **consumer-distributed-identity** (port 8099) - 1 topic:
    - colleague-card-pin-v2

11. **consumer-dcxp-content** (port 8100) - 1 topic:
    - dcxp-content

12. **consumer-restriction-service-on-tills** (port 8101) - 1 topic:
    - restriction-rules

13. **consumer-dcxp-ugc** (port 8102) - 1 topic:
    - dcxp-ugc

## Monitoring Stack (../monitoring)

The monitoring directory contains Prometheus and Grafana configurations.

### Monitoring Directory Structure
```
monitoring/
├── prometheus/
│   └── prometheus.yml              - Scrape configs for broker + consumers
└── grafana/
    ├── provisioning/
    │   ├── dashboards/
    │   │   └── dashboard-provisioning.yml
    │   └── datasources/
    │       └── prometheus.yml      - Auto-configure Prometheus datasource
    └── dashboard-files/
        ├── messaging-broker-dashboard.json       - Multi-consumer overview
        ├── per-consumer-dashboard.json           - Individual consumer metrics
        ├── broker-system-metrics.json            - System metrics (CPU, memory)
        └── thread-monitoring-dashboard.json      - Thread monitoring
```

### Accessing Monitoring

**Grafana:**
- URL: http://localhost:3000
- Username: admin
- Password: admin

**Dashboards:**
- Multi-Consumer Overview: http://localhost:3000/d/messaging-broker-multi-consumer
- Per-Consumer Metrics: http://localhost:3000/d/per-consumer-metrics
- System Metrics: http://localhost:3000/d/broker-system-metrics
- Thread Monitoring: http://localhost:3000/d/thread-monitoring

**Prometheus:**
- URL: http://localhost:9090
- Metrics endpoint: http://localhost:8081/prometheus (broker)

### Prometheus Metrics Configuration

The Prometheus config scrapes:
- **Broker** at `broker:8081/prometheus`
- **Consumers** (if they expose metrics) at various ports
- **Prometheus** itself for self-monitoring

### Modifying Dashboards

**Option 1: Edit in Grafana UI**
1. Make changes in UI
2. Export dashboard JSON (Share → Export)
3. Save to `../monitoring/grafana/dashboard-files/`
4. Restart Grafana: `docker compose restart grafana`

**Option 2: Edit JSON directly**
```bash
cd ../monitoring/grafana/dashboard-files
vim messaging-broker-dashboard.json
# Restart Grafana
cd ../../..
docker compose restart grafana
```

## Docker Compose Full System

The parent directory (`../`) contains a `docker-compose.yml` that orchestrates the entire system.

### Complete System Architecture
```
┌─────────────────┐
│  Cloud Server   │ (Mock registry + data injection)
│    (Port 8080)  │ TEST mode: Random data | PRODUCTION: Your data
└────────┬────────┘
         │ HTTP Poll (/pipe/poll)
         ↓
┌─────────────────┐
│     Broker      │ ← Prometheus scrapes metrics (port 8081)
│  (Port 9092)    │   TCP server for consumers
│  (Port 8081)    │   HTTP admin/metrics
└────────┬────────┘
         │ TCP Binary Protocol
         ├──────────┬──────────┬─────────┐
         ↓          ↓          ↓         ↓
    [Consumer1] [Consumer2] ... [Consumer13]
    Port 8090   Port 8091       Port 8102
         │          │          │         │
         └──────────┴──────────┴─────────┘
                    │
              ┌─────┴─────┐
              ↓           ↓
         Prometheus   Grafana
         Port 9090    Port 3000
```

### Running the Complete System

**Start all services:**
```bash
# From parent directory (messaging/)
cd ..
docker compose up -d

# Check status
docker compose ps

# View logs
docker compose logs -f broker
docker compose logs -f consumer-price-quote

# View all logs
docker compose logs -f
```

**Stop all services:**
```bash
docker compose down

# Stop and remove volumes (clean slate)
docker compose down -v
```

**Restart specific service:**
```bash
docker compose restart broker
docker compose restart consumer-price-quote
docker compose restart grafana
```

### Docker Compose Services

The compose file defines:
- 1 cloud-server (mock registry)
- 1 broker
- 13 consumer services
- 1 Prometheus
- 1 Grafana

**Total: 16 containers**

### Memory Allocation
- Cloud Server: 300MB
- Broker: 500MB (with optimized JVM settings)
- Each Consumer: 200MB
- Prometheus: Default
- Grafana: Default

**Total system memory:** ~4GB

### Network Configuration
All services run on a custom bridge network: `messaging-network`

Service discovery uses container names:
- Consumers connect to `broker:9092`
- Broker connects to `cloud-server:8080`
- Prometheus scrapes `broker:8081`
- Grafana connects to `prometheus:9090`

### Volume Persistence

**Data volumes:**
- `broker-data` - Broker segments and metadata
- `consumer-*-data` - Consumer data (13 volumes)
- `prometheus-data` - Prometheus TSDB
- `grafana-data` - Grafana dashboards and settings

**Shared data:**
- `./data` - Mounted to both cloud-server and broker for SQLite database

## Complete System Commands

### Building Everything
```bash
# From messaging/ directory (parent)
cd provider
./gradlew build

cd ../consumer-app
./gradlew shadowJar

# Build Docker images
cd ..
docker compose build
```

### Development Workflow

**1. Start infrastructure (Broker + Cloud Server + Monitoring):**
```bash
cd messaging/
docker compose up -d cloud-server broker prometheus grafana
```

**2. Run consumer locally for development:**
```bash
cd consumer-app
CONSUMER_TYPE=test \
CONSUMER_TOPICS="test-topic" \
CONSUMER_GROUP=test-group \
CONSUMER_PORT=8090 \
BROKER_HOST=localhost \
BROKER_PORT=9093 \
./gradlew run
```

**3. Monitor in Grafana:**
- Open http://localhost:3000
- Login with admin/admin
- View dashboards

**4. Trigger data refresh:**
```bash
curl -X POST http://localhost:8081/api/refresh/trigger
```

### Troubleshooting

**Broker won't start:**
```bash
# Check logs
docker logs messaging-broker

# Check if port is occupied
lsof -i:9092
lsof -i:8081

# Check health
curl http://localhost:8081/health
```

**Consumer not connecting:**
```bash
# Check consumer logs
docker logs consumer-price-quote

# Verify network
docker network inspect messaging_messaging-network

# Check broker is accepting connections
nc -zv localhost 9093  # 9093 is mapped to broker's 9092
```

**Grafana shows no data:**
```bash
# Check Prometheus is scraping
curl http://localhost:9090/api/v1/targets

# Check broker metrics endpoint
curl http://localhost:8081/prometheus

# Restart Grafana
docker compose restart grafana
```

**Out of memory:**
```bash
# Check container memory usage
docker stats

# Check JVM heap settings in docker-compose.yml
# Adjust JAVA_OPTS for broker or consumers
```

## Related Projects & Tools

- **consumer-app** - Remote consumer implementation (../consumer-app)
- **cloud-server** - Mock registry and data injection (../cloud-server, Micronaut-based)
- **monitoring** - Prometheus + Grafana configs (../monitoring)
- **cloud-test-server.py** - Legacy Python version of cloud server

## Performance Characteristics

- **Write throughput:** ~50,000 msgs/sec (mmap)
- **Read throughput:** ~20,000 msgs/sec (active), ~100,000 msgs/sec (sealed)
- **Network latency:** <5ms (push model, local network)
- **Storage lookup:** <1ms (indexed)
- **E2E latency:** <10ms (cloud-server → broker → consumer)
- **Consumer delivery:** Event-driven push, ~1-2ms from storage to TCP send
