# Messaging System — Architecture Reference

> Living document. Update when components are added or responsibilities change.

---

## Table of Contents

1. [System Context](#1-system-context)
2. [Container Diagram](#2-container-diagram)
3. [Broker Component Diagram](#3-broker-component-diagram)
4. [Storage Layer](#4-storage-layer)
5. [Network Protocol](#5-network-protocol)
6. [Message Flow](#6-message-flow)
7. [Data Refresh Workflow](#7-data-refresh-workflow)
8. [Threading Model](#8-threading-model)
9. [Deployment Architecture](#9-deployment-architecture)
10. [Observability](#10-observability)
11. [Missing / Recommended Additions](#11-missing--recommended-additions)

---

## 1. System Context

```
┌─────────────────────────────────────────────────────────────────────┐
│                          External World                             │
│                                                                     │
│   ┌──────────────┐        ┌──────────────┐       ┌─────────────┐   │
│   │   Producers  │        │ Cloud Server │       │  Consumers  │   │
│   │              │        │  (Registry + │       │  (13 svcs)  │   │
│   │ Inject data  │        │   test data) │       │             │   │
│   │ via TCP or   │        │              │       │ Legacy TCP  │   │
│   │ HTTP pipe    │        │ Mock: random │       │ or Modern   │   │
│   └──────┬───────┘        │ Real: your   │       │ TCP clients │   │
│          │                │ data source  │       └──────┬──────┘   │
│          │ TCP :9092      └──────┬───────┘              │ TCP:9092 │
│          │                      │ HTTP /pipe/poll        │          │
│          │                      │ HTTP /registry/topo    │          │
│          ▼                      ▼                        ▼          │
│   ┌──────────────────────────────────────────────────────────────┐  │
│   │                     MESSAGING BROKER                         │  │
│   │                      (this system)                           │  │
│   └──────────────────────────────────────────────────────────────┘  │
│          │                      │                                   │
│          │ HTTP :8081            │ Prometheus scrape                 │
│          ▼                      ▼                                   │
│   ┌─────────────┐        ┌─────────────────────────┐               │
│   │   Ops Team  │        │  Prometheus + Grafana   │               │
│   │  /admin API │        │  Dashboards + Alerts    │               │
│   └─────────────┘        └─────────────────────────┘               │
└─────────────────────────────────────────────────────────────────────┘
```

### Actors

| Actor | Role |
|-------|------|
| **Producers** | Send messages via TCP (modern protocol) or via HTTP pipe from parent broker |
| **Cloud Server** | Registry: assigns parent broker URL to each child. Also injects test data |
| **Consumers (13 svcs)** | Subscribe via TCP. Legacy clients multi-topic; modern clients single-topic |
| **Prometheus / Grafana** | Scrape `/prometheus` endpoint every 60s; alert on SLOs |
| **Ops Team** | Trigger data refresh, inspect thread pools, view health via `/admin` HTTP API |

---

## 2. Container Diagram

```
╔══════════════════════════════════════════════════════════════════════════════╗
║  messaging-network (Docker bridge)                                           ║
║                                                                              ║
║  ┌─────────────────────┐      HTTP poll       ┌──────────────────────────┐  ║
║  │    cloud-server     │◄─────────────────────│                          │  ║
║  │    :8080            │  /pipe/poll           │      BROKER              │  ║
║  │                     │  /registry/topology   │      :9092 (TCP)         │  ║
║  │  Micronaut          │─────────────────────►│      :8081 (HTTP/admin)  │  ║
║  │  SQLite: events.db  │  topology response    │                          │  ║
║  │  Shared: ./data     │                       │  Java 17 / Micronaut     │  ║
║  └─────────────────────┘                       │  600 MB heap             │  ║
║                                                │  Shared vol: broker-data │  ║
║                                                └──────────┬───────────────┘  ║
║                                                           │ TCP :9092        ║
║                    ┌──────────────────────────────────────┼──────────┐       ║
║                    │                                      │          │       ║
║           ┌────────▼───────┐              ┌──────────────▼──┐ ┌─────▼────┐  ║
║           │consumer-price  │  …×13 svcs…  │consumer-product │ │  ...     │  ║
║           │-quote  :8090   │              │-svc-lite :8091  │ │          │  ║
║           │                │              │                 │ │          │  ║
║           │ Legacy TCP     │              │ Legacy TCP      │ │          │  ║
║           │ 6 topics       │              │ 1 topic         │ │          │  ║
║           │ 200 MB heap    │              │ 200 MB heap     │ │          │  ║
║           └────────────────┘              └─────────────────┘ └──────────┘  ║
║                                                                              ║
║  ┌──────────────────────┐       ┌──────────────────────────────────────┐    ║
║  │  prometheus  :9090   │◄──────│  broker :8081/prometheus             │    ║
║  │                      │ scrape│                                      │    ║
║  │  15s interval        │       └──────────────────────────────────────┘    ║
║  └──────────┬───────────┘                                                   ║
║             │ datasource                                                     ║
║  ┌──────────▼───────────┐                                                   ║
║  │  grafana   :3000     │                                                   ║
║  │  4 dashboards        │                                                   ║
║  └──────────────────────┘                                                   ║
╚══════════════════════════════════════════════════════════════════════════════╝
```

### Volumes & Persistence

| Volume | Mounted In | Contains |
|--------|-----------|---------|
| `./data` (bind mount) | cloud-server + broker | events.db, pipe-offset.properties |
| `broker-data` | broker | segment `.log`/`.index` files, consumer-offsets.properties, topology.properties, refresh-state/ |
| `consumer-*-data` (×13) | each consumer | consumer local data store |
| `prometheus-data` | prometheus | TSDB data |
| `grafana-data` | grafana | dashboards, settings |

---

## 3. Broker Component Diagram

```
┌────────────────────────────────────────────────────────────────────────────┐
│                              BROKER PROCESS                                │
│                                                                            │
│  ┌──────────────────────────────────────────────────────────────────────┐  │
│  │                         NETWORK LAYER                               │  │
│  │                                                                      │  │
│  │  ┌─────────────────────────────────────────────────────────────┐   │  │
│  │  │                   NettyTcpServer  :9092                     │   │  │
│  │  │   Boss Threads (×2) ──► Worker Threads (×16)               │   │  │
│  │  │                                                             │   │  │
│  │  │  ProtocolDetectionDecoder                                   │   │  │
│  │  │       ├── MODERN ──► BinaryMessageDecoder                  │   │  │
│  │  │       │                   ↓ BrokerMessage                  │   │  │
│  │  │       └── LEGACY ──► LegacyEventDecoder                   │   │  │
│  │  │                       LegacyConnectionState                │   │  │
│  │  │                           ↓ BrokerMessage                  │   │  │
│  │  │                                                             │   │  │
│  │  │              ServerMessageHandler                           │   │  │
│  │  │              MessageHandlerRegistry                         │   │  │
│  │  └──────────────────────┬──────────────────────────────────────┘   │  │
│  └─────────────────────────│──────────────────────────────────────────┘  │
│                            │ dispatch by MessageType                       │
│  ┌─────────────────────────▼──────────────────────────────────────────┐  │
│  │                       HANDLER LAYER                                │  │
│  │                                                                    │  │
│  │  ┌───────────┐ ┌───────────┐ ┌──────────────┐ ┌───────────────┐  │  │
│  │  │DataHandler│ │Subscribe  │ │BatchAckHandler│ │ResetAck/Ready │  │  │
│  │  │           │ │Handler    │ │               │ │AckHandler     │  │  │
│  │  │Validate   │ │Modern:    │ │Modern:offset  │ │               │  │  │
│  │  │→ Storage  │ │ JSON      │ │ commit        │ │→ Refresh      │  │  │
│  │  │→ Watermark│ │Legacy:    │ │Legacy: merge  │ │  Coordinator  │  │  │
│  │  │→ Notify   │ │ REGISTER  │ │ batch ACK     │ │               │  │  │
│  │  └─────┬─────┘ └─────┬─────┘ └──────┬────────┘ └───────┬───────┘  │  │
│  └────────│─────────────│──────────────│──────────────────│───────────┘  │
│           │             │              │                   │              │
│  ┌────────▼─────────────▼──────────────▼───────────────────▼───────────┐  │
│  │                         CORE SERVICES                               │  │
│  │                                                                     │  │
│  │  ┌──────────────────┐   ┌──────────────────┐  ┌──────────────────┐ │  │
│  │  │  BrokerService   │   │  TopologyManager │  │  RefreshCoord-   │ │  │
│  │  │                  │   │                  │  │  inator          │ │  │
│  │  │ Orchestrates all │   │ Polls Cloud Reg  │  │                  │ │  │
│  │  │ startup/shutdown │   │ every 30s        │  │ RESET_SENT       │ │  │
│  │  │                  │   │ → PipeConnector  │  │ → REPLAYING      │ │  │
│  │  └──────────────────┘   │   .connect()     │  │ → READY_SENT     │ │  │
│  │                         └──────────────────┘  │ → COMPLETED      │ │  │
│  │  ┌──────────────────┐                         │                  │ │  │
│  │  │ HttpPipeConnector│                         │ RefreshInitiator │ │  │
│  │  │                  │                         │ ResetPhase       │ │  │
│  │  │ Polls parent     │                         │ ReplayPhase      │ │  │
│  │  │ every 200ms-10s  │                         │ ReadyPhase       │ │  │
│  │  │ Streaming JSON   │                         │ RecoveryService  │ │  │
│  │  │ pause/resume for │                         │ RefreshStateStore│ │  │
│  │  │ data refresh     │                         └──────────────────┘ │  │
│  │  └──────────────────┘                                              │  │
│  └─────────────────────────────────────────────────────────────────────┘  │
│                                                                            │
│  ┌─────────────────────────────────────────────────────────────────────┐  │
│  │                      CONSUMER DELIVERY LAYER                        │  │
│  │                                                                     │  │
│  │  ┌───────────────────────┐     ┌─────────────────────────────────┐ │  │
│  │  │  ConsumerRegistry     │     │  AdaptiveBatchDeliveryManager   │ │  │
│  │  │                       │     │                                 │ │  │
│  │  │  registerConsumer()   │     │  TopicFairScheduler             │ │  │
│  │  │  unregisterConsumer() │     │  ├── WatermarkGatePolicy        │ │  │
│  │  │  deliverBatch()       │◄────│  ├── RefreshGatePolicy          │ │  │
│  │  │  handleBatchAck()     │     │  └── DeliveryRetryPolicy        │ │  │
│  │  │                       │     │      (exp: 1ms→1s, fixed: 30s)  │ │  │
│  │  │  MODERN:              │     └─────────────────────────────────┘ │  │
│  │  │   BatchDeliveryService│                                         │  │
│  │  │  LEGACY:              │     ┌─────────────────────────────────┐ │  │
│  │  │   LegacyDeliveryMgr   │     │  ConsumerOffsetTracker          │ │  │
│  │  │   k-way merge by      │     │  consumer-offsets.properties    │ │  │
│  │  │   global offset       │     │  flush every 5s                 │ │  │
│  │  └───────────────────────┘     └─────────────────────────────────┘ │  │
│  └─────────────────────────────────────────────────────────────────────┘  │
│                                                                            │
│  ┌─────────────────────────────────────────────────────────────────────┐  │
│  │                         STORAGE LAYER                               │  │
│  │                                                                     │  │
│  │  MMapStorageEngine (FileChannel implementation)                     │  │
│  │                                                                     │  │
│  │  ┌────────────────────────────────────────────────────────────┐    │  │
│  │  │  SegmentManager  (per topic-partition)                     │    │  │
│  │  │                                                            │    │  │
│  │  │  Sealed Segments         Active Segment                   │    │  │
│  │  │  ┌──────────┐ ┌────┐    ┌──────────────────────┐         │    │  │
│  │  │  │00000.log │ │... │    │ 01073741824.log       │         │    │  │
│  │  │  │00000.idx │ │... │    │ 01073741824.idx       │         │    │  │
│  │  │  │(sparse   │ │    │    │ (sequential scan)     │         │    │  │
│  │  │  │ index,   │ │    │    │                       │         │    │  │
│  │  │  │ O(log n))│ │    │    │  Roll at 1GB          │         │    │  │
│  │  │  └──────────┘ └────┘    └──────────────────────┘         │    │  │
│  │  │                                                            │    │  │
│  │  │  SegmentMetadataStore (SQLite)                             │    │  │
│  │  │  StorageWatermarkTracker (in-memory, for fast poll)        │    │  │
│  │  └────────────────────────────────────────────────────────────┘    │  │
│  └─────────────────────────────────────────────────────────────────────┘  │
│                                                                            │
│  ┌──────────────────────────────────────────────────────────────────────┐  │
│  │                     OBSERVABILITY LAYER                              │  │
│  │  BrokerMetrics (Micrometer)  DataRefreshMetrics  Structured Logging  │  │
│  │  /prometheus  /health  /admin/threads                                │  │
│  └──────────────────────────────────────────────────────────────────────┘  │
└────────────────────────────────────────────────────────────────────────────┘
```

---

## 4. Storage Layer

### Segment File Format

```
 SEGMENT (topic / partition-0 / 00000000000000000000.log)
 ┌──────────────────────────────────────────────────────┐
 │  HEADER (6 bytes)                                    │
 │    magic   : 4B = "MLOG"                             │
 │    version : 2B = 1                                  │
 ├──────────────────────────────────────────────────────┤
 │  RECORD 1                                            │
 │    recordSize : 4B                                   │
 │    payload    : JSON-encoded MessageRecord           │
 │    {offset, topic, partition, msgKey, eventType,     │
 │     data, createdAt, contentType}                    │
 ├──────────────────────────────────────────────────────┤
 │  RECORD 2 ...                                        │
 └──────────────────────────────────────────────────────┘

 INDEX (00000000000000000000.idx) — v2 format
 ┌──────────────────────────────────────────────────────┐
 │  HEADER (6 bytes)                                    │
 │    magic   : 4B = "MIDX"                             │
 │    version : 2B = 2                                  │
 ├──────────────────────────────────────────────────────┤
 │  ENTRY (16 bytes each, sparse: every 4KB of log)     │
 │    offset      : 8B  ← global message offset         │
 │    logPosition : 4B  ← byte offset in .log file      │
 │    recordSize  : 4B                                  │
 ├──────────────────────────────────────────────────────┤
 │  ENTRY ...                                           │
 └──────────────────────────────────────────────────────┘
```

### Read Strategy

```
Read(topic, partition, offset, count)
        │
        ▼
  Is offset in active segment?
        │
   YES  │  NO
        │   └──► Binary search sealed segment index
        │         └──► Seek to logPosition, read sequentially
        │
        └──► Sequential scan from segment start until offset found
```

### Data Directory Layout

```
${DATA_DIR}/
├── consumer-offsets.properties       ← all consumer offsets (flushed every 5s)
├── pipe-offset.properties            ← current offset from parent broker
├── topology.properties               ← discovered parent URL
├── refresh-state/                    ← persisted refresh context per topic
│   ├── minimum-price.json
│   ├── prices-v1.json
│   └── ...
│
├── prices-v1/
│   └── partition-0/
│       ├── 00000000000000000000.log
│       ├── 00000000000000000000.idx
│       ├── 00000000001073741824.log  ← segment rolled at 1GB
│       ├── 00000000001073741824.idx
│       └── ...
│
├── reference-data-v5/
│   └── partition-0/
│       └── ...
│
└── (24 topic directories total)
```

---

## 5. Network Protocol

### Modern Protocol — Binary Wire Format

```
┌──────────┬─────────────┬───────────────┬─────────────────────────┐
│ Type     │ MessageId   │ PayloadLength │ Payload                  │
│ 1 byte   │ 8 bytes     │ 4 bytes       │ variable                 │
└──────────┴─────────────┴───────────────┴─────────────────────────┘

Message Types:
  0x01  DATA          Broker→Consumer  Message delivery (JSON payload)
  0x02  ACK           Consumer→Broker  Single message ACK
  0x03  SUBSCRIBE     Consumer→Broker  {"topic":"prices-v1","group":"my-group"}
  0x04  COMMIT_OFFSET Consumer→Broker  Persist offset to disk
  0x05  RESET         Broker→Consumer  Data refresh: clear local state
  0x06  READY         Broker→Consumer  Data refresh: all data sent, resume
  0x07  DISCONNECT    Broker→Consumer  Graceful shutdown signal
  0x08  HEARTBEAT     Bidirectional    Keep-alive (60s interval)
  0x09  BATCH_HEADER  Broker→Consumer  Zero-copy batch: {count,bytes,lastOffset,topic}
  0x0A  BATCH_ACK     Consumer→Broker  Batch receipt acknowledgment
  0x0B  RESET_ACK     Consumer→Broker  ACK of RESET message (clears local data)
  0x0C  READY_ACK     Consumer→Broker  ACK of READY message (confirms ready)
```

### Legacy Protocol — Event Binary Format

```
  RegisterEvent  → auto-subscribe using serviceName as lookup key in config
  Event.MESSAGE  → single message delivery (key + data)
  Event.BATCH    → batch of messages with FIFO ACK tracking
  Event.RESET    → data refresh: clear local state
  Event.READY    → data refresh: all data delivered
  Event.ACK      → acknowledges one RESET, BATCH, or READY (FIFO queue in broker)
  Event.EOF      → end of stream marker
  Event.DELETE   → tombstone message (no data)

  Protocol detection: first byte value 0-7 → legacy; >= 8 → modern
```

### Protocol Auto-Detection

```
New TCP connection
      │
      ▼
ProtocolDetectionDecoder (Netty ChannelHandler)
      │  reads first byte
      ├── 0x00–0x07 → Legacy Event ordinal
      │       └──► Remove self, add LegacyEventDecoder + LegacyConnectionState
      │
      └── 0x08–0xFF → Modern binary header
              └──► Remove self, add BinaryMessageDecoder
```

---

## 6. Message Flow

### 6A — Normal Delivery (Modern Consumer)

```
Producer                  Broker                        Consumer
    │                        │                               │
    │── TCP: SUBSCRIBE ──────►│                               │
    │                        │  registerConsumer(clientId,    │
    │                        │   topic, group, isLegacy=false)│
    │                        │  → AdaptiveBatchDelivery       │
    │                        │    Manager.registerConsumer()  │
    │                        │◄────────── TCP: SUBSCRIBE ACK ─┤
    │                        │                               │
    │── TCP: DATA(msg) ──────►│                               │
    │                        │  DataHandler:                 │
    │                        │   1. parse + validate         │
    │                        │   2. storage.append()         │
    │                        │   3. watermark.update()       │
    │                        │                               │
    │                        │  WatermarkGatePolicy fires:   │
    │                        │   scheduleDelivery(consumer)  │
    │                        │                               │
    │                        │  BatchDeliveryService:        │
    │                        │   storage.read(topic, offset) │
    │                        │─────── TCP: BATCH_HEADER ─────►│
    │                        │─────── TCP: DATA(payload) ────►│
    │                        │                               │
    │                        │◄──────── TCP: BATCH_ACK ───────│
    │                        │  offsetTracker.update()       │
    │                        │  (persisted every 5s)         │
```

### 6B — Normal Delivery (Legacy Consumer — k-way merge)

```
LegacyClient               Broker
    │                        │
    │── TCP: REGISTER ───────►│  SubscribeHandler:
    │   {serviceName:         │   look up topics from LegacyClientConfig
    │    "price-quote-svc"}   │   register for: prices-v1, ref-data-v5,
    │                        │    non-promotable, prices-v4, min-price, deposit
    │                        │   (one RemoteConsumer per topic, same clientId)
    │                        │
    │                        │  AdaptiveBatchDeliveryManager:
    │                        │   ONE scheduler task per clientId
    │                        │   (not per topic — deduplication)
    │                        │
    │                        │  LegacyConsumerDeliveryManager:
    │                        │   buildMergedBatch([6 topics])
    │                        │   ┌─────────────────────────────┐
    │                        │   │  k-way merge (priority heap) │
    │                        │   │  topic cursors sorted by     │
    │                        │   │  global offset               │
    │                        │   │  → one sorted message stream │
    │                        │   └─────────────────────────────┘
    │◄── TCP: EVENT.BATCH ───│  (all 6 topics merged, sorted)
    │◄── TCP: EVENT.MESSAGE ─│
    │                        │
    │── TCP: EVENT.ACK ──────►│  FIFO queue resolves generic ACK
    │                        │  to typed ACK (RESET_ACK / BATCH_ACK / READY_ACK)
```

### 6C — Pipe (Parent → Child Broker)

```
Parent Broker               Child Broker
      │                          │
      │                          │  TopologyManager:
      │                          │   GET /registry/topology
      │                          │  ← {parentUrl: "http://parent:8081"}
      │                          │
      │◄── HTTP GET /pipe/poll ──│  HttpPipeConnector (200ms–10s polling)
      │    ?offset=1234          │
      │                          │
      │── HTTP 200 (streaming) ─►│  Streaming JSON parser:
      │   [{topic,key,data,...}, │   for each record:
      │    {topic,key,data,...}] │    dataHandler(record)
      │                          │     → storage.append()
      │                          │     → watermark.update()
      │                          │     → notify consumers
      │                          │
      │                          │  pipe-offset.properties updated
      │                          │  immediately after success
```

---

## 7. Data Refresh Workflow

### 7A — State Machine

```
                    ┌─────────┐
     POST           │  IDLE   │
  /admin/refresh ──►│         │
                    └────┬────┘
                         │ startRefresh()
                         │ - create RefreshContext
                         │ - pausePipeCalls()
                         │ - send RESET to all consumers
                         ▼
                    ┌──────────────┐     abort timeout
         ┌──────────│  RESET_SENT  │────────────────────────┐
         │  retry   │              │  (10 min watchdog)      │
         │  every   │  waiting for │                         │
         │  5s      │  RESET_ACK   │                         │
         └──────────┤  from each   │                         │
                    │  consumer    │                         │
                    └──────┬───────┘                         │
                           │ all RESET_ACKs received         │
                           │ - resetConsumerOffset→0         │
                           │ - schedule replayCheck (1s)     │
                           ▼                                 │
                    ┌──────────────┐                         │
                    │  REPLAYING   │                         │
                    │              │  check every 1s:        │
                    │  deliver all │  allConsumersCaughtUp?  │
                    │  messages    │  = offsetTracker.get()  │
                    │  from offset │    >= storageHead        │
                    │  0           │                         │
                    └──────┬───────┘                         │
                           │ all consumers at storage head   │
                           │ - send READY to each consumer   │
                           │ - schedule readyTimeout (10s)   │
                           ▼                                 │
                    ┌──────────────┐                         │
                    │  READY_SENT  │                         │
                    │              │                         │
                    │  waiting for │                         │
                    │  READY_ACK   │                         │
                    │  from each   │                         │
                    │  consumer    │                         │
                    └──────┬───────┘                         │
                           │ all READY_ACKs received         │
                           │ - resumePipeCalls()             │
                           │ - cleanup after 60s             │
                           ▼                                 ▼
                    ┌──────────────┐               ┌─────────────────┐
                    │  COMPLETED   │               │    ABORTED      │
                    │  (terminal)  │               │    (terminal)   │
                    └──────────────┘               └─────────────────┘
```

### 7B — Late Joiner Handling

```
Consumer connects AFTER refresh already started (broker restart scenario):

SubscribeHandler detects:
  anyRefreshActive(topics) == true
      │
      ├── state == RESET_SENT or REPLAYING:
      │     markLegacyConsumerReady(clientId)
      │     registerLateJoiningConsumer(topic, groupTopic)
      │       - recordResetAck(groupTopic)   ← treats as if RESET was ACKed
      │       - if RESET_SENT && allResetAcksReceived():
      │           transition → REPLAYING     ← FIX: drives state machine
      │           cancelResetRetry()
      │           scheduleReplayCheck()
      │
      └── state == READY_SENT:
            sendRefreshReadyToConsumer(clientId)  ← direct READY to late joiner
```

### 7C — Recovery on Broker Restart

```
Broker starts
      │
      ▼
RefreshRecoveryService.recoverAndResumeRefreshes()
      │  loads all refresh-state/*.json files
      │
      ├── state == RESET_SENT (all ACKs persisted):
      │     → advance directly to REPLAYING        ← avoids redundant RESET
      │     → scheduleReplayCheck()
      │
      ├── state == RESET_SENT (missing ACKs):
      │     → re-send RESET broadcast
      │     → scheduleResetRetry(every 5s)
      │
      ├── state == REPLAYING:
      │     → scheduleReplayCheck(every 1s)
      │
      ├── state == READY_SENT:
      │     → scheduleReadyTimeout(10s)
      │
      └── state == COMPLETED:
            → remove from activeRefreshes
            → delete state file
```

---

## 8. Threading Model

```
┌─────────────────────────────────────────────────────────────────────────┐
│                        THREAD POOLS                                     │
│                                                                         │
│  ┌─────────────────────────┐   ┌─────────────────────────────────────┐ │
│  │  Netty Boss (×2)        │   │  Netty Worker (×16)                 │ │
│  │                         │   │                                     │ │
│  │  accept() TCP           │   │  read / decode / write / encode     │ │
│  │  connections            │   │  → dispatch to handler              │ │
│  └─────────────────────────┘   └─────────────────────────────────────┘ │
│                                                                         │
│  ┌─────────────────────────┐   ┌─────────────────────────────────────┐ │
│  │  TopicFairScheduler     │   │  ackExecutor (4–8 threads)          │ │
│  │  (NumCPU threads)       │   │                                     │ │
│  │                         │   │  Processes BATCH_ACK messages off   │ │
│  │  Delivery tasks per     │   │  the Netty event loop               │ │
│  │  consumer, gated by:    │   │  → offsetTracker.update()           │ │
│  │   WatermarkGatePolicy   │   │                                     │ │
│  │   RefreshGatePolicy     │   └─────────────────────────────────────┘ │
│  │  Adaptive backoff:      │                                           │
│  │   1ms → 1s (exp)        │   ┌─────────────────────────────────────┐ │
│  │   30s (fixed)           │   │  consumerScheduler (2+ threads)     │ │
│  └─────────────────────────┘   │                                     │ │
│                                │  READY retries                      │ │
│  ┌─────────────────────────┐   │  Legacy ACK timeouts (60s)          │ │
│  │  dataRefreshScheduler   │   └─────────────────────────────────────┘ │
│  │  (2 threads)            │                                           │
│  │                         │   ┌─────────────────────────────────────┐ │
│  │  RESET retry (5s)       │   │  flushScheduler (1 thread)          │ │
│  │  Replay check (1s)      │   │                                     │ │
│  │  Ready timeout (10s)    │   │  consumer-offsets.properties        │ │
│  │  Abort watchdog (10min) │   │  flush every 5s                     │ │
│  └─────────────────────────┘   └─────────────────────────────────────┘ │
│                                                                         │
│  ┌─────────────────────────┐   ┌─────────────────────────────────────┐ │
│  │  TopologyManager (1)    │   │  HttpPipeConnector (1)              │ │
│  │                         │   │                                     │ │
│  │  Registry poll every 30s│   │  Parent polling 200ms–10s           │ │
│  └─────────────────────────┘   │  Paused during data refresh         │ │
│                                └─────────────────────────────────────┘ │
└─────────────────────────────────────────────────────────────────────────┘
```

### Key Synchronization Rules

| Rule | Reason |
|------|--------|
| Never acquire storage lock from Netty thread | Prevents I/O blocking event loop |
| ACK processing offloaded to ackExecutor | Netty thread returns immediately |
| Offset flush is async (5s) | Avoids write-per-ACK latency spike |
| Refresh state in ConcurrentHashMap + atomic ops | No global lock on delivery path |
| One delivery task per legacy clientId | Prevents N scheduler threads racing on same connection |

---

## 9. Deployment Architecture

### Docker Compose Services (16 containers)

```
┌──────────────────────────────────────────────────────────────────────────┐
│                         messaging-network                                │
│                                                                          │
│  ┌────────────────┐                                                      │
│  │  cloud-server  │  :8080                                               │
│  │  300 MB        │  SQLite: events.db, topology registry                │
│  └───────┬────────┘                                                      │
│          │ HTTP topology + pipe                                          │
│  ┌───────▼────────┐                                                      │
│  │    broker      │  :9092 (TCP)   :8081 (HTTP/metrics)                  │
│  │    600 MB      │  DATA_DIR=/app/data                                  │
│  └──┬──┬──┬──┬───┘                                                      │
│     │  │  │  │  TCP connections                                          │
│     │  │  │  └─────────────────────────────────────────────────────┐    │
│     │  │  └──────────────────────────────────────────┐             │    │
│     │  └───────────────────────────────┐             │             │    │
│     │                                  │             │             │    │
│  ┌──▼──────────────────┐  ┌────────────▼──┐  ┌──────▼──┐  ┌──────▼──┐ │
│  │consumer-price-quote │  │consumer-prod  │  │  ...×9  │  │consumer │ │
│  │:8090  200MB  6 topics│  │svc :8091 200MB│  │consumers│  │dcxp-ugc │ │
│  └─────────────────────┘  └───────────────┘  └─────────┘  │:8102    │ │
│                                                             └─────────┘ │
│  ┌──────────────────────────────────────────┐                           │
│  │  prometheus :9090  ←scrape broker:8081   │                           │
│  └─────────────────────┬────────────────────┘                           │
│                        │ datasource                                      │
│  ┌─────────────────────▼────────────────────┐                           │
│  │  grafana :3000   4 dashboards             │                           │
│  └──────────────────────────────────────────┘                           │
└──────────────────────────────────────────────────────────────────────────┘
```

### Consumer Services Map (13 services, 24 topics)

| Service | Port | Topics | Protocol |
|---------|------|--------|----------|
| consumer-price-quote | 8090 | prices-v1, reference-data-v5, non-promotable-products, prices-v4, minimum-price, deposit | Legacy |
| consumer-product-svc-lite | 8091 | product-base-document | Legacy |
| consumer-search-enterprise | 8092 | search-product | Legacy |
| consumer-tesco-location-service | 8093 | location, location-clusters | Legacy |
| consumer-customer-order-on-till | 8094 | selling-restrictions | Legacy |
| consumer-colleague-facts | 8095 | colleague-facts-jobs, colleague-facts-legacy | Legacy |
| consumer-loss-prevention-api | 8096 | loss-prevention-configuration, loss-prevention-store-configuration, loss-prevention-product, loss-prevention-rule-config | Legacy |
| consumer-stored-value-services | 8097 | stored-value-services-banned-promotion, stored-value-services-active-promotion | Legacy |
| consumer-colleague-identity | 8098 | colleague-card-pin | Legacy |
| consumer-distributed-identity | 8099 | colleague-card-pin-v2 | Legacy |
| consumer-dcxp-content | 8100 | dcxp-content | Legacy |
| consumer-restriction-service-on-tills | 8101 | restriction-rules | Legacy |
| consumer-dcxp-ugc | 8102 | dcxp-ugc | Legacy |

> All 13 current consumers use the **Legacy protocol** (single TCP connection, multiple topics via k-way merge).

---

## 10. Observability

### Metrics Taxonomy

```
BROKER METRICS (BrokerMetrics)
├── Throughput Counters
│   ├── broker_messages_received_total
│   ├── broker_messages_stored_total
│   ├── broker_messages_sent_total
│   ├── broker_bytes_received_total
│   └── broker_bytes_sent_total
│
├── Per-Consumer Counters {consumer_id, topic, group}
│   ├── broker_consumer_messages_sent_total
│   ├── broker_consumer_bytes_sent_total
│   ├── broker_consumer_acks_total
│   ├── broker_consumer_failures_total
│   ├── broker_consumer_retries_total
│   └── broker_consumer_ack_timeouts_total
│
├── Per-Consumer Gauges {consumer_id, topic, group}
│   ├── broker_consumer_offset
│   ├── broker_consumer_lag
│   ├── broker_consumer_last_delivery_time
│   └── broker_consumer_last_ack_time
│
└── Latency Timers (p50/p95/p99)
    ├── broker_storage_write_seconds
    ├── broker_storage_read_seconds
    ├── broker_message_delivery_latency_seconds
    └── broker_message_e2e_latency_seconds

DATA REFRESH METRICS (DataRefreshMetrics)
├── Gauges {topic, refresh_id, refresh_type}
│   ├── data_refresh_bytes_transferred_total_bytes    ← key: size of replay
│   ├── data_refresh_messages_transferred_total       ← key: count of replay
│   ├── data_refresh_start_time_seconds
│   ├── data_refresh_end_time_seconds
│   └── data_refresh_transfer_rate_bytes_per_second
│
└── Histograms {topic, consumer, refresh_id}
    ├── data_refresh_reset_ack_duration_seconds
    ├── data_refresh_ready_ack_duration_seconds
    └── data_refresh_duration_seconds
```

### Key Grafana Queries

```promql
-- Total bytes replayed per refresh
sum by(refresh_id) (
  max_over_time(data_refresh_bytes_transferred_total_bytes[$__range:])
)

-- Consumer lag
broker_consumer_lag{consumer_id="$consumer"}

-- Refresh duration
max by(topic) (data_refresh_end_time_seconds{refresh_id="$refresh_id"})
- max by(topic) (data_refresh_start_time_seconds{refresh_id="$refresh_id"})

-- E2E latency p99
histogram_quantile(0.99, rate(broker_message_e2e_latency_seconds_bucket[5m]))
```

### HTTP Admin API

| Endpoint | Method | Purpose |
|----------|--------|---------|
| `/admin/refresh-topic` | POST | Trigger data refresh for one or more topics |
| `/admin/refresh-status?topic=X` | GET | Current refresh state for topic |
| `/admin/refresh-current` | GET | Active refresh context |
| `/admin/threads` | GET | Thread pool diagnostics |
| `/health` | GET | Micronaut health (includes RefreshHealthIndicator) |
| `/prometheus` | GET | Prometheus scrape endpoint |

---

## 11. Missing / Recommended Additions

### Priority 1 — Reliability (High Impact)

#### 1.1 Message Compaction
```
PROBLEM: Storage grows unbounded. DELETE tombstones never purged.
         storage.compact() exists but is never called.

ADD:
  SegmentCompactionService (scheduled, off-peak)
  ├── Scan sealed segments for topic
  ├── Keep only latest value per msgKey
  ├── Write compacted segment
  └── Atomically swap in new segment

CONFIGURATION:
  broker.storage.compaction.enabled: true
  broker.storage.compaction.cron: "0 2 * * *"   # 2am daily
  broker.storage.compaction.min-segment-age-hours: 24
```

#### 1.2 Circuit Breaker on Consumer Failures
```
PROBLEM: A slow/dead consumer keeps receiving retries with exponential backoff.
         No alerting when a consumer fails for a sustained period.

ADD:
  ConsumerCircuitBreaker (per consumer)
  ├── CLOSED  → normal delivery
  ├── OPEN    → stop delivery, emit metric, alert
  └── HALF_OPEN → probe with single batch

TRIGGER:  failure count > 10 within 60s window
RECOVERY: consumer re-subscribes (SUBSCRIBE message)

METRIC:   broker_consumer_circuit_breaker_state{consumer_id, topic} = 0|1|2
```

#### 1.3 ACK-Based Offset Guarantee
```
PROBLEM: Offsets flushed every 5s. Crash between flush = duplicate delivery.
         Currently "at-least-once" by design, but undocumented.

ADD:
  - Document delivery guarantee explicitly (at-least-once)
  - Flush offsets synchronously on graceful shutdown
  - Add metric: broker_consumer_unflushed_acks (how many in pending flush)
```

### Priority 2 — Operations (Medium Impact)

#### 2.1 Structured Correlation IDs through Pipe
```
PROBLEM: traceId generated at broker but not propagated via HTTP pipe.
         Cross-broker message tracing is impossible.

ADD:
  - X-Trace-Id header on /pipe/poll responses
  - Preserve traceId from parent MessageRecord in child broker
  - Log parent traceId alongside child traceId
```

#### 2.2 Authentication Enforcement
```
PROBLEM: AUTH_BEARER_TOKEN config exists but not enforced.
         Any TCP client can subscribe and receive messages.

ADD:
  SubscribeHandler: validate bearer token on SUBSCRIBE
  NettyTcpServer: reject unauthenticated connections after grace period
  Per-topic ACL (future): allow/deny per consumer group
```

#### 2.3 Backpressure Signal from Consumer
```
PROBLEM: Consumer cannot slow down the broker.
         1MB batch cap is the only rate control.

ADD:
  BrokerMessage.SLOW_DOWN  (new type 0x0D)  Consumer → Broker
  BrokerMessage.RESUME     (new type 0x0E)  Consumer → Broker

  DeliveryScheduler: if SLOW_DOWN received → pause delivery for 5s
  Metric: broker_consumer_backpressure_events_total{consumer_id}
```

#### 2.4 Health Indicator for Storage
```
PROBLEM: /health only covers RefreshHealthIndicator.
         Full disk or segment corruption not surfaced.

ADD:
  StorageHealthIndicator (Micronaut @Singleton HealthIndicator)
  ├── check: free disk space > 10%
  ├── check: active segment writeable
  └── check: SQLite metadata store accessible

  STATUS: UP / DOWN / WARN
```

### Priority 3 — Scalability (Lower Urgency)

#### 3.1 Multiple Partitions per Topic
```
CURRENT: All topics hardcoded to partition-0.
ADD:     partition count per topic in config, round-robin on producer side.
BENEFIT: Parallel writes and reads; horizontal throughput scaling.
```

#### 3.2 Replication / HA
```
CURRENT: Single broker, single point of failure.
ADD:
  - Leader-follower replication (broker-001 leads, broker-002 follows)
  - Follower polls leader via existing pipe mechanism
  - On leader failure: follower promotes (manual or via registry)
```

#### 3.3 Schema Registry
```
CURRENT: Messages are opaque strings (JSON assumed but not validated).
ADD:
  - SchemaRegistry service (Avro or JSON Schema)
  - DataHandler validates payload on write
  - Consumers receive schema version header with batch
```

### Priority 4 — Developer Experience

#### 4.1 Replay API (per consumer)
```
ADD:  POST /admin/replay?consumer=price-quote-service&topic=prices-v1&from=0
      → resets only that consumer's offset (no broadcast RESET)
      → useful for individual consumer replay without full data refresh
```

#### 4.2 Message Browser
```
ADD:  GET /admin/browse?topic=prices-v1&offset=100&limit=10
      → returns decoded messages from storage for inspection
      → essential for debugging stale or corrupted messages
```

#### 4.3 Consumer Lag Alert
```
ADD:  Alertmanager rule:
      ALERT ConsumerLagHigh
        IF broker_consumer_lag{group!=""} > 10000
        FOR 5m
        LABELS {severity="warning"}
        ANNOTATIONS {summary="Consumer {{ $labels.consumer_id }} is {{ $value }} messages behind"}
```

---

## Best Practices

### 1. Dependency Injection

**Constructor injection is the default.** All dependencies are declared as constructor parameters with `final` fields — no field injection (`@Inject` on fields). This makes the dependency graph explicit and enables clean unit testing.

```java
// BrokerService.java — 10 deps, all final, all visible at construction time
public BrokerService(StorageEngine storage, NetworkServer network, ...) {
    this.storage = storage;
    ...
}
```

**Setter injection only for circular dependencies.** When two singletons depend on each other (e.g., `RefreshCoordinator ↔ ConsumerRegistry`), one side uses a `volatile` field and a setter called from `@PostConstruct`:

```java
// ConsumerRegistry.java
private volatile RefreshCoordinator refreshCoordinator;  // set after construction

public void setRefreshCoordinator(RefreshCoordinator c) { this.refreshCoordinator = c; }

// RefreshCoordinator.java
@PostConstruct
public void init() {
    remoteConsumers.setRefreshCoordinator(this);  // break the cycle
}
```

**Named qualifiers for multiple beans of the same type.** All five executor services are created in a single `@Factory` class and injected with `@Named`:

```java
// ExecutorFactory.java — one factory, five named executors
@Named("ackExecutor") @Singleton ExecutorService ackExecutor() { ... }
@Named("consumerScheduler") @Singleton ScheduledExecutorService consumerScheduler() { ... }
```

**Conditional bean registration.** Storage engine and network server implementations are selected at startup via `@Requires`:

```java
@Requires(property = "broker.storage.type", value = "filechannel")
public class FileChannelStorageEngine implements StorageEngine { ... }

@Requires(property = "broker.network.type", value = "tcp")
public class NettyTcpServer implements NetworkServer { ... }
```

---

### 2. Concurrency

**Lock-free data structures over `synchronized`.** `ConcurrentHashMap` is the default for shared maps; `ConcurrentSkipListMap` for sorted maps (segment lookup); `CopyOnWriteArrayList` for handler lists that are read far more than written.

```java
// SegmentManager.java
ConcurrentSkipListMap<Long, Segment> segments = new ConcurrentSkipListMap<>();
AtomicReference<Segment> activeSegment = new AtomicReference<>();

// NettyTcpServer.java
List<MessageHandler> handlers = new CopyOnWriteArrayList<>();
```

**`volatile` for visibility, `AtomicLong`/`AtomicReference` for atomic updates.** Fields shared across threads but updated by a single writer use `volatile`. Fields updated concurrently (counters, references) use atomic types.

```java
// HttpPipeConnector.java
private volatile boolean running;
private volatile PipeConnectionImpl connection;

// BrokerMetrics.java
ConcurrentHashMap<String, AtomicLong> consumerOffsets = new ConcurrentHashMap<>();
```

**`CompletableFuture` for all async I/O.** Network send operations return `CompletableFuture<Void>`; the Netty `ChannelFuture` is bridged inside. Callers compose results without blocking threads.

```java
// NettyTcpServer.java
public CompletableFuture<Void> send(String clientId, BrokerMessage msg) {
    CompletableFuture<Void> future = new CompletableFuture<>();
    channel.writeAndFlush(msg).addListener(f -> {
        if (f.isSuccess()) future.complete(null);
        else future.completeExceptionally(f.cause());
    });
    return future;
}
```

**Named thread pools sized by purpose, not by convention.** Each logical concern has its own named pool; size is based on workload type (I/O-bound vs CPU-bound):

| Pool | Size | Purpose |
|------|------|---------|
| `ackExecutor` | 4 threads | ACK processing |
| `consumerScheduler` | 2 threads | Delivery scheduling |
| `storageExecutor` | `nCPU` | Storage I/O |
| `dataRefreshScheduler` | 2 threads | Refresh workflow |
| `flushScheduler` | 1 thread | Offset flush |

Thread names follow `"PoolName-N"` (e.g., `"ACK-Processor-1"`) for readable thread dumps.

**`InterruptedException` always restores the interrupt flag.**

```java
} catch (InterruptedException e) {
    scheduler.shutdownNow();
    Thread.currentThread().interrupt();  // restore flag, never swallow
}
```

---

### 3. Error Handling

**Custom typed exception hierarchy.** `MessagingException` is the base; `StorageException` and `NetworkException` extend it with error codes and context. Callers catch the specific type they can handle; everything else propagates.

```java
throw new StorageException("Write failed", StorageException.ErrorCode.STORAGE_WRITE_FAILED)
        .withTopic(topic)
        .withDetails("segment=" + currentSegment.getId());
```

**`ExceptionLogger.logAndThrow()` — log once, rethrow.** Avoids duplicated log-then-rethrow patterns. Includes a `getMonitoringSummary()` one-liner for alerting.

```java
throw ExceptionLogger.logAndThrow(log,
    new StorageException(...).withTopic(topic));
```

**Boolean return for non-fatal failures on hot paths.** Methods that may fail transiently (e.g., pipe message handling) return `boolean` instead of throwing. Callers decide whether to retry, skip, or log.

```java
// BrokerService.java
public boolean handlePipeMessage(MessageRecord record) {
    try { ... return true; }
    catch (Exception e) { log.error(...); return false; }
}
// HttpPipeConnector advances offset only when true is returned
```

**Graceful degradation in protocol detection.** If the first byte cannot be parsed, the pipeline defaults to the modern decoder and continues — no crash, a warning log.

---

### 4. Observability

**Micrometer metrics with consistent tag taxonomy.** All metrics share the same tag set (`topic`, `consumer_id`, `group`, `refresh_id`, `refresh_type`) so Grafana can correlate across panels without joins.

Metric types by use case:
- `Counter` — event counts (messages received, stored, sent)
- `Timer` with percentiles (p50/p95/p99) — latency (storage write, e2e, delivery)
- `Gauge` — current state (consumer offset, lag, active consumers)
- `DistributionSummary` — size distributions (batch sizes, bytes transferred)

**Structured `LogContext` for machine-readable logs.** Instead of ad-hoc string interpolation, a builder produces `"topic=X, clientId=Y, traceId=Z"` that log aggregators can parse.

```java
LogContext ctx = LogContext.builder()
    .topic(topic).clientId(clientId).traceId(traceId).build();
log.info("[CONSUMER] Registered: {}", ctx);
// → "[CONSUMER] Registered: topic=prices-v1, clientId=client-1, traceId=abc123"
```

**Trace IDs on every request.** `TraceIds.newTraceId()` is called at the entry point of each message and propagated through all handler calls. Enables correlated log search across broker components.

**Health indicators for critical subsystems.** `RefreshHealthIndicator` exposes data refresh state at `/health`, enabling orchestration platforms to route traffic away from a stuck broker.

---

### 5. Resource Lifecycle

**`@PostConstruct` / `@PreDestroy` for every stateful singleton.** Initialization (starting schedulers, wiring late dependencies) happens in `@PostConstruct` after DI is complete. Cleanup (cancelling tasks, flushing state, shutting down executors) happens in `@PreDestroy` before JVM exit.

**Executor shutdown sequence — always timeout then force.**

```java
executor.shutdown();
if (!executor.awaitTermination(10, TimeUnit.SECONDS)) {
    List<Runnable> dropped = executor.shutdownNow();
    log.warn("Force-shutdown dropped {} task(s)", dropped.size());
}
```

**Offset persistence on a dedicated single-threaded flush scheduler.** Consumer offsets are flushed to disk every N seconds by `flushScheduler` (1 thread), not inline with delivery. This prevents storage I/O from blocking the delivery hot path.

---

### 6. Design Patterns

**State machine for the refresh workflow.** `RefreshStateMachine` holds a `Map<RefreshState, Set<RefreshState>>` of valid transitions and rejects illegal ones with a structured `StateTransitionResult`. All state changes go through `stateMachine.transition()` — no direct state field assignments in business logic.

```java
RefreshWorkflow.StateTransitionResult result =
    stateMachine.transition(context.getState(), RefreshState.REPLAYING);
if (result.isSuccess()) {
    context.setState(RefreshState.REPLAYING);
}
```

**Strategy / policy chain for delivery gating.** `DeliveryScheduler` holds a `List<DeliveryGatePolicy>` and evaluates each in order. New policies (e.g., backpressure, quota) are added without changing the scheduler.

```java
// DeliveryScheduler.java
for (DeliveryGatePolicy policy : gatePolicies) {
    if (!policy.allowDelivery(consumer, topic)) return;
}
```

**Registry for handler lookup.** `MessageHandlerRegistry` maps `EventType → MessageHandler` using `ConcurrentHashMap`. Adding a new message type requires registering a handler — no switch statement to update.

**Factory for parser selection.** `MessageParserFactory` inspects the first byte of a subscribe payload to pick the legacy or modern parser. No conditional logic leaks into the handler.

**Protocol detection by self-replacing decoder.** `ProtocolDetectionDecoder` reads the first byte, installs the correct decoder in the Netty pipeline, removes itself, and re-fires the buffer — transparent to all downstream handlers.

---

### 7. Storage Patterns

**Sequential append-only writes.** No random writes. Every message is appended to the active segment's end, keeping write throughput sequential and predictable.

**Active vs. sealed read strategies.**
- *Active segment*: sequential scan from last known position — no index overhead.
- *Sealed segment*: binary search on sparse index (one entry per 4 KB) → O(log n) seek, then short forward scan.

**Zero-copy delivery via `FileChannel` + Netty `FileRegion`.** Sealed segment data is sent to consumers using OS-level `sendfile()` — no user-space copy of message bytes.

**Watermark tracker decouples write from delivery notification.** After `storage.append()`, the broker updates an `AtomicLong` watermark. The delivery scheduler polls this watermark instead of doing a storage read to check for new messages.

---

### 8. Testing

**Spock BDD specs with `given / when / then` blocks.** Every unit test reads as a sentence. Parameterized tests use Spock's `where:` tables, not duplicated test methods.

```groovy
def "should reject invalid transition from COMPLETED to RESET_SENT"() {
    given: "a completed refresh"
    when: "transition to RESET_SENT is attempted"
    then: "result is failure"
}
```

**`@TempDir` for isolated file-system tests.** All storage tests receive a fresh temp directory per test. No shared state, no cleanup code.

**`PollingConditions` for async assertions.** Async delivery tests wait up to N seconds for a condition rather than using `Thread.sleep()`.

```groovy
def conditions = new PollingConditions(timeout: 3)
conditions.eventually {
    assert harness.received.any { it.topic == "prices-v1" }
}
```

**Separate `unitTest` and `integrationTest` source sets.** Unit specs mock all I/O dependencies and run in milliseconds. Integration specs start the real embedded broker with `AppHarness.start()` and test against actual TCP connections.

---

## Architecture Decision Log

| Decision | Rationale | Trade-off |
|----------|-----------|-----------|
| **FileChannel over mmap** | Avoids mmap page fault latency spikes under GC pressure; simpler memory management | Slightly lower peak throughput vs true mmap |
| **Sparse index (every 4KB)** | O(log n) lookup with minimal index memory footprint | Miss-seek on reads, recover with sequential scan |
| **Push delivery (watermark polling)** | <5ms latency; avoids consumer-driven pull complexity | Broker must track delivery state per consumer |
| **k-way merge for legacy** | Single TCP connection handles 6 topics in global offset order | Complex ACK tracking (LegacyConnectionState FIFO queue) |
| **No CRC on reads** | Removed for performance; OS + filesystem provide integrity | Silent corruption theoretically possible (very low risk) |
| **Refresh per topic (not global)** | Fine-grained; one topic refresh doesn't block others | Coordination complexity across topics for same consumer |
| **At-least-once delivery** | Simpler implementation; consumers handle idempotency | Duplicate messages on consumer crash between flush intervals |
| **Single partition** | Simplicity; avoid partition assignment complexity | No parallel write/read throughput scaling |
| **Pipe polling (not push)** | Child broker can restart without parent knowing; resumable | Adds 200ms–10s latency through hierarchy |

---

*Generated: 2026-03-16 | Branch: remove-crc-validation*
