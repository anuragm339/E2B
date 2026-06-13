# Event And Kafka-Like Flow

Related: [Overview](01-system-overview.md), [Features](03-feature-catalog.md), [Data model](05-data-model.md), [POS state](07-pos-machine-state.md), [Compaction](08-compaction-handling.md).

## Kafka Clarification

There is no Apache Kafka dependency, producer, consumer, broker protocol, or Kafka topic configuration in this repository. "Kafka-like" means the code implements:

- named topics;
- partition `0`;
- monotonic/sparse offsets;
- consumer groups;
- committed offsets;
- batched delivery and ACK;
- keyed compaction and tombstones.

Sources: `settings.gradle`, module build files, `StorageEngine.java`, `ConsumerOffsetTracker.java`, and compaction package.

## Modern Wire Protocol

Envelope:

```text
[messageType:1][messageId:8][payloadLength:4][payload]
```

Definitions: `common/src/main/java/com/messaging/common/model/BrokerMessage.java`. Codec: `network/src/main/java/com/messaging/network/codec/BinaryMessageEncoder.java` and `BinaryMessageDecoder.java`.

Codes:

| Code | Type |
|---:|---|
| `0x01` | `DATA` |
| `0x02` | `ACK` |
| `0x03` | `SUBSCRIBE` |
| `0x04` | `COMMIT_OFFSET` |
| `0x05` | `RESET` |
| `0x06` | `READY` |
| `0x07` | `DISCONNECT` |
| `0x08` | `HEARTBEAT` |
| `0x09` | `BATCH_HEADER` |
| `0x0A` | `BATCH_ACK` |
| `0x0B` | `RESET_ACK` |
| `0x0C` | `READY_ACK` |

No `HEARTBEAT` handler is registered by `HandlerInitializer.java`.

## Producer Flow

```text
Producer DATA JSON
  -> BinaryMessageDecoder
  -> ServerMessageHandler
  -> BrokerService handler registry
  -> DataHandler
  -> StorageEngine.append(topic, 0, record)
  -> CompactionIndex.updateKey
  -> ACK(messageId)
```

Sources: `NettyTcpServer.java`, `ServerMessageHandler.java`, `BrokerService.java`, `DataHandler.java`.

Producer JSON fields:

- required: `topic`
- optional: `msg_key`, `event_type`, `data`

`DELETE` creates a null-data tombstone. Other event-type text becomes `MESSAGE`.

## Pipe Replication Flow

```text
TopologyManager
  -> CloudRegistryClient GET /registry/topology?nodeId=...
  -> select/probe parent
  -> HttpPipeConnector GET /pipe/poll?offset=...&limit=...
  -> streaming JSON MessageRecord
  -> BrokerService.handlePipeMessage
  -> duplicate-offset guard
  -> StorageEngine.append
  -> CompactionIndex.updateKey
  -> persist pipe.current.offset
```

Sources: `CloudRegistryClient.java`, `TopologyManager.java`, `HttpPipeConnector.java`, `BrokerService.java`.

The handler returns a boolean. The connector advances only through the last successful record and retries failures.

## Modern Subscription Flow

```text
SUBSCRIBE {topic, group}
  -> SubscribeHandler
  -> restore group:topic offset
  -> register RemoteConsumer
  -> ACK subscribe
  -> READY(topic)
  <- READY_ACK(topic, group)
  -> mark readiness
```

During active refresh, `SubscribeHandler` follows late-joiner rules instead of sending an ordinary startup READY. Source: `broker/src/main/java/com/messaging/broker/handler/SubscribeHandler.java`.

## Modern Delivery Flow

```text
DeliveryScheduler
  -> RefreshGatePolicy
  -> WatermarkGatePolicy
  -> BatchDeliveryService gates
  -> BatchReadableStorage.getBatch
  -> optional compaction delivery filter
  -> BATCH_HEADER(count, bytes, topic, group)
  -> raw segment bytes via FileRegion
  -> ZeroCopyBatchDecoder
  -> BatchDecodedEvent
  -> client BatchAckHandler sends BATCH_ACK(topic, group)
  -> broker BatchAckHandler on ackExecutor
  -> BatchAckService commits next offset
  -> async per-record RocksDB ACK write
```

Sources:

- Scheduling: `DeliveryScheduler.java`, `TopicFairScheduler.java`
- Broker delivery: `BatchDeliveryService.java`
- Transport: `NettyTcpServer.java`
- Client decode/ACK: `ZeroCopyBatchDecoder.java`, network `BatchAckHandler.java`
- Broker ACK: broker `BatchAckHandler.java`, `BatchAckService.java`

### Batch Header

Payload:

```text
[recordCount:4][totalBytes:8]
[topicLen:4][topic]
[groupLen:4][group]
```

Raw records use the current segment log format without offsets or CRC. `ZeroCopyBatchDecoder.decodeZeroCopyBatch` is the authoritative client parser.

If the decoder parses fewer records than advertised, it does not ACK and closes the channel to force replay.

## Legacy Wire Protocol

Protocol detection is based on the first byte. A legacy connection must begin with `REGISTER` ordinal `0`. Sources: `DefaultProtocolDetectionService.java`, `ProtocolDetectionDecoder.java`.

Legacy event ordinals from `network/src/main/java/com/messaging/network/legacy/events/EventType.java`:

```text
REGISTER, MESSAGE, RESET, READY, ACK, EOF, DELETE, BATCH
```

`LegacyEventDecoder` converts legacy events to internal `BrokerMessage` types. `LegacyEventEncoder` performs the reverse.

`LegacyConnectionState` tracks a FIFO of outbound expectations:

- outbound RESET -> generic ACK becomes `RESET_ACK`;
- outbound READY -> generic ACK becomes `READY_ACK`;
- outbound DATA/BATCH -> generic ACK becomes `BATCH_ACK`.

For refresh ACKs, it creates the structured topic payload with an empty group; broker handlers resolve the group from the consumer registry.

## Legacy Delivery Flow

```text
REGISTER(serviceName)
  -> SUBSCRIBE legacy adapter
  -> service-to-topics map
  -> one RemoteConsumer per topic
  -> startup READY / ACK
  -> LegacyConsumerDeliveryManager creates TopicCursor per topic
  -> priority-queue k-way merge by offset
  -> JSON BatchEvent
  <- generic ACK
  -> commit max offset for every topic in batch
  -> per-record RocksDB ACKs
```

Sources: `SubscribeHandler.java`, `LegacyClientConfig.java`, `LegacyConsumerDeliveryManager.java`, `ConsumerRegistry.java`, `BatchAckService.java`.

## Refresh Event Flow

```text
pause pipe
RESET(topic) -> RESET_ACK(topic, group)
reset group offset to 0
replay BATCH_HEADER/raw/BATCH_ACK until caught up
READY(topic) -> READY_ACK(topic, group)
resume pipe and ACK reconciliation
```

Full state and race handling: [POS machine state](07-pos-machine-state.md).

## Compaction Event Semantics

- Same topic/key later offset supersedes earlier offset.
- Null key is not compactable.
- A latest `DELETE` remains until tombstone retention expires.
- Physical compaction keeps original offsets, creating gaps.
- Delivery-time filtering prevents stale values before rewrite.

Sources: `CompactionIndex.java`, `CompactionRewriter.java`, `BatchDeliveryService.java`. See [Compaction](08-compaction-handling.md).

## Topic Inventory

Topics are dynamic; no enum or central complete topic registry exists. Sources are:

- producer `topic` JSON;
- pipe `MessageRecord.topic`;
- `legacy-clients.service-topics` in `broker/src/main/resources/application.yml`;
- test fixtures.

Configured legacy topic names are indexed in [Runtime config](11-runtime-config.md#legacy-service-topic-map).
