# Data Model

Related: [Features](03-feature-catalog.md), [Events](06-event-kafka-flow.md), [POS state](07-pos-machine-state.md), [Compaction](08-compaction-handling.md), [Recovery](09-error-retry-recovery.md).

## Record Models

### `MessageRecord`

Source: `common/src/main/java/com/messaging/common/model/MessageRecord.java`.

Fields used by the system:

- `offset`
- `topic`
- `partition`
- `msgKey`
- `eventType`
- nullable `data`
- `createdAt`
- `contentType`

Producer construction is in `broker/src/main/java/com/messaging/broker/handler/DataHandler.java`; pipe JSON binding is in `pipe/src/main/java/com/messaging/pipe/HttpPipeConnector.java`.

### `ConsumerRecord`

Source: `common/src/main/java/com/messaging/common/model/ConsumerRecord.java`.

The modern client receives key, event type, data, and timestamp. Offset is tracked by the broker rather than included in each decoded client record. The current zero-copy decoder confirms this in `network/src/main/java/com/messaging/network/codec/ZeroCopyBatchDecoder.java`.

### Event Types

`common/src/main/java/com/messaging/common/model/EventType.java` defines:

- `MESSAGE`, wire code `'M'`
- `DELETE`, wire code `'D'`

DELETE records have null data in producer ingestion and act as tombstones for compaction.

## Segment Storage

Sources:

- `storage/src/main/java/com/messaging/storage/segment/Segment.java`
- `storage/src/main/java/com/messaging/storage/segment/SegmentManager.java`
- `storage/src/main/java/com/messaging/storage/segment/DefaultStorageRecoveryService.java`

Directory layout:

```text
<dataDir>/
  <topic>/
    segment_metadata.db
    partition-0/
      00000000000000000000.log
      00000000000000000000.index
      00000000000000000000.compacted.log
      00000000000000000000.compacted.index
      *.compacting.*
```

Only partition `0` is used by broker handlers and pipe code.

### Log Format

The file starts with a `MLOG` header and version. Each current-format record is:

```text
[keyLen:4][key:keyLen][eventType:1][dataLen:4][data:dataLen][timestampMillis:8]
```

Offsets are not stored in the log payload. Source: `Segment.java`.

### Index Format

The file starts with `MIDX` and a version.

Current v2 entry:

```text
[offset:8][logPosition:4][recordSize:4]
```

Recovery also recognizes the older 20-byte v1 entry with a CRC field that is ignored by current reads. Source: `Segment.java`.

The index is dense: one entry per log record. `Segment.findPositionForOffset` binary-searches for the first offset greater than or equal to the requested offset, so sparse parent offsets are supported.

### Segment Lifecycle

- Active segment accepts appends.
- Full or explicitly rolled segment becomes sealed/inactive.
- Reads may walk sealed and active segments.
- `getBatch` returns data from one segment and does not cross a segment boundary.
- File-backed `DeliveryBatch.transferTo` uses the segment's channel.
- Compaction atomically replaces sealed segment ranges.

Source: `SegmentManager.java`.

### Durability

The active segment is periodically forced by `FileChannelStorageEngine`. Segment seal also forces files. There is no force per record. Configuration is in `broker/src/main/resources/application.yml`.

Recovery:

- validates file headers;
- truncates orphan log bytes when the index is missing or shorter;
- truncates index entries that point beyond the log;
- ignores/removes staging files;
- reopens the last recovered segment as active where applicable.

Tests: `storage/src/unitTest/groovy/com/messaging/storage/segment/SegmentCrashRecoverySpec.groovy` and `storage/src/integrationTest/groovy/com/messaging/storage/segment/DefaultStorageRecoveryServiceIntegrationSpec.groovy`.

## SQLite Segment Metadata

No migration directory or `.sql` migration file exists. Schema creation is runtime code in `storage/src/main/java/com/messaging/storage/metadata/SegmentMetadataStore.java`.

Table `segment_metadata`:

| Column | Meaning |
|---|---|
| `id` | SQLite primary key |
| `topic` | Topic name |
| `partition` | Partition number |
| `base_offset` | First segment offset |
| `max_offset` | Highest indexed offset |
| `log_file_path` | Log path |
| `index_file_path` | Index path |
| `size_bytes` | Segment size |
| `message_count` | Record count |
| `created_at` | Creation time |
| `updated_at` | Update time |

Indexes:

- `idx_topic_partition(topic, partition)`
- `idx_base_offset(base_offset)`

The metadata store is updated periodically and on roll/close by `SegmentManager`.

## RocksDB

Source: `broker/src/main/java/com/messaging/broker/compaction/SharedRocksDb.java`.

One RocksDB instance is shared:

- default column family: per-record ACK data
- `compaction` column family: latest-key index and compaction checkpoints

The configured default path is `${DATA_DIR}/ack-store` in `broker/src/main/resources/application.yml`. A single configured block cache is shared across column families.

### ACK Key/Value

Source: `broker/src/main/java/com/messaging/broker/ack/RocksDbAckStore.java`.

- Key: topic, group, and a zero-padded 20-digit offset
- Value: 16 bytes containing offset and ACK timestamp
- Operations: single/batch put, get, prefix clear, range scan

### Compaction Data

Sources: `RocksDbCompactionIndex.java`, `CompactionCheckpointStore.java`.

The latest key state records the latest offset/timestamp per `topic + msgKey`; checkpoints record how far a topic's sealed segments were compacted.

## Properties State Files

All files live under `broker.storage.data-dir`. Atomic write behavior is implemented by `broker/src/main/java/com/messaging/broker/consumer/PropertiesFileStore.java`: write temporary file, force it, then rename.

| File | Owner | Key semantics |
|---|---|---|
| `consumer-offsets.properties` | `ConsumerOffsetTracker.java` | `group:topic -> offset`; local annotation consumers use their consumer ID |
| `delivery-state.properties` | `DeliveryStateStore.java` | `<group:topic>.offset` and `.until` |
| `data-refresh-state.properties` | `RefreshStateStore.java` | Active topic contexts, ACK sets, timestamps, replay state |
| `pipe-offset.properties` | `HttpPipeConnector.java` | `pipe.current.offset` |
| `topology.properties` | `TopologyPropertiesStore.java` | Last role and parent |

`FlushingPropertiesStore.java` adds periodic flushing. `ConsumerOffsetTracker` also rate-limits synchronous flushes to one per second.

## Consumer State Models

- `ConsumerKey(clientId, topic, group)`: socket-session registration.
- `DeliveryKey(group, topic)`: durable/logical delivery state.
- `RemoteConsumer`: current in-memory offset, legacy flag, failure/backoff state, scheduled task.
- `PendingDelivery`: immutable generation snapshot for ACK/timeout arbitration.
- `RefreshContext`: state, expected consumers, RESET/READY ACKs, replay offsets/times, downtime.

Sources: `broker/src/main/java/com/messaging/broker/model/` and `broker/src/main/java/com/messaging/broker/consumer/`.

## Offset Semantics

- Modern batch delivery sets pending and persisted offsets to `lastOriginalBatchOffset + 1`: `BatchDeliveryService.java`.
- Legacy merged delivery commits the highest acknowledged record offset per topic: `LegacyConsumerDeliveryManager.java`.
- Pipe state is the last successfully handled parent offset: `HttpPipeConnector.java`.
- Segment head is the last stored offset: `StorageEngine.getCurrentOffset`.

These meanings are not interchangeable. See [Offset risks](13-risk-and-edge-cases.md#offset-semantics).

## Data Ownership And Cleanup

- `DeliveryBatch` ownership transfers to `NettyTcpServer.sendBatch`, which closes it from `BatchPayloadFileRegion.deallocate`.
- Compaction closes/deletes replaced segments after swapping the segment map.
- `SharedRocksDb`, storage engines, property flushers, and executors have shutdown hooks.
- Test-created temp directories are managed by test harnesses; production data retention outside tombstone compaction is not implemented as a general time/size retention policy.

General message retention behavior is **not confirmed from code** beyond compaction of superseded/tombstone records.
