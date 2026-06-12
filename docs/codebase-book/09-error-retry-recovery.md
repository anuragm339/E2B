# Error, Retry, And Recovery

Related: [Overview](01-system-overview.md), [POS state](07-pos-machine-state.md), [Compaction](08-compaction-handling.md), [Debugging](12-debugging-guide.md), [Tests](10-test-map.md).

## Exception Model

Shared exception hierarchy:

- `MessagingException`
- `StorageException`
- `NetworkException`
- `ConsumerException`
- `DataRefreshException`
- `ErrorCode`

Sources: `common/src/main/java/com/messaging/common/exception/`.

Exceptions can carry context through `MessagingException.withContext`. `ExceptionLogger.java` formats shared logging.

## Startup Failures

- Storage recovery failure is fatal and wrapped in `RuntimeException`: `BrokerService.java`.
- TCP bind failure is fatal: `NettyTcpServer.java`, `BrokerService.java`.
- ACK seeding failure is non-fatal; reconciliation is expected to expose gaps.
- Registry absence/failure is non-fatal; topology manager can continue without a parent.

## Storage Recovery

`DefaultStorageRecoveryService.java` handles:

- current and legacy index versions;
- log bytes without index entries;
- index entries beyond log end;
- staging compaction files;
- discovery of topic/partition directories.

`FileChannelStorageEngine.recover` creates managers for discovered partitions. Storage corruption that cannot be reconciled prevents startup.

Tests: `SegmentCrashRecoverySpec.groovy`, `DefaultStorageRecoveryServiceSpec.groovy`, integration equivalents, restart journeys.

## Pipe Retry And Recovery

`HttpPipeConnector.java`:

- adaptive poll delay between configured min/max;
- retries network/HTTP/parser failures;
- persists only the last successfully handled offset;
- resumes from `pipe-offset.properties`;
- supports pause/resume during refresh;
- copies/releases streaming Netty buffers before parsing.

The connector treats partial successful processing transactionally: successfully handled records can advance in `finally`, while the failed record and later data are retried.

`TopologyManager.java` probes a new parent before switching and keeps the old parent when the probe fails.

## Delivery Retry

### Adaptive Scheduling

`AdaptiveBackoffPolicy.java` calculates delay between configured minimum and maximum. Success reduces delay; no data/failure increases it.

`TopicFairScheduler.java` permits configured concurrent work per topic and retains at most one pending retry per delivery key.

### Consumer Failure Backoff

`RemoteConsumer.java` records consecutive failures and backoff time. `BatchDeliveryService.java`:

- blocks while backoff remains;
- retries failed consumers;
- unregisters after ten consecutive failures;
- clears state immediately if send failed before timeout installation;
- leaves timeout-owned state intact if failure occurred after timeout installation.

### ACK Timeout

Modern:

- each delivery gets a generation;
- timeout claims only that generation;
- claimed timeout restores original offset and clears state.

Sources: `BatchDeliveryService.java`, `InMemoryInFlightDeliveryStore.java`.

Legacy:

- one pending merged batch per client;
- generation-specific timeout clears the slot and metrics.

Sources: `ConsumerRegistry.java`, `InMemoryPendingAckStore.java`.

## Readiness Retry

`ConsumerReadinessManager.java` retries READY every 5 seconds up to three times and cancels the retry when matching readiness is recorded.

Refresh uses separate retry timing in `RefreshCoordinator.java`: RESET every 5 seconds and READY checks every 10 seconds.

## Client Reconnect

`ClientConsumerManager.java`:

- detects disconnect through Netty close future;
- maintains per-topic/group reconnect guards;
- exponential reconnect delay `5s, 10s, 20s, 40s, 60s cap`;
- health-checks every 10 seconds;
- cleans one connection before recreating it;
- shares one event-loop group.

## ACK Recovery

### Seeder

`AckStoreSeeder.java` scans all committed offsets after storage recovery and fills missing per-record ACK entries in chunks. It distinguishes no committed offset from offset `0`.

### Reconciliation

`AckReconciliationScheduler.java` periodically scans registered topic/group ranges up to committed offsets:

- reports missing count;
- optionally backfills when `auto-sync-enabled=true`;
- keeps in-memory scan checkpoints;
- pauses and clears checkpoints for a topic during refresh;
- resumes with a full scan after refresh.

Only active registered pairs are reconciled. Historical disconnected groups are not confirmed to be scanned.

## Refresh Recovery

Detailed state behavior is in [POS state](07-pos-machine-state.md).

Recovery source: `RefreshRecoveryService.java`. It loads persisted contexts, pauses pipe first, records downtime, restores task scheduling, and resumes by state.

Abort watchdog behavior:

- 10-minute initial window;
- REPLAYING re-arms when progress is recent;
- READY_SENT receives one extra window;
- terminal states stop work.

## Compaction Recovery

See [Compaction recovery](08-compaction-handling.md#recovery-safety). The key mechanisms are staging files, atomic rename, checkpoints, and segment replacement locks.

## Property-File Recovery

`PropertiesFileStore.java` uses temp files, force, and move. `FlushingPropertiesStore.java`:

- retries/logs periodic flush failure;
- performs a final flush during stop;
- can propagate a final flush failure.

Consumer offsets intentionally allow a small replay window because synchronous writes are rate-limited.

## Error Handling By Interface

| Interface | Behavior |
|---|---|
| TCP producer/subscribe/commit validation | Log and close connection |
| Malformed ACK lengths | Log and close connection |
| Unknown legacy ACK group | Soft-fail without close |
| Delivery send/read failure | Backoff, rollback/timeout, eventual unregister |
| Pipe record storage failure | Return false; do not advance that offset |
| HTTP controller validation | Usually return error map with HTTP 200 |
| Storage recovery failure | Fail broker startup |
| ACK seeding/reconciliation failure | Degrade and log |

Sources: broker handlers/controllers and `BrokerService.java`.

## Recovery Risks

- A full journey run observed a pending modern ACK surviving beyond timeout after reconnect; exact rerun passed.
- Broker shutdown may invoke `NettyTcpServer.shutdown` twice through explicit broker shutdown and bean disposal; integration logs showed `RejectedExecutionException` during the second disposal.
- `ClientMessageHandler` keeps a channel alive for generic `DecoderException`, while `ZeroCopyBatchDecoder` explicitly closes on partial batch. Other decoder-state failures may not reset the stream.
- Final property flush failure can surface during shutdown after network/executors have already stopped.

See [Risks](13-risk-and-edge-cases.md) and [Debugging](12-debugging-guide.md).
