# POS Machine State

Related: [Features](03-feature-catalog.md#pos-data-refresh), [Events](06-event-kafka-flow.md#refresh-event-flow), [Data model](05-data-model.md#consumer-state-models), [Recovery](09-error-retry-recovery.md), [Tests](10-test-map.md).

## Scope

There is no class literally named `PosMachineState`. POS state is represented by:

- TCP connection and registration state;
- startup READY/ACK state;
- per-group/topic delivery and committed offset;
- refresh RESET/replay/READY state;
- reconnect behavior in the client library.

Sources: `ConsumerRegistry.java`, `ConsumerReadinessManager.java`, `RefreshCoordinator.java`, and `ClientConsumerManager.java`.

## Startup Readiness

Modern:

1. Client creates one connection per `topic:group`.
2. Client sends `SUBSCRIBE`.
3. Broker restores `group:topic` offset.
4. Broker sends `READY(topic)`.
5. Client invokes every handler's `onReady(topic)`.
6. Client sends structured `READY_ACK(topic,group)`.
7. Broker opens the readiness gate.

Legacy:

1. Client sends `REGISTER(serviceName)`.
2. Broker maps service name to topics.
3. Broker sends empty-payload READY.
4. Generic ACK is resolved to READY_ACK by `LegacyConnectionState`.

Sources: `SubscribeHandler.java`, `ConsumerReadinessManager.java`, `ClientConsumerManager.java`, `LegacyConnectionState.java`.

READY retries occur after 5 seconds, up to three retries. Source: `ConsumerReadinessManager.java`.

## Refresh State Machine

States are defined in `broker/src/main/java/com/messaging/broker/consumer/RefreshState.java`.

```text
RESET_SENT -> REPLAYING -> READY_SENT -> COMPLETED
      \           \            \
       +-----------+-------------> ABORTED
```

Transition validation: `RefreshStateMachine.java`.

### Initiation

`RefreshInitiator.startRefresh`:

- force-cancels any existing refresh for the same topic;
- snapshots currently registered `group:topic` consumers;
- returns `COMPLETED` immediately if none exist;
- creates a UUID refresh ID shared by concurrent topics in the same batch;
- leaves pipe polling active because local consumer refresh does not mutate `pipe-offset.properties` or topic folders;
- resolves `broker.refresh.replay.window-hours` into a concrete replay start offset and captured target head;
- persists `RESET_SENT`.

Sources: `RefreshInitiator.java`, `RefreshController.java`.

### RESET Phase

`RefreshResetService.sendReset`:

- pauses ACK reconciliation for the topic;
- clears ACK RocksDB entries for expected groups;
- broadcasts RESET;
- records metrics.

On RESET_ACK:

- validate expected consumer and duplicate;
- reset that consumer's offset to the captured replay start offset;
- for legacy consumers, reset to `replayStartOffset - 1` because legacy offsets mean last-delivered;
- initialize replay metrics;
- persist context;
- the first accepted ACK atomically claims transition to `REPLAYING`.

Sources: `RefreshResetService.java`, `ResetAckHandler.java`, `RefreshCoordinator.java`.

The coordinator retries RESET every 5 seconds while missing ACKs.

### Replay Phase

Normal adaptive delivery resumes because `RefreshGatePolicy` blocks only `RESET_SENT`. `RefreshReplayService` checks committed offsets for consumers that ACKed RESET and compares them with storage head.

Replay completion requires:

- every expected RESET ACK received; and
- every ACKed consumer considered caught up to the captured replay target. The target is the **settled-history** offset: the last record whose `created_time` is older than `broker.refresh.ready-settle-window-ms` (default 6h). Records created within that window are still settling and are not required for READY (they arrive via normal delivery after). If no record is within the window the target is the head (deliver everything); if every record is within the window the target is `-1` (READY immediately). `broker.refresh.replay.window-hours` separately bounds how far back replay *starts*.

Source: `RefreshReplayService.java`.

### READY Phase

`RefreshReadyService.sendReady`:

- snapshots RESET ACKs;
- publishes `READY_SENT`;
- sends READY to the snapshot;
- sends a supplemental READY to consumers that ACKed in the snapshot/state visibility window;
- persists state.

READY ACK timeout checks run every 10 seconds and re-send to the live ACK set. All READY ACKs atomically claim completion.

On completion:

- persist `COMPLETED`;
- clear the topic's refresh state;
- resume ACK reconciliation;
- retain in-memory context for 60 seconds before guarded cleanup.

`/health` normally reports DOWN while any topic refresh is active. If `broker.refresh.health-critical-topics` is configured, only active refreshes for those topics block health; non-critical topics can continue replaying while the broker reports green.

Sources: `RefreshReadyService.java`, `RefreshCoordinator.java`.

## Late Joiners And Reconnects

`SubscribeHandler` examines active refresh state:

- `RESET_SENT` or `REPLAYING`: register the group as a late RESET ACK before opening delivery.
- `READY_SENT`: add it to the live READY audience and send refresh READY directly.
- terminal/cleaned state: use startup READY.

Race guards and atomic completion markers are in `RefreshCoordinator.registerLateJoiningConsumer` and `RefreshContext`.

Client reconnect uses exponential delay from 5 seconds to 60 seconds and one connection per `topic:group`: `client/src/main/java/com/messaging/client/ClientConsumerManager.java`.

## Disconnect State Cleanup

`ConsumerRegistry.unregisterConsumer`:

- cancels each scheduled delivery task;
- removes `DeliveryKey` in-flight/pending state;
- completes pending metrics;
- removes readiness;
- clears pending ACKs;
- removes registration and eligible metrics.

Sources: `ConsumerRegistry.java`, `ClientDisconnectHandler.java`.

Durable `group:topic` offsets are retained, so reconnect resumes from the last persisted position.

## Persisted Refresh State

File: `<dataDir>/data-refresh-state.properties`.

Owner: `broker/src/main/java/com/messaging/broker/consumer/RefreshStateStore.java`.

Persisted content includes active topics, refresh ID/type/state, expected consumers, ACK sets, timestamps, replay progress, replay-window offsets/cutoff, and downtime. The store includes backward-compatible loading for an older single-active-refresh format.

At broker startup, `RefreshRecoveryService`:

- loads all contexts;
- leaves pipe polling active while resuming consumer-refresh state;
- fills missing old refresh IDs with UUIDs;
- groups topics by refresh ID;
- records startup/downtime;
- resumes RESET, replay, READY, or completed handling;
- re-arms watchdogs and timeout tasks.

## Timeouts And Abort

Constants in `RefreshCoordinator.java`:

- replay check: 1 second;
- RESET retry: 5 seconds;
- READY ACK timeout: 10 seconds;
- abort watchdog: 10 minutes.

REPLAYING watchdog re-arms if replay recently progressed. READY_SENT gets one additional full watchdog window before abort. ABORTED cleanup removes active maps and tasks, but automatic pipe/reconciliation recovery after every abort scenario is not fully confirmed; see [Open questions](15-open-questions.md).

## POS Safety Invariants

1. Delivery must not occur before startup READY_ACK.
2. Delivery is blocked while refresh is waiting for RESET ACK.
3. Local consumer refresh must not own pipe pause/resume; only destructive download-refresh bootstrap sections may pause pipe polling.
4. A decoded partial batch must not be ACKed.
5. ACK and timeout may complete only the matching delivery generation.
6. Reconnect must preserve durable group/topic offset and remove stale socket state.
7. Compaction must preserve delivery offset progress across gaps.

Source implementations: `BatchDeliveryService.java`, `InMemoryInFlightDeliveryStore.java`, `ZeroCopyBatchDecoder.java`, `RefreshReadyService.java`, `SegmentManager.java`.

## Confirmed Test Signal

The full journey run had one failure in `ConsumerCrashDuringReplayJourneySpec`: after group B reconnected, its pending ACK remained for more than 25 seconds despite a 5-second timeout and READY was never sent. The exact spec passed on an immediate clean rerun. Treat this as a flaky concurrency signal, not a dismissed failure. See [Test map](10-test-map.md#observed-failures) and [Risks](13-risk-and-edge-cases.md#pos-refresh-and-reconnect).
