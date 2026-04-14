# Broker Test & Logic Gap TODO

Produced by full codebase review (2026-04-14). Items are grouped by type and
ordered roughly by risk/impact. Each item has: component, what's missing, why
it matters.

---

## SECTION 1 — Logic / Production Bugs

### BUG-1: `storage.read()` stops at offset gaps → RocksDB ack-store is incomplete for gapped batches

**File**: `storage/…/segment/Segment.java:readRecordAtOffset()` (line ~604)

**Problem**: `readRecordAtOffset(offset)` does a binary-search for the first
index entry ≥ `offset`, then returns `null` if `entry.offset != offset`.
`SegmentManager.readWithSizeLimit()` interprets `null` as "no more records in
this segment" and stops traversal. Result: a `storage.read(topic, 0, 1L, 1000)`
call for a segment with records at offsets 1, 100, 1000 returns only the record
at offset 1.

**Impact**:
- `BatchAckService.handleModernBatchAck()` uses `storage.read(fromOffset,
  maxRecords)` to find msgKeys for RocksDB writes. For gapped batches only the
  first record is written to the ack-store; the rest are silently dropped.
- `AckReconciliationScheduler` will report every gapped record as "missing" even
  though they were correctly delivered.

**Fix**: Change `readRecordAtOffset` to return the record at `entry.offset` when
`entry.offset > requested offset` (i.e. treat it as a gap-skip rather than an
error). Update `readWithSizeLimit` to advance `currentOffset = record.getOffset()
+ 1` (already done at line 249) and NOT stop when the returned record offset
doesn't equal the requested offset.

**Test needed**: Unit test in `SegmentManagerSpec` — read starting from an offset
that falls in a gap; assert all records up to maxRecords are returned.

---

### BUG-2: `PipeMessageForwarder.getCurrentOffset()` hardcoded to 0

**File**: `broker/…/core/PipeMessageForwarder.java:57-60`

**Problem**: Returns 0 unconditionally, regardless of how many records have been
forwarded. Any component using this method to decide how far a child broker is
behind will permanently think it is at offset 0.

**Fix**: Track the highest forwarded offset in an `AtomicLong` and return it.
Persist it alongside the pipe-poll offset in `pipe-offset.properties`.

**Test needed**: Integration test — forward N records, assert
`getCurrentOffset(topic)` equals the last forwarded record's offset.

---

### BUG-3: `AckReconciliationScheduler` auto-sync is a silent no-op stub

**File**: `broker/…/ack/AckReconciliationScheduler.java:136`

**Problem**: Setting `ack-store.reconciliation.auto-sync-enabled=true` logs a
debug message but does nothing. Operators who enable this expecting missing records
to be re-queued will be silently misled.

**Fix**: Either implement re-delivery (trigger `notifyNewMessage` for missing
offsets) or throw an `UnsupportedOperationException` / convert the config flag
to a logged warning at startup.

**Test needed**: Unit test — enable auto-sync, run `reconcile()` with missing
keys, assert re-delivery is triggered (or that the feature is clearly
communicated as unimplemented).

---

### BUG-4: Zombie delivery task leaks on modern-consumer disconnect

**File**: `broker/…/consumer/AdaptiveBatchDeliveryManager.java:106-108`

**Problem**: `removeConsumer(clientId)` only removes from `scheduledLegacyClients`.
For modern consumers the `ScheduledFuture` stored in `RemoteConsumer.deliveryTask`
is never cancelled. After disconnect, the zombie task continues looping in
`DeliveryScheduler.executeDelivery()` indefinitely.

After B crashes and new records arrive, the zombie reads a batch, sets a
`pendingOffset`, fails the send (channel gone), retains `pendingOffset` without
scheduling an ACK timeout, then loops permanently at Gate 2. This contaminates
the `group:topic` delivery state until the broker restarts.

**Fix**: In `removeConsumer`, cancel the `RemoteConsumer.deliveryTask` future:
```java
public void removeConsumer(String clientId) {
    scheduledLegacyClients.remove(clientId);
    // cancel zombie task for modern consumers
    for (RemoteConsumer c : consumerRegistry.getAllConsumers()) { // or via registrationService
        if (c.getClientId().equals(clientId)) {
            Future<?> task = c.getDeliveryTask();
            if (task != null) task.cancel(false);
        }
    }
}
```
Also call `stateService.removeDeliveryState(...)` for each consumer's key.

**Test needed**: Unit test — register modern consumer, disconnect, assert
`deliveryTask.isCancelled()` is true.

---

### BUG-5: `BatchDeliveryService` transient failure retains `pendingOffset` without scheduling an ACK timeout

**File**: `broker/…/consumer/BatchDeliveryService.java:318-370`

**Problem**: When `sendBatchToConsumer` throws (e.g. channel gone), the catch
block retains `pendingOffset` for transient failures but does NOT schedule an ACK
timeout. The pending offset will never be cleared by a timeout. The consumer is
permanently blocked at Gate 2 until the broker restarts or the consumer
accumulates 10 failures (which it cannot, because Gate 2 short-circuits before
the failure counter is incremented).

**Fix**: In the transient-failure catch path, schedule the ACK timeout so the
offset is cleared after `ackTimeoutMs`:
```java
} else {
    // Schedule cleanup timeout even for transient failures where send failed
    // to prevent permanent pendingOffset contamination
    scheduler.schedule(() -> {
        if (stateService.removePendingOffset(deliveryKey) != null) {
            consumer.setCurrentOffset(startOffset);
            inFlight.set(false);
        }
    }, ackTimeoutMs, TimeUnit.MILLISECONDS);
}
```

**Test needed**: Unit test — mock send to throw immediately, assert that after
`ackTimeoutMs` the `pendingOffset` is cleared and the consumer's offset is
reverted.

---

## SECTION 2 — Missing Unit Tests

### UNIT-1: `BatchDeliveryService` MAX_CONSECUTIVE_FAILURES circuit breaker

**File**: `broker/…/consumer/BatchDeliveryService.java:128-133`

After 10 consecutive failures the consumer should be unregistered and delivery
permanently stopped. No test exercises this path.

**Test**: Fail `sendBatchToConsumer` 10 times; assert `registrationService.
unregisterConsumer()` is called and `DeliveryResult.failure("max-failures-exceeded")`
is returned.

---

### UNIT-2: `ConsumerReadinessManager` — retry exhaustion (MAX_READY_RETRIES exceeded)

**File**: `broker/…/consumer/ConsumerReadinessManager.java`

Schedules up to 3 retries to resend READY at 5 s intervals. No test exercises
what happens when all 3 fail (consumer never ACKs READY).

**Test**: Mock `NetworkServer.send()` to always fail; assert no further retries
are scheduled after `MAX_READY_RETRIES` and that an appropriate log/metric is
emitted.

---

### UNIT-3: `ConsumerRegistrationManager.validateAndCorrectOffset()` correction branches

**File**: `broker/…/consumer/ConsumerRegistrationManager.java`

Three branches: (a) offset within valid range → unchanged; (b) offset below
`earliestOffset` (compaction removed older segments) → corrected to earliest; (c)
storage throws during validation → offset reset to 0.

Only branch (a) is covered. Branches (b) and (c) allow silent duplicate delivery
or infinite-empty-batch polling on restart.

**Test**: Three separate `when/then` blocks with mocked `StorageEngine` returning
appropriate values for each scenario.

---

### UNIT-4: `FlushingPropertiesStore` flush failure

**File**: `broker/…/consumer/FlushingPropertiesStore.java` (or equivalent
`ConsumerOffsetTracker` persistence path)

The atomic temp-file write + rename can fail under disk-full or read-only
filesystem. No test covers this path.

**Test**: Write to a read-only directory; assert the exception is logged, a metric
is incremented, and the existing properties file is not corrupted.

---

### UNIT-5: `DeliveryScheduler` gate chain — short-circuit on first denied gate

**File**: `broker/…/consumer/DeliveryScheduler.java:84-96`

No test verifies that a list of three gate policies [allow, deny, allow] results
in delivery being blocked with only the first two gates evaluated.

**Test**: Three mock `DeliveryGatePolicy` instances; assert `shouldDeliver()` is
called on the first two and NOT on the third.

---

### UNIT-6: `RefreshCoordinator.abortRefreshIfStuck()` — 10-minute abort path

**File**: `broker/…/consumer/RefreshCoordinator.java:140-162`

The 10-minute abort watchdog fires via a `ScheduledExecutorService`. The abort
path itself (`abortRefreshIfStuck`) is never directly tested.

**Test**: Call `abortRefreshIfStuck` directly (package-private or via reflection);
assert `context.setState(ABORTED)`, pipe is resumed, and metrics are updated.

---

### UNIT-7: `RefreshCoordinator.startRefresh()` — concurrent call rejection

**File**: `broker/…/consumer/RefreshCoordinator.java`

Calling `startRefresh("prices-v1")` while a refresh is already in progress for
that topic should return an appropriate error/no-op. This path is not tested.

**Test**: Start a refresh that pauses in RESETTING state; call `startRefresh`
again; assert it returns a completed-with-failure future or the second call is
a no-op.

---

### UNIT-8: `TopicFairScheduler` with `maxInFlightPerTopic > 1`

**File**: `broker/…/consumer/TopicFairScheduler.java`

All existing tests and production configuration use the default
`maxInFlightPerTopic = 1`. Higher values are config-supported but untested.

**Test**: Set `maxInFlightPerTopic = 2`; assert two tasks for the same topic can
hold the semaphore simultaneously while a third is deferred.

---

## SECTION 3 — Missing Integration Tests

### INT-1: `AckReconciliationScheduler` with genuinely missing ack records

**File**: `broker/src/integrationTest/…/ack/AckStoreIntegrationSpec.groovy`

The existing `AckStoreIntegrationSpec` tests RocksDB CRUD. The
`AckReconciliationScheduler` integration test (`AckStoreIntegrationSpec`) runs
`reconcile()` but primarily checks it doesn't throw. There is no test that:
- Stores N records in storage
- ACKs only M < N of them in RocksDB
- Runs `reconcile()`
- Asserts the missing-key gauge is N − M

---

### INT-2: `RefreshGatePolicy` + `WatermarkGatePolicy` combined in `DeliveryScheduler`

Both gate policies are individually tested (`WatermarkGatePolicyIntegrationSpec`,
`RefreshStateMachineIntegrationSpec`) but their interaction in the scheduler chain
is not: a refresh state should block delivery even when new data is available at
the watermark.

**Test**: Start a refresh (gate = DENY), enqueue records (watermark gate = ALLOW);
assert `deliverBatch` is NOT called. Complete the refresh; assert delivery resumes.

---

## SECTION 4 — Missing System Journey Tests

### JOURNEY-1: Pipe outage — broker buffers, consumer receives on resume

`MockCloudServer` already has `pausePipe()` / `resumePipe()` methods but they are
not used in any existing spec.

**Scenario**:
1. Deliver initial records to consumer.
2. `cloudServer.pausePipe()` → broker polls get 204 for N seconds.
3. Enqueue new records into MockCloudServer while paused.
4. `cloudServer.resumePipe()`.
5. Assert broker polls and delivers the buffered records.
6. Assert no records are duplicated or skipped.

---

### JOURNEY-2: Broker restart — pipe offset and consumer offsets survive

No system test verifies end-to-end state persistence across a full broker restart.

**Scenario**:
1. Deliver 5 records; verify committed offsets persisted to disk.
2. Close `brokerCtx` (broker restart simulation).
3. Recreate `brokerCtx` with the SAME `dataDir`.
4. Reconnect consumer.
5. Enqueue 3 more records.
6. Assert consumer receives only the 3 new records (not the first 5).
7. Assert pipe connector resumes from its persisted offset (not 0).

---

### JOURNEY-3: Legacy consumer full journey

`BrokerLegacyConsumerIntegrationSpec` covers legacy at integration level only.
No system-level journey test exercises the full legacy protocol E2E.

**Scenario**:
1. Connect a real legacy consumer (using `LegacyConsumerClient` helper).
2. Enqueue records for multiple topics.
3. Assert merged-batch delivery arrives.
4. Send BATCH_ACK.
5. Assert per-topic offsets advance in `ConsumerOffsetTracker`.
6. Assert RocksDB ack-store has all delivered msgKeys.

---

### JOURNEY-4: Multi-topic single consumer

`MultiConsumerJourneySpec` covers two consumers on different topics. No test
covers a single consumer subscribing to two topics simultaneously.

**Scenario**:
1. Start one consumer with `consumer.topics = prices-v1,ref-data-v5`.
2. Enqueue records for both topics.
3. Assert consumer receives records for both topics on the correct `topic` field.
4. Assert per-topic committed offsets tracked independently.
5. Assert no cross-topic contamination in RocksDB ack-store.

---

### JOURNEY-5: Data refresh with two consumer groups

`DataRefreshJourneySpec` only tests one consumer group. The refresh
RESET → RESET_ACK → REPLAY → READY_ACK cycle requires ALL registered consumers
to ACK before replay starts.

**Scenario**:
1. Start two consumers: `group-a` and `group-b`, both on `prices-v1`.
2. Trigger refresh.
3. Both receive RESET; both send RESET_ACK.
4. Broker enters REPLAYING only after BOTH groups ACK.
5. Both receive replayed records.
6. Both send READY_ACK; broker reaches COMPLETED.
7. Assert both groups' offsets reset and advanced correctly.

---

### JOURNEY-6: Pipe HTTP error recovery

No test exercises the pipe connector's behaviour when `MockCloudServer` returns
5xx errors.

**Scenario**:
1. Have MockCloudServer return 503 for 3 polls.
2. Assert broker retries (poll count advances, no crash).
3. Resume normal responses.
4. Assert records eventually delivered.

---

### JOURNEY-7: Segment rollover during delivery

All existing journey tests use small record sets that fit in one segment. A record
batch that spans a segment boundary is not system-tested.

**Scenario**:
1. Configure a very small segment size (e.g. 4 KB).
2. Enqueue enough records to force a segment rollover mid-batch.
3. Assert consumer receives all records across the boundary.
4. Assert no records are duplicated or skipped at the rollover point.
5. Assert RocksDB ack-store has all msgKeys.

---

## Summary

| ID | Type | Risk | Component |
|---|---|---|---|
| BUG-1 | Logic bug | High | `storage.read()` gap traversal → incomplete RocksDB writes |
| BUG-2 | Logic bug | Medium | `PipeMessageForwarder.getCurrentOffset()` → always 0 |
| BUG-3 | Logic bug | Medium | `AckReconciliationScheduler` auto-sync stub → silent no-op |
| BUG-4 | Logic bug | Medium | Zombie delivery task not cancelled on disconnect |
| BUG-5 | Logic bug | Medium | `pendingOffset` retained without ACK timeout on immediate send failure |
| UNIT-1 | Unit test | High | Circuit breaker (10 failures → unregister) |
| UNIT-2 | Unit test | Medium | READY retry exhaustion |
| UNIT-3 | Unit test | High | Offset validation correction branches |
| UNIT-4 | Unit test | Medium | Flush failure under disk-full |
| UNIT-5 | Unit test | Low | Gate chain short-circuit ordering |
| UNIT-6 | Unit test | Low | Refresh abort watchdog direct test |
| UNIT-7 | Unit test | Medium | Concurrent `startRefresh` rejection |
| UNIT-8 | Unit test | Low | `TopicFairScheduler` with `maxInFlightPerTopic > 1` |
| INT-1 | Integration | Medium | Reconciliation missing-key gauge |
| INT-2 | Integration | Medium | Gate policy combination (refresh + watermark) |
| JOURNEY-1 | System | High | Pipe outage buffering and resume |
| JOURNEY-2 | System | High | Broker restart — persistent offset recovery |
| JOURNEY-3 | System | Medium | Legacy consumer full E2E journey |
| JOURNEY-4 | System | Medium | Multi-topic single consumer |
| JOURNEY-5 | System | Medium | Data refresh with two consumer groups |
| JOURNEY-6 | System | Low | Pipe HTTP error recovery |
| JOURNEY-7 | System | Medium | Segment rollover during delivery |
