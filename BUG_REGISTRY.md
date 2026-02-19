# Bug Registry — Messaging Broker

## How to Use This File
- Every bug has a **Status** field: `OPEN` | `IN_PROGRESS` | `FIXED`
- When fixing a bug: update Status → `FIXED`, add the git commit hash, and fill in the verification steps
- Each fix gets its own commit so changes can be traced individually
- Cross-check column: before marking FIXED, run the listed verification command and paste the output

---

## Already Fixed (Pre-Registry)

| ID | Description | Commit | Status |
|----|-------------|--------|--------|
| Phase-5A | Keep pending offset on transient failures — prevents rapid retry spam | 54b62cd | ✅ FIXED |
| Phase-5B | Timeout unit bug `MINUTES → SECONDS` in `sendBatchToConsumer()` | 54b62cd | ✅ FIXED |

---

## CRITICAL Bugs (System stops / data permanently lost)

---

### B1-1 — Adaptive delivery loop dies permanently on semaphore miss

**Status:** `FIXED`
**Commit:** `84a393a`
**Severity:** Critical
**Batch:** 1

**File:**
- `broker/src/main/java/com/messaging/broker/consumer/AdaptiveBatchDeliveryManager.java:116-136`
- `broker/src/main/java/com/messaging/broker/consumer/TopicFairScheduler.java:58-72`

**Bug:**
Reschedule call `scheduleAdaptiveDelivery(consumer, nextDelay)` at line 134 is inside the task lambda.
`TopicFairScheduler.schedule()` wraps that lambda in a `tryAcquire()` check — if semaphore is exhausted,
the entire task body is skipped including the reschedule. The delivery loop for that consumer permanently dies.
No error is logged. Consumer stops receiving data silently.

**Root Cause Code:**
```java
// TopicFairScheduler.java:68
if (semaphore.tryAcquire()) {
    try { task.run(); } finally { semaphore.release(); }
} else {
    log.trace("Skipping task for topic={}", topic);  // reschedule never happens
}

// AdaptiveBatchDeliveryManager.java:134 — inside task.run() so skipped:
scheduleAdaptiveDelivery(consumer, nextDelay);
```

**Fix:**
Move reschedule outside the semaphore-guarded block in `TopicFairScheduler`, OR always reschedule
from `scheduleAdaptiveDelivery()` regardless of task execution outcome.

**Verification:**
```bash
# After fix: check that consumers recover after semaphore contention
docker logs messaging-broker 2>&1 | grep "Skipping task" | wc -l
# Before fix: after N skips the consumer stops polling entirely
# After fix: skips are followed by resumed delivery
```

---

### B3-3 — `allConsumersCaughtUp()` ignores clientId, uses substring match

**Status:** `FIXED`
**Commit:** `921fa05`
**Severity:** Critical
**Batch:** 3

**File:**
- `broker/src/main/java/com/messaging/broker/consumer/RemoteConsumerRegistry.java:592-620`

**Bug:**
The loop iterates over `consumerClientIds` but ignores each `clientId` in the filter.
Uses `filter(e -> e.getKey().contains(topic))` + `findFirst()` — always checks the same
first consumer found by substring match, regardless of which clientId is being checked.
READY is sent while other consumers are still replaying.

**Root Cause Code:**
```java
for (String clientId : consumerClientIds) {
    String consumerKey = clientId + ":" + topic;  // built but NEVER used in filter
    RemoteConsumer consumer = consumers.entrySet().stream()
            .filter(e -> e.getKey().contains(topic))  // substring, ignores clientId
            .findFirst()                              // always same consumer
            .orElse(null);
```

**Fix:**
Use exact key lookup: `consumers.get(clientId + ":" + topic)` instead of stream filter.

**Verification:**
```bash
# With 2 consumer groups on same topic, verify both must catch up before READY
docker logs messaging-broker 2>&1 | grep "All consumers caught up"
# Should only appear after BOTH groups have committed to latest offset
```

---

### B3-5 — `resumeRefresh(COMPLETED)` deletes entire state file

**Status:** `FIXED`
**Commit:** `9bd9993`
**Severity:** Critical
**Batch:** 3

**File:**
- `broker/src/main/java/com/messaging/broker/refresh/DataRefreshManager.java:672-677`

**Bug:**
`case COMPLETED:` calls `stateStore.clearState()` (no-arg) which calls `Files.deleteIfExists(stateFilePath)`
— deletes the entire `data-refresh-state.properties` file. If topic A was COMPLETED but topic B was
REPLAYING at crash time, restarting wipes topic B's state. Pipes stay paused forever.

**Root Cause Code:**
```java
case COMPLETED:
    activeRefreshes.remove(topic);
    stateStore.clearState();  // ← no-arg: deletes ENTIRE file, not just this topic
    break;
```

**Fix:**
Change to `stateStore.clearState(topic)` — 2 extra characters, fixes the bug.

**Verification:**
```bash
# Simulate: start 2-topic refresh, crash after first completes, restart
# After fix: second topic's state survives restart
cat /data/data-refresh-state.properties | grep "topic\."
# Should still show second topic's entries after restart
```

---

### B5-1 — READY broadcast to non-reset consumers

**Status:** `FIXED`
**Commit:** `3830980`, `a7fe869`
**Severity:** Critical
**Batch:** 5

**File:**
- `broker/src/main/java/com/messaging/broker/refresh/DataRefreshManager.java:379-381`
- `broker/src/main/java/com/messaging/broker/consumer/RemoteConsumerRegistry.java` (`broadcastReadyToTopic`)

**Bug:**
`checkReplayProgress()` checks only `ackedConsumers` (those who ACKed RESET) for catch-up.
But `sendReady()` calls `broadcastReadyToTopic(topic)` which sends READY to ALL consumers
registered on that topic — including those that never sent RESET ACK. Non-reset consumers
receive READY on stale data and complete refresh incorrectly.

**Root Cause Code:**
```java
boolean allCaughtUp = remoteConsumers.allConsumersCaughtUp(topic, ackedConsumers);
if (allCaughtUp) {
    sendReady(topic, context);  // broadcasts to ALL, not just ackedConsumers
}
// sendReady → broadcastReadyToTopic → getConsumersForTopic (all registered consumers)
```

**Fix:**
`sendReady()` should only send READY to consumers in `context.getReceivedResetAcks()`,
not to all consumers on the topic. Also `checkReplayProgress()` should not declare allCaughtUp
unless `ackedConsumers.equals(expectedConsumers)` — i.e. all expected consumers have reset.

**Verification:**
```bash
# With 2 groups on same topic, disconnect one before RESET
docker logs messaging-broker 2>&1 | grep "READY sent"
# After fix: READY only sent after both groups ACK RESET
```

---

## HIGH Bugs (Wrong behavior / correctness broken)

---

### B1-2 — `deliveryTask` field never stored, cancel() is a no-op

**Status:** `FIXED`
**Commit:** `d843a41`
**Severity:** High
**Batch:** 1

**File:**
- `broker/src/main/java/com/messaging/broker/consumer/RemoteConsumerRegistry.java`

**Bug:**
The `ScheduledFuture` returned by the scheduler is never assigned back to `consumer.deliveryTask`.
On `unregisterConsumer()`, `consumer.deliveryTask.cancel(false)` either NPEs or cancels nothing.
Old scheduler task keeps running against a removed consumer entry.

**Fix:**
Assign `consumer.deliveryTask = scheduledFuture` after scheduling the delivery task.

**Verification:**
```bash
# After consumer disconnect, verify no more delivery attempts for that clientId
docker logs messaging-broker 2>&1 | grep "deliverBatch" | grep "<disconnected-clientId>"
# After fix: zero entries after disconnect
```

---

### B1-3 — ClassCastException reading sealed segments

**Status:** `FIXED`
**Commit:** `442e4f4`
**Severity:** High
**Batch:** 1

**File:**
- `storage/src/main/java/com/messaging/storage/mmap/MMapStorageEngine.java`
- `storage/src/main/java/com/messaging/storage/filechannel/FileChannelStorageEngine.java`

**Bug:**
Storage engine returns a type incompatible with what the delivery layer expects when reading
sealed segments. `ClassCastException` thrown, delivery fails for all sealed segment reads.

**Verification:**
```bash
docker logs messaging-broker 2>&1 | grep "ClassCastException"
```

---

### B1-6 — `pendingOffsets` not cleared on unregister — re-registration stalls 30 min

**Status:** `FIXED`
**Commit:** `37c0d31`
**Severity:** High
**Batch:** 1

**File:**
- `broker/src/main/java/com/messaging/broker/consumer/RemoteConsumerRegistry.java:143-165`

**Bug:**
`unregisterConsumer()` removes from `consumers` map but does NOT clear `pendingOffsets` entries.
When the same consumer reconnects and re-registers with the same `clientId:topic` key, Gate 2
finds the stale pending offset and blocks all delivery for 30 minutes until ACK timeout fires.

**Root Cause Code:**
```java
public int unregisterConsumer(String clientId) {
    // removes from consumers map
    consumers.remove(key);
    // pendingOffsets NOT cleared — stale entry blocks Gate 2 on re-registration
}
```

**Fix:**
In `unregisterConsumer()`, also remove all `pendingKey` entries for the client:
```java
String pendingKey = key + ":pending";
pendingOffsets.remove(pendingKey);
```

**Verification:**
```bash
# Restart a consumer, verify delivery resumes immediately (not after 30 min)
docker logs messaging-broker 2>&1 | grep "Gate 2 BLOCKED" | grep "<clientId>"
# After fix: zero Gate 2 blocks after re-registration
```

---

### B2-6 — ACK after unregister drops offset commit — message duplication

**Status:** `FIXED`
**Commit:** `719e64a`
**Severity:** High
**Batch:** 2

**File:**
- `broker/src/main/java/com/messaging/broker/consumer/RemoteConsumerRegistry.java:478-489`

**Bug:**
Race: consumer permanently fails and is unregistered. ACK arrives just after.
`consumers.get(deliveryKey)` returns null → `offsetTracker.updateOffset()` skipped.
Consumer reconnects at last persisted offset and replays already-delivered messages.

**Root Cause Code:**
```java
RemoteConsumer consumer = consumers.get(deliveryKey);
if (consumer != null) {
    offsetTracker.updateOffset(...);  // skipped if consumer was unregistered
}
```

**Fix:**
Persist offset using the `committedOffset` from `pendingOffsets` even when consumer is null.
The group:topic key needed for `offsetTracker` can be derived from the `pendingKey`.

**Verification:**
```bash
# After fix: no offset regression when consumer reconnects after permanent failure
cat /data/consumer-offsets.properties
# Offset should not go backwards
```

---

### B3-1 + B3-2 — `config.getExpectedConsumers()` ignored + `getConsumerGroupTopic()` wrong return value

**Status:** `FIXED`
**Commit:** `921fa05` (B3-2), `8f5c741` (B3-1)
**Severity:** High
**Batch:** 3
**Note:** Fixed together — B3-2 fixed first, B3-1 required subsequent fix after dependency was identified.

**File:**
- `broker/src/main/java/com/messaging/broker/refresh/DataRefreshManager.java:162`
- `broker/src/main/java/com/messaging/broker/consumer/RemoteConsumerRegistry.java:638`

**Bug B3-1:**
`startRefresh()` always hardcoded `expectedConsumers = Set.of(topic)` (bare topic name). After B3-2
fixed `getConsumerGroupTopic()` to return `"group:topic"`, `handleResetAck()` compared
`Set{"prices-v1"}.contains("price-quote-group:prices-v1")` → false, rejecting every RESET ACK.

**Bug B3-2:**
`getConsumerGroupTopic()` returned `consumer.topic` (just topic string) instead of `group + ":" + topic`.

**Fix:**
- B3-2: Return `consumer.group + ":" + consumer.topic` from `getConsumerGroupTopic()`
- B3-1: Add `getGroupTopicIdentifiers(topic)` to `RemoteConsumerRegistry` (iterates live consumers,
  returns `Set<"group:topic">`). `startRefresh()` uses this instead of `Set.of(topic)`.
  Skips refresh early if no consumers are registered (prevents hang).

**Verification:**
```bash
# After fix: should show all registered group:topic pairs, not just bare topic name
docker logs messaging-broker 2>&1 | grep "Starting refresh for topic"
```

---

### B4-3 — Late ACK after timeout leaves `inFlight` stuck true — delivery permanently stalls

**Status:** `FIXED`
**Commit:** `bb57609`
**Severity:** High
**Batch:** 4

**File:**
- `broker/src/main/java/com/messaging/broker/consumer/RemoteConsumerRegistry.java:472-494`

**Bug:**
Consumer goes offline for 30 min → ACK timeout fires → removes `pendingOffsets` entry, reverts offset.
Consumer comes back, sends late BATCH_ACK → `pendingOffsets.remove(pendingKey)` returns null → early
return at line 475 → `inFlight.set(false)` at line 492-494 is NEVER reached.
Gate 1 (`inFlight`) permanently true → all future delivery blocked for that consumer. Silent stall.

**Root Cause Code:**
```java
Long committedOffset = pendingOffsets.remove(pendingKey);
if (committedOffset == null) {
    log.warn("ACK with no pending offset: {}", deliveryKey);
    return;  // ← inFlight never cleared here
}
// ...
AtomicBoolean inFlight = inFlightDeliveries.get(deliveryKey);
if (inFlight != null) {
    inFlight.set(false);  // ← never reached on late ACK
}
```

**Fix:**
Always clear `inFlight` regardless of whether `committedOffset` was found:
```java
if (committedOffset == null) {
    log.warn("ACK with no pending offset: {}", deliveryKey);
    // still clear inFlight to unblock delivery
    AtomicBoolean inFlight = inFlightDeliveries.get(deliveryKey);
    if (inFlight != null) inFlight.set(false);
    return;
}
```

**Verification:**
```bash
# After 30-min timeout recovery, verify delivery resumes
docker logs messaging-broker 2>&1 | grep "Gate 1 BLOCKED\|inFlight"
# After fix: no permanent Gate 1 blocks after late ACK
```

---

## MEDIUM Bugs (Metrics wrong / behavioral edge cases)

---

### B2-1 — Consumer lag metric always 0

**Status:** `FIXED`
**Commit:** `2d6a510`
**Severity:** Medium
**Batch:** 2

**File:**
- `broker/src/main/java/com/messaging/broker/consumer/RemoteConsumerRegistry.java:121`

**Bug:**
`updateConsumerLag()` called only once on `registerConsumer()` with hardcoded `0`.
Never updated during delivery. `broker_consumer_lag` gauge always 0. Lag panel useless.

**Fix:**
After each successful `deliverBatch()`, calculate and update lag:
```java
long lag = latestOffset - consumer.getCurrentOffset();
metrics.updateConsumerLag(consumer.clientId, consumer.topic, consumer.group, lag);
```

**Verification:**
```bash
curl -s http://localhost:8081/prometheus | grep "broker_consumer_lag"
# After fix: non-zero values when consumers are behind
```

---

### B2-2 — `recordConsumerFailure()` / `recordConsumerRetry()` never called

**Status:** `FIXED`
**Commit:** `2d6a510`
**Severity:** Medium
**Batch:** 2

**File:**
- `broker/src/main/java/com/messaging/broker/metrics/BrokerMetrics.java:364,379`
- `broker/src/main/java/com/messaging/broker/consumer/RemoteConsumerRegistry.java` (catch block)

**Bug:**
Both methods are defined and Prometheus counters registered but zero call sites in delivery path.
`broker_consumer_failures_total` and `broker_consumer_retries_total` always 0.

**Fix:**
Call `metrics.recordConsumerFailure()` in the `deliverBatch()` catch block on each failure.

**Verification:**
```bash
curl -s http://localhost:8081/prometheus | grep "broker_consumer_failures_total"
# After fix: non-zero when failures occur
```

---

### B2-4 — ACK timeout scheduled before send — false-positive timeouts

**Status:** `FIXED`
**Commit:** `9b5a54f`
**Severity:** Medium
**Batch:** 2

**File:**
- `broker/src/main/java/com/messaging/broker/consumer/RemoteConsumerRegistry.java:280-296`

**Bug:**
ACK timeout scheduled at line 280, send happens at line 296. If send fails (Phase 5A keeps
pending offset), the 30-min timeout fires and records false `recordAckTimeout()`, reverts offset
for a batch the consumer never received.

**Fix:**
Move timeout scheduling to after `sendBatchToConsumer()` succeeds, inside a try block.
Cancel the timeout in the catch block.

**Verification:**
```bash
curl -s http://localhost:8081/prometheus | grep "broker_consumer_ack_timeouts_total"
# After fix: count drops — only real timeouts (consumer received but didn't ACK)
```

---

### B2-5 — `DataRefreshMetrics` non-atomic `value += delta`

**Status:** `FIXED`
**Commit:** `03261a1`
**Severity:** Medium
**Batch:** 2 & 3

**File:**
- `broker/src/main/java/com/messaging/broker/metrics/DataRefreshMetrics.java:499-518`

**Bug:**
Custom `AtomicLong`/`AtomicDouble` inner classes use `volatile value += delta` — not thread-safe.
Concurrent `recordDataTransferred()` calls from multiple delivery threads lose increments.
Byte/message counts undercounted.

**Root Cause Code:**
```java
private static class AtomicLong {
    private volatile long value;
    long addAndGet(long delta) {
        this.value += delta;  // NOT atomic — compound read-modify-write
        return this.value;
    }
}
```

**Fix:**
Replace with `java.util.concurrent.atomic.AtomicLong` and `java.util.concurrent.atomic.DoubleAdder`.

**Verification:**
```bash
# Under concurrent load, byte counts should match sum of individual deliveries
curl -s http://localhost:8081/prometheus | grep "data_refresh_bytes_transferred"
```

---

### B3-4 — Duplicate READY ACK guard commented out — metrics double-counted

**Status:** `FIXED`
**Commit:** `3974901`
**Severity:** Medium
**Batch:** 3

**File:**
- `broker/src/main/java/com/messaging/broker/refresh/DataRefreshManager.java:452-455`

**Bug:**
```java
//        if (context.getReceivedReadyAcks().contains(consumerGroupTopic)) {
//            log.debug("Duplicate READY ACK from {} for topic {}, ignoring", ...);
//            return;
//        }
```
Guard is commented out. Duplicate READY ACK calls `metrics.recordReadyAckReceived()` twice.
Counter not idempotent — `data_refresh_ready_ack_duration_seconds` double-recorded.

**Fix:**
Uncomment the 4 lines.

**Verification:**
```bash
# Send duplicate READY ACK manually, verify metric increments only once
docker logs messaging-broker 2>&1 | grep "Duplicate READY ACK"
```

---

### B4-4 — `saveSegmentMetadata()` called on every append — SQLite hammering

**Status:** `FIXED`
**Commit:** `2993cd0`
**Severity:** Medium
**Batch:** 4

**File:**
- `storage/src/main/java/com/messaging/storage/segment/SegmentManager.java:32,163-164`

**Bug:**
`METADATA_UPDATE_INTERVAL = 1000` is defined but never used as a gate.
`saveSegmentMetadata()` is called unconditionally on every single append — a SQLite UPSERT
per message. High write amplification at scale.

**Fix:**
```java
private final AtomicLong appendCount = new AtomicLong(0);

// In append():
long offset = current.append(record);
if (appendCount.incrementAndGet() % METADATA_UPDATE_INTERVAL == 0) {
    saveSegmentMetadata(current);
}
```

**Note:** After this fix, the metadata watermark can lag by up to 999 messages.
AdaptiveBatchDeliveryManager's `getMaxOffset()` check may temporarily skip delivery for
up to 1 poll cycle (max 1 second backoff). Functionally acceptable.

**Verification:**
```bash
# Measure SQLite write rate before/after
docker exec messaging-broker iostat -x 1 5
```

---

### B4-6 — `refresh_id` label causes unbounded Prometheus cardinality

**Status:** `FIXED`
**Commit:** `f62230b`
**Severity:** Medium
**Batch:** 4

**File:**
- `broker/src/main/java/com/messaging/broker/metrics/DataRefreshMetrics.java`

**Bug:**
`refresh_id` (a timestamp string) is used as a Prometheus label on every metric series.
Each refresh batch creates N×M new permanent time-series (N=topics, M=metric families).
After weeks of operation: unbounded Prometheus memory growth, slow dashboard queries.

**Fix:**
Remove `refresh_id` as a Prometheus label. Instead record it as a gauge value:
```java
Gauge.builder("data_refresh_current_batch_id", ...)
```
Or use a fixed label value `"current"` that gets overwritten each refresh.

**Verification:**
```bash
curl -s http://localhost:9090/api/v1/label/__name__/values | jq '.data | length'
# After fix: metric count stops growing with each refresh
```

---

### B5-2 — `completeRefresh()` tags metrics with wrong `refresh_id`

**Status:** `FIXED`
**Commit:** `eb642e5`
**Severity:** Medium
**Batch:** 5

**File:**
- `broker/src/main/java/com/messaging/broker/refresh/DataRefreshManager.java:488`

**Bug:**
```java
metrics.recordRefreshCompleted(topic, "LOCAL", "SUCCESS", currentRefreshId, context);
```
Uses instance field `currentRefreshId` instead of `context.getRefreshId()`. After restart
with multiple batches resumed, older topics completing are tagged with the newest batch ID.

**Fix:**
```java
metrics.recordRefreshCompleted(topic, "LOCAL", "SUCCESS", context.getRefreshId(), context);
```

**Verification:**
```bash
# After multi-topic refresh with restart, verify each topic's completion has correct refresh_id
curl -s http://localhost:8081/prometheus | grep "data_refresh_completed_total"
```

---

### B5-3 — `readySentTimes` map never cleared — memory leak

**Status:** `FIXED`
**Commit:** `eb642e5`
**Severity:** Medium
**Batch:** 5

**File:**
- `broker/src/main/java/com/messaging/broker/metrics/DataRefreshMetrics.java:247,525-584`

**Bug:**
`readySentTimes.put(key, ...)` in `recordReadySent()` — never removed in:
- `recordReadyAckReceived()` (uses `.get()` not `.remove()`)
- `resetMetricsForNewRefresh()` (clears other maps but not `readySentTimes`)
- `clearTopicState()` (clears other maps but not `readySentTimes`)

Map grows by N entries per refresh cycle (N = consumers × topics). Never shrinks.

**Fix:**
1. In `recordReadyAckReceived()`: change `readySentTimes.get(key)` → `readySentTimes.remove(key)`
2. In `resetMetricsForNewRefresh()`: add `readySentTimes.clear()`
3. In `clearTopicState()`: add `readySentTimes.keySet().removeIf(k -> k.startsWith(topic + ":"))`

**Verification:**
```bash
# After N refresh cycles, memory should be stable
docker stats messaging-broker --no-stream
# Heap should not grow proportionally to number of refreshes
```

---

### B9-1 — `resumeRefresh()` re-records ACK duration metrics on every restart — inflates Timers

**Status:** `FIXED`
**Commit:** `b7cafb2`
**Severity:** Medium
**Batch:** 9

**File:**
- `broker/src/main/java/com/messaging/broker/refresh/DataRefreshManager.java:564-670`
- `broker/src/main/java/com/messaging/broker/metrics/DataRefreshMetrics.java:287-308`

**Bug:**
`handleResetAck()` correctly guards against duplicate processing (line 254). However
`resumeRefresh()` bypasses this guard. In the `REPLAYING` and `READY_SENT` cases it
re-records `recordResetAckDuration()` and `recordReadyAckDuration()` for every consumer
already in `getReceivedResetAcks()` / `getReceivedReadyAcks()` (lines 613-615, 645-647,
657-659). `Timer.record()` accumulates — it does not overwrite. Each broker restart during
an active refresh adds a duplicate observation to the histogram.

```java
// resumeRefresh() REPLAYING case — re-records for all already-ACKed consumers
for (String consumer : context.getExpectedConsumers()) {
    metrics.recordResetSent(topic, consumer, replayingRefreshId);  // re-records
    if (context.getReceivedResetAcks().contains(consumer)) {
        // ... durationMs = time between resetSentTime and resetAckTime (historical) ...
        metrics.recordResetAckDuration(topic, consumer, replayingRefreshId, durationMs);
        // ↑ Called again on every restart — timer.record() accumulates, not overwrites
    }
}
```

With N broker restarts during a refresh, each already-ACKed consumer's duration timer
has N observations instead of 1. p50/p95/p99 percentiles are skewed toward repeated
historical values. `data_refresh_reset_ack_duration_seconds` and
`data_refresh_ready_ack_duration_seconds` become meaningless for long refresh cycles.

**Fix:**
Add a flag to `DataRefreshContext` to track whether resume metrics have already been
re-recorded. Only record on first resume after restart:
```java
// In DataRefreshContext:
private volatile boolean resumeMetricsRecorded = false;

// In resumeRefresh() before recording historical durations:
if (!context.isResumeMetricsRecorded()) {
    metrics.recordResetAckDuration(...);
    // ... other historical metrics ...
    context.setResumeMetricsRecorded(true);
}
```

**Verification:**
```bash
# Restart broker 3 times during active refresh, then check timer count
curl -s http://localhost:8081/prometheus | grep "data_refresh_reset_ack_duration_seconds_count"
# Before fix: count = 3 × (number of ACKed consumers) after 3 restarts
# After fix: count = 1 × (number of ACKed consumers) regardless of restart count
```

---

## LOW Bugs (Observability gaps / minor drift)

---

### B8-1 — `SegmentMetadataStoreFactory.closeAll()` not wired to `@PreDestroy` — SQLite connections not closed on shutdown

**Status:** `FIXED`
**Commit:** `ba86ebf`
**Severity:** Low
**Batch:** 8

**File:**
- `storage/src/main/java/com/messaging/storage/metadata/SegmentMetadataStoreFactory.java:55`

**Bug:**
`closeAll()` is defined at line 55 but has no `@PreDestroy` annotation. No storage engine
(`MMapStorageEngine`, `FileChannelStorageEngine`) calls `closeAll()` in its own `@PreDestroy`.
The 24 per-topic SQLite `Connection` objects are abandoned on JVM shutdown. Consequences:
- SQLite WAL journal files may not be checkpointed (extra files on disk after restart)
- Any pending dirty pages may not be flushed to the main database file
- JVM relies on GC finalizers to close connections, which is not guaranteed to run before exit

**Fix:**
Add `@PreDestroy` to `closeAll()`:
```java
@jakarta.annotation.PreDestroy
public void closeAll() {
    // existing code
}
```

**Verification:**
```bash
# After graceful broker shutdown, check for orphaned WAL files
ls -la /data/*/segment_metadata.db-wal 2>/dev/null
# Before fix: WAL files may exist after shutdown
# After fix: WAL files checkpointed and removed
```

---

### B2-3 — `activeConsumers` gauge drifts on duplicate SUBSCRIBE

**Status:** `FIXED`
**Commit:** `ba86ebf`
**Severity:** Low
**Batch:** 2

**File:**
- `broker/src/main/java/com/messaging/broker/core/BrokerService.java:273`
- `broker/src/main/java/com/messaging/broker/metrics/BrokerMetrics.java:206`

**Bug:**
`recordConsumerConnection()` called on every SUBSCRIBE without checking if already registered.
Duplicate SUBSCRIBE on same TCP connection increments counter without matching decrement.

**Fix:**
Check `consumers.containsKey(consumerKey)` before calling `recordConsumerConnection()`.

**Verification:**
```bash
curl -s http://localhost:8081/prometheus | grep "broker_consumer_active"
# Should match actual number of connected consumers
```

---

### B2-7 — `recordAckTimeout()` missing `group` label

**Status:** `FIXED`
**Commit:** `ba86ebf`
**Severity:** Low
**Batch:** 2

**File:**
- `broker/src/main/java/com/messaging/broker/metrics/BrokerMetrics.java:492`

**Bug:**
`recordAckTimeout(String topic)` — no `group` parameter or tag.
Cannot filter ACK timeout alerts by consumer group in Grafana.

**Fix:**
Add `group` parameter: `recordAckTimeout(String topic, String group)` and add `.tag("group", group)`.

**Verification:**
```bash
curl -s http://localhost:8081/prometheus | grep "broker_consumer_ack_timeouts_total"
# After fix: label set includes {topic, group}
```

---

### B3-7 — `startReplayForConsumer()` called for non-ACKed consumers

**Status:** `FIXED`
**Commit:** `ba86ebf`
**Severity:** Low
**Batch:** 3

**File:**
- `broker/src/main/java/com/messaging/broker/refresh/DataRefreshManager.java:396-399`

**Bug:**
In `checkReplayProgress()` else branch, `startReplayForConsumer()` called for all consumers
from `getAllConsumerIds()`, not just those in `ackedConsumers`. Inflates
`data_refresh_replay_started_total` metric. No functional delivery impact.

**Fix:**
Filter `allConsumerIds` to only those present in `ackedConsumers` before iterating.

**Verification:**
```bash
curl -s http://localhost:8081/prometheus | grep "data_refresh_replay_started_total"
# After fix: count matches number of consumers that ACKed RESET
```

---

---

## HIGH Bugs (Batches 6 & 7 additions)

---

### B6-1 / B7-1 — Segment boundary permanent stall (active segment never checked as fallback)

**Status:** `FIXED`
**Commit:** `f633440`
**Severity:** High (upgraded from Low — stall is permanent, not transient)
**Batch:** 6 (revised in Batch 7)

**File:**
- `storage/src/main/java/com/messaging/storage/segment/SegmentManager.java:326-357`
- `storage/src/main/java/com/messaging/storage/segment/Segment.java:719-836`

**Bug:**
After a segment rolls over, the new active segment has `baseOffset = oldSealedSegment.nextOffset`.
`getZeroCopyBatch(topic, fromOffset)` calls `floorEntry(fromOffset)`. When `fromOffset` equals
the new active's `baseOffset`, `floorEntry()` returns the OLD sealed segment (since its
`baseOffset ≤ fromOffset`). `getBatchFileRegion(fromOffset)` on the sealed segment returns
empty because `startOffset >= nextOffset` (equality). Because `entry != null`, the active segment
fallback at lines 333-348 is **never reached**. This repeats on every poll cycle — permanent stall.

```java
// SegmentManager.java:326-357
Map.Entry<Long, Segment> entry = segments.floorEntry(fromOffset);
if (entry != null) {
    result = entry.getValue().getBatchFileRegion(fromOffset, maxBatchBytes);
    // ← active segment fallback only runs when entry == null
}
```

**Fix:**
After `getBatchFileRegion()` returns empty from the sealed segment, also check the active segment:
```java
if (entry != null) {
    result = entry.getValue().getBatchFileRegion(fromOffset, maxBatchBytes);
}
// NEW: If sealed segment returned empty, check active segment as fallback
if ((result == null || result.isEmpty()) && activeSegment != null) {
    result = activeSegment.getBatchFileRegion(fromOffset, maxBatchBytes);
}
```

**Verification:**
```bash
# Trigger segment rollover (fill > 1GB) then check delivery continues
docker logs messaging-broker 2>&1 | grep "Rolled segment"
# After rollover: consumer lag should not stall
curl -s http://localhost:8081/prometheus | grep "broker_consumer_lag"
```

---

### B6-2 — Consumer offset below earliest segment → silent delivery freeze

**Status:** `FIXED`
**Commit:** `654abf2`
**Severity:** High
**Batch:** 6

**File:**
- `storage/src/main/java/com/messaging/storage/segment/SegmentManager.java:326-357`
- `storage/src/main/java/com/messaging/storage/segment/Segment.java:719-836`

**Bug:**
If a consumer's stored offset is below the earliest segment's `baseOffset` (e.g., after log
compaction or a storage wipe while consumer was down), `floorEntry(fromOffset)` returns `null`
(no segment has baseOffset ≤ fromOffset). The active segment fallback runs but
`getBatchFileRegion(fromOffset)` returns empty because `fromOffset < segment.baseOffset`.
No exception is thrown, no log is emitted — delivery silently freezes. The consumer never
catches up and no alert is raised.

**Fix:**
Detect when `fromOffset < earliestBaseOffset` and either:
1. Reset consumer offset to `earliestBaseOffset` (skip lost data, log a warning), or
2. Throw a recoverable exception that triggers consumer re-registration

```java
long earliest = segments.isEmpty() ? activeSegment.baseOffset : segments.firstKey();
if (fromOffset < earliest) {
    log.warn("Consumer offset {} is below earliest segment base {}, resetting to earliest",
             fromOffset, earliest);
    fromOffset = earliest;  // or unregister consumer to force re-subscribe
}
```

**Verification:**
```bash
# Manually set a consumer offset below segment base, restart broker
# Before fix: consumer silently stalls
# After fix: log shows "below earliest" warning and delivery resumes
docker logs messaging-broker 2>&1 | grep "below earliest"
```

---

### B7-2 — Crash window between log write and index write causes permanent record loss

**Status:** `FIXED`
**Commit:** `fe5a76e`
**Severity:** High
**Batch:** 7

**File:**
- `storage/src/main/java/com/messaging/storage/segment/Segment.java:382-464`

**Bug:**
`writeRecord()` writes to the log file first (line 425), then to the index file (line 446),
then `force()`s both (lines 456-457). If the JVM crashes or host loses power between lines
425 and 446, the log file has the record but the index file does not. On restart,
`recoverFromLogScan()` at line 336 throws `RuntimeException`:

```java
// Segment.java:340
throw new RuntimeException(
    "Cannot recover unified format segment without index file. " +
    "Log scan recovery not supported for this format.");
```

The record is permanently inaccessible. The segment cannot even be opened.

**Fix:**
Write and `force()` the index BEFORE writing the log (write-ahead-log pattern):
```java
// Write index entry first and sync it
indexChannel.write(indexEntry);
indexChannel.force(false);  // index durable first

// Then write log record
logChannel.write(ByteBuffer.wrap(record));
logChannel.force(false);  // log durable second
```
Or use a journal/WAL file for crash recovery.

**Verification:**
```bash
# Kill broker mid-write with SIGKILL, restart and verify no exception in logs
kill -9 $(docker inspect --format='{{.State.Pid}}' messaging-broker)
docker compose up -d broker
docker logs messaging-broker 2>&1 | grep "RuntimeException\|Cannot recover"
# Before fix: RuntimeException on startup
# After fix: clean startup with any complete records intact
```

---

## CRITICAL Bugs (Batches 6 & 7 additions)

---

### B6-3 — Header sent, FileRegion fails → `ZeroCopyBatchDecoder` stuck in `READING_ZERO_COPY_BATCH`

**Status:** `FIXED`
**Commit:** `c6d49ed`
**Severity:** Critical
**Batch:** 6

**File:**
- `broker/src/main/java/com/messaging/broker/consumer/RemoteConsumerRegistry.java:395-445`
- `network/src/main/java/com/messaging/network/codec/ZeroCopyBatchDecoder.java:39-44,115`

**Bug:**
`sendBatchToConsumer()` sends the `BATCH_HEADER` message at line 432, then sends the
`FileRegion` at line 444. If the header send succeeds but the FileRegion send fails
(e.g., channel closes mid-transfer), the consumer's `ZeroCopyBatchDecoder` has already
transitioned to state `READING_ZERO_COPY_BATCH` and is waiting for `expectedTotalBytes`.
These bytes never arrive. No state reset is triggered from the broker side. The consumer's
decoder is permanently stuck — all subsequent messages on that TCP connection are silently
discarded (interpreted as zero-copy batch bytes).

```java
// RemoteConsumerRegistry.java:432 — header sent, decoder transitions state
server.send(consumer.clientId, headerMsg).get(timeoutSeconds, TimeUnit.SECONDS);

// RemoteConsumerRegistry.java:444 — if this throws, decoder is stuck
server.sendFileRegion(consumer.clientId, batchRegion.fileRegion).get(timeoutSeconds, TimeUnit.SECONDS);
```

**Fix:**
On `sendFileRegion()` failure, close the consumer's TCP channel to force decoder reset
(consumer will reconnect and re-subscribe, resetting decoder to `READING_BROKER_MESSAGE`):
```java
try {
    server.sendFileRegion(...).get(timeout, SECONDS);
} catch (Exception e) {
    log.error("FileRegion send failed after header was sent — closing channel to reset decoder");
    server.closeChannel(consumer.clientId);  // forces consumer reconnect + decoder reset
    throw e;
}
```

**Verification:**
```bash
# Check for "decoder stuck" symptoms: consumer connected but no ACKs
docker logs messaging-broker 2>&1 | grep "Gate 2 BLOCKED" | tail -20
# If Gate 2 is blocked for the same consumer for > 30 min, decoder is stuck
```

---

### B7-3 — `decodeZeroCopyBatch()` always emits `BatchDecodedEvent` on partial parse → missing data + offset advanced

**Status:** `FIXED`
**Commit:** `e06412f`
**Severity:** Critical
**Batch:** 7

**File:**
- `network/src/main/java/com/messaging/network/codec/ZeroCopyBatchDecoder.java:251-352`

**Bug:**
`decodeZeroCopyBatch()` parses records in a while loop that `break`s on invalid `keyLen`/`dataLen`
or truncated data. After the loop exits (whether fully or partially parsed), line 344
**unconditionally** emits a `BatchDecodedEvent`:

```java
// ZeroCopyBatchDecoder.java:344 — always fires regardless of parse completeness
out.add(new BatchDecodedEvent(records, currentBatchTopic));
```

`BatchAckHandler` handles this event and sends `BATCH_ACK` to the broker. The broker's
`handleBatchAck()` calls `pendingOffsets.remove(pendingKey)` and commits the offset past all
records in the batch. Records that were not parsed (mid-batch corruption or sizing bug) are
**permanently lost** — the offset is advanced past them with no error logged.

**Fix:**
Track expected vs actual record count (from the `BATCH_HEADER`). Only emit if all records parsed:
```java
// Option A: Discard entire batch on parse error, close channel to trigger reconnect
if (records.size() < expectedRecordCount) {
    log.error("Partial parse: expected {} records, got {}. Discarding batch, closing channel.",
              expectedRecordCount, records.size());
    ctx.close();  // forces reconnect + re-delivery from last committed offset
    return;
}
out.add(new BatchDecodedEvent(records, currentBatchTopic));

// Option B: Emit only successfully parsed records (accept partial, advance offset carefully)
// Note: broker has no per-record offset tracking, so Option A is safer
```

**Verification:**
```bash
# Inject a batch with a corrupt record mid-batch (keyLen = -1)
# Before fix: consumer ACKs, offset advances, records lost silently
# After fix: error logged, channel closed, consumer reconnects, batch redelivered
docker logs consumer-price-quote 2>&1 | grep "Partial parse\|re-delivery"
```

---

## MEDIUM Bugs (Batches 6 & 7 additions)

---

### B6-5 — `SegmentMetadataStore` single `Connection` unsynchronized — concurrent writes cause `SQLITE_BUSY`

**Status:** `FIXED`
**Commit:** `fbcda46`
**Severity:** Medium
**Batch:** 6

**File:**
- `storage/src/main/java/com/messaging/storage/metadata/SegmentMetadataStore.java:23,80`

**Bug:**
`SegmentMetadataStore` uses a single `private Connection connection` with no `synchronized`
blocks on `saveSegment()`. Multiple broker threads calling `append()` → `saveSegmentMetadata()`
concurrently will hit the same `Connection`. SQLite's single-writer model rejects concurrent
writes with `SQLITE_BUSY`. The exception is silently swallowed — metadata is not saved, segment
boundaries become stale.

**Fix:**
Add `synchronized` to `saveSegment()` (and `getMaxOffset()` if sharing the connection):
```java
public synchronized void saveSegment(SegmentMetadata metadata) throws SQLException {
    // existing code
}
```
Or use a `ReentrantLock` for finer control, or use a dedicated writer thread.

**Verification:**
```bash
# Enable SQLite busy logging, run broker under load
docker logs messaging-broker 2>&1 | grep "SQLITE_BUSY\|database is locked"
# After fix: no SQLITE_BUSY errors under concurrent append load
```

---

## Summary

| Severity | Total | Fixed | Open |
|----------|-------|-------|------|
| Critical | 6     | 6     | 0    |
| High     | 10    | 10    | 0    |
| Medium   | 11    | 11    | 0    |
| Low      | 4     | 4     | 0    |
| **Total**| **31**| **31** | **0**|

### Fix Order
```
Round 1 — Critical: ✅ ALL DONE
  [x] B3-5   clearState() wrong overload (1-line fix)             9bd9993
  [x] B1-1   Adaptive delivery loop dies                          84a393a
  [x] B3-3   allConsumersCaughtUp() wrong consumer lookup         921fa05
  [x] B5-1   READY broadcast to non-reset consumers               3830980, a7fe869
  [x] B6-3   Header+FileRegion split → decoder stuck              c6d49ed
  [x] B7-3   Partial parse always emits ACK                       e06412f

Round 1 add-on — High (prerequisite for Critical fixes to work):
  [x] B3-1   startRefresh() expectedConsumers format mismatch     8f5c741

Round 2 — High: ✅ ALL DONE
  [x] B6-1   Segment boundary stall (active segment fallback)     f633440
  [x] B6-2   Offset below earliest segment → silent freeze        654abf2
  [x] B7-2   Crash window log→index → orphan bytes truncated      fe5a76e
  [x] B1-6   pendingOffsets not cleared on unregister             37c0d31
  [x] B4-3   Late ACK leaves inFlight stuck true                  bb57609
  [x] B2-6   ACK after unregister drops offset commit             719e64a
  [x] B1-2   deliveryTask never assigned                          d843a41
  [x] B1-3   ClassCastException sealed segment (instanceof fix)   442e4f4

Round 3 — Medium: ✅ ALL DONE
  [x] B2-1   Consumer lag never updated                           2d6a510
  [x] B2-2   Wire recordConsumerFailure/Retry                     2d6a510
  [x] B2-4   ACK timeout before send                              9b5a54f
  [x] B2-5   Non-atomic DataRefreshMetrics                        03261a1
  [x] B3-4   Uncomment duplicate READY ACK guard (4-line uncomment) 3974901
  [x] B4-4   SQLite write throttle                                2993cd0
  [x] B4-6   refresh_id cardinality explosion                     f62230b
  [x] B5-2   completeRefresh() uses wrong refresh_id              eb642e5
  [x] B5-3   readySentTimes memory leak                           eb642e5
  [x] B6-5   SegmentMetadataStore unsynchronized Connection       fbcda46
  [x] B9-1   resumeRefresh() inflates ACK duration Timers         b7cafb2

Round 4 — Low: ✅ ALL DONE
  [x] B8-1   SegmentMetadataStoreFactory.closeAll() @PreDestroy   ba86ebf
  [x] B2-3   activeConsumers gauge drift on duplicate SUBSCRIBE   ba86ebf
  [x] B2-7   Add group tag to recordAckTimeout                    ba86ebf
  [x] B3-7   Gate startReplayForConsumer by ackedConsumers        ba86ebf
```

---

*Last updated: 2026-02-19*
*Total bugs found: 31 | Fixed: 31 | Open: 0 — ALL BUGS RESOLVED*
