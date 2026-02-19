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

**Status:** `OPEN`
**Commit:** —
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

**Status:** `OPEN`
**Commit:** —
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

**Status:** `OPEN`
**Commit:** —
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

**Status:** `OPEN`
**Commit:** —
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

**Status:** `OPEN`
**Commit:** —
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

**Status:** `OPEN`
**Commit:** —
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

**Status:** `OPEN`
**Commit:** —
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

**Status:** `OPEN`
**Commit:** —
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

**Status:** `OPEN`
**Commit:** —
**Severity:** High
**Batch:** 3
**Note:** Must fix together — fixing one without the other breaks refresh.

**File:**
- `broker/src/main/java/com/messaging/broker/refresh/DataRefreshManager.java:162`
- `broker/src/main/java/com/messaging/broker/consumer/RemoteConsumerRegistry.java:638`

**Bug B3-1:**
`startRefresh()` always hardcodes `expectedConsumers = Set.of(topic)` ignoring `application.yml` config.
Multi-group refresh completes after first group ACKs.

**Bug B3-2:**
`getConsumerGroupTopic()` returns `consumer.topic` (just topic string) instead of `group + ":" + topic`.
If B3-1 is fixed to use configured `group:topic` identifiers, all RESET/READY ACKs will be rejected
because they return just the topic name but the expected set has `group:topic`.

**Fix:**
- B3-1: Replace `Set.of(topic)` with `config.getExpectedConsumers()` filtered for this topic
- B3-2: Return `consumer.group + ":" + consumer.topic` from `getConsumerGroupTopic()`

**Verification:**
```bash
# Verify configured expected consumers are logged at refresh start
docker logs messaging-broker 2>&1 | grep "Starting refresh for topic"
# After fix: should show all configured groups, not just the topic name
```

---

### B4-3 — Late ACK after timeout leaves `inFlight` stuck true — delivery permanently stalls

**Status:** `OPEN`
**Commit:** —
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

**Status:** `OPEN`
**Commit:** —
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

**Status:** `OPEN`
**Commit:** —
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

**Status:** `OPEN`
**Commit:** —
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

**Status:** `OPEN`
**Commit:** —
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

**Status:** `OPEN`
**Commit:** —
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

**Status:** `OPEN`
**Commit:** —
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

**Status:** `OPEN`
**Commit:** —
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

**Status:** `OPEN`
**Commit:** —
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

**Status:** `OPEN`
**Commit:** —
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

## LOW Bugs (Observability gaps / minor drift)

---

### B2-3 — `activeConsumers` gauge drifts on duplicate SUBSCRIBE

**Status:** `OPEN`
**Commit:** —
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

**Status:** `OPEN`
**Commit:** —
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

**Status:** `OPEN`
**Commit:** —
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

## Summary

| Severity | Total | Fixed | Open |
|----------|-------|-------|------|
| Critical | 4     | 0     | 4    |
| High     | 7     | 0     | 7    |
| Medium   | 9     | 0     | 9    |
| Low      | 3     | 0     | 3    |
| **Total**| **23**| **0** | **23**|

### Fix Order
```
Round 1 — Critical:
  [ ] B3-5   clearState() wrong overload (1-line fix)
  [ ] B1-1   Adaptive delivery loop dies
  [ ] B3-3   allConsumersCaughtUp() wrong consumer lookup
  [ ] B5-1   READY broadcast to non-reset consumers

Round 2 — High:
  [ ] B1-6   pendingOffsets not cleared on unregister
  [ ] B4-3   Late ACK leaves inFlight stuck true
  [ ] B3-1+B3-2  config ignored + getConsumerGroupTopic() wrong return (fix together)
  [ ] B2-6   ACK after unregister drops offset commit
  [ ] B1-2   deliveryTask never assigned
  [ ] B1-3   ClassCastException sealed segment

Round 3 — Medium:
  [ ] B2-1   Consumer lag never updated
  [ ] B2-4   ACK timeout before send
  [ ] B2-5   Non-atomic DataRefreshMetrics
  [ ] B3-4   Uncomment duplicate READY ACK guard (4-line uncomment)
  [ ] B2-2   Wire recordConsumerFailure/Retry
  [ ] B4-4   SQLite write throttle
  [ ] B4-6   refresh_id cardinality explosion
  [ ] B5-2   completeRefresh() uses wrong refresh_id
  [ ] B5-3   readySentTimes memory leak

Round 4 — Low:
  [ ] B2-3   activeConsumers gauge drift
  [ ] B2-7   Add group tag to recordAckTimeout
  [ ] B3-7   Gate startReplayForConsumer by ackedConsumers
```

---

*Last updated: 2026-02-19*
*Total bugs found: 23 | Fixed: 0 | Open: 23*
