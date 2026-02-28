# Data Refresh Properties File Fix

## Issues Found

### Issue 1: Expected Consumers Configuration Bug ⚠️ CRITICAL
**Problem:**
- `startRefresh(topic)` was using ALL 23 topics from config as "expected consumers"
- When refreshing "prices-v1", it expected ALL 23 topics to send ACKs!
- Only 1 consumer could respond, so refresh never completed

**Code (DataRefreshManager.java:78-95):**
```java
public CompletableFuture<RefreshResult> startRefresh(String topic) {
    // ❌ BUG: Uses ALL topics as expected consumers!
    List<String> expectedConsumers = config.getExpectedConsumers();

    DataRefreshContext context = new DataRefreshContext(
            topic,
            Set.copyOf(expectedConsumers)  // All 23 topics!
    );
}
```

**Symptoms:**
- Refresh for "prices-v1" expects 23 ACKs but only gets 1
- Refresh never completes
- Properties file shows all 23 topics in expected.consumers

### Issue 2: State File Overwrites with Multiple Topics ⚠️ CRITICAL
**Problem:**
- `saveState()` saved only ONE topic at a time
- When triggering 3 topics simultaneously, they overwrote each other
- Only the LAST topic's state was saved

**Code (DataRefreshStateStore.java:37-92):**
```java
public synchronized void saveState(DataRefreshContext context) {
    Properties props = new Properties();

    // ❌ Creates new Properties - loses other topics!
    props.setProperty("active.refresh.topic", context.getTopic());

    // ❌ Overwrites entire file
    Files.move(tempFile, stateFilePath, StandardCopyOption.REPLACE_EXISTING);
}
```

**What Happened:**
1. "prices-v1" saves state ✅
2. "reference-data-v5" saves state → overwrites "prices-v1" ❌
3. "non-promotable-products" saves state → overwrites "reference-data-v5" ❌
4. Only last topic saved in file ❌

## Fixes Applied

### Fix 1: Per-Topic Expected Consumers (DataRefreshManager.java:74-92)
**Changes:**
- Each topic refresh now expects only 1 consumer (the topic itself)
- Uses `Set.of(topic)` instead of loading all 23 topics from config
- Config is no longer used for per-topic refresh

**Before:**
```java
List<String> expectedConsumers = config.getExpectedConsumers(); // All 23 topics
DataRefreshContext context = new DataRefreshContext(topic, Set.copyOf(expectedConsumers));
```

**After:**
```java
Set<String> expectedConsumers = Set.of(topic); // Just this topic
DataRefreshContext context = new DataRefreshContext(topic, expectedConsumers);
```

**Result:**
- "prices-v1" refresh expects 1 ACK (just "prices-v1")
- "reference-data-v5" refresh expects 1 ACK (just "reference-data-v5")
- Each topic completes independently ✅

### Fix 2: Multi-Topic State Storage (DataRefreshStateStore.java:34-129)
**Changes:**
- New format: `topic.<topic-name>.<property>` instead of global properties
- `saveState()` loads existing state, adds new topic, saves all together
- `clearState(topic)` removes only the specified topic
- Tracks active topics in `active.refresh.topics` comma-separated list

**New Properties Format:**
```properties
active.refresh.topics=prices-v1,reference-data-v5,non-promotable-products

topic.prices-v1.state=REPLAYING
topic.prices-v1.expected.consumers=prices-v1
topic.prices-v1.consumer.prices-v1.reset.ack.received=true

topic.reference-data-v5.state=RESET_SENT
topic.reference-data-v5.expected.consumers=reference-data-v5

topic.non-promotable-products.state=READY_SENT
topic.non-promotable-products.expected.consumers=non-promotable-products
```

**Methods Added:**
- `loadAllStates()` - Load existing properties without clearing
- `getActiveTopics(props)` - Extract list of active topics
- `clearState(topic)` - Remove specific topic (overloaded)

### Fix 3: Smart Pipe Resume (DataRefreshManager.java:292-322)
**Changes:**
- Only resume pipe calls when ALL topic refreshes complete
- Checks if other topics are still refreshing before resuming
- Each topic clears only its own state

**Logic:**
```java
// Resume pipe calls only if NO other refreshes are in progress
boolean otherRefreshesActive = activeRefreshes.values().stream()
        .anyMatch(ctx -> !ctx.getTopic().equals(topic) &&
                         ctx.getState() != DataRefreshState.COMPLETED);

if (!otherRefreshesActive) {
    pipeConnector.resumePipeCalls();
} else {
    log.info("Pipe calls remain PAUSED (other topic refreshes still in progress)");
}

// Clear state for this topic only (keeps other topics' state)
stateStore.clearState(topic);
```

## Expected Behavior After Fix

### Single Topic Refresh:
```bash
curl -X POST "http://localhost:8081/admin/refresh-topic" \
  -H "Content-Type: application/json" \
  -d '{"topics": ["prices-v1"]}'
```

**Properties File:**
```properties
active.refresh.topics=prices-v1
topic.prices-v1.state=REPLAYING
topic.prices-v1.expected.consumers=prices-v1
topic.prices-v1.consumer.prices-v1.reset.ack.received=true
```

### Multiple Topics Refresh:
```bash
curl -X POST "http://localhost:8081/admin/refresh-topic" \
  -H "Content-Type: application/json" \
  -d '{"topics": ["prices-v1","reference-data-v5","non-promotable-products"]}'
```

**Properties File (while all 3 are refreshing):**
```properties
active.refresh.topics=prices-v1,reference-data-v5,non-promotable-products

topic.prices-v1.state=REPLAYING
topic.prices-v1.expected.consumers=prices-v1
topic.prices-v1.consumer.prices-v1.reset.ack.received=true

topic.reference-data-v5.state=RESET_SENT
topic.reference-data-v5.expected.consumers=reference-data-v5

topic.non-promotable-products.state=READY_SENT
topic.non-promotable-products.expected.consumers=non-promotable-products
topic.non-promotable-products.consumer.non-promotable-products.ready.ack.received=true
```

**Properties File (after "prices-v1" completes):**
```properties
active.refresh.topics=reference-data-v5,non-promotable-products

topic.reference-data-v5.state=RESET_SENT
topic.reference-data-v5.expected.consumers=reference-data-v5

topic.non-promotable-products.state=READY_SENT
topic.non-promotable-products.expected.consumers=non-promotable-products
```

**Properties File (after all complete):**
```
(empty - file deleted)
```

## Testing

### 1. Rebuild and Restart
```bash
cd /Users/anuragmishra/Desktop/workspace/messaging/provider
./gradlew :broker:build
docker compose restart broker
```

### 2. Clean State
```bash
# Delete old state file
rm /Users/anuragmishra/Desktop/workspace/messaging/data/data-refresh-state.properties

# Restart broker
docker compose restart broker
```

### 3. Trigger Refresh
```bash
curl -X POST "http://localhost:8081/admin/refresh-topic" \
  -H "Content-Type: application/json" \
  -d '{
    "topics": [
      "prices-v1",
      "reference-data-v5",
      "non-promotable-products"
    ]
  }'
```

### 4. Monitor Properties File
```bash
# Watch file update in real-time
watch -n 1 'cat /Users/anuragmishra/Desktop/workspace/messaging/data/data-refresh-state.properties'

# Or check once
cat /Users/anuragmishra/Desktop/workspace/messaging/data/data-refresh-state.properties
```

### 5. Expected Log Output
```
[INFO ] Received refresh request for 3 topics: [prices-v1, reference-data-v5, non-promotable-products]
[INFO ] Starting refresh for topic: prices-v1 with 1 expected consumer(s)
[INFO ] Starting refresh for topic: reference-data-v5 with 1 expected consumer(s)
[INFO ] Starting refresh for topic: non-promotable-products with 1 expected consumer(s)
[INFO ] Pipe calls PAUSED for refresh of topic: prices-v1
[INFO ] RESET sent to all consumers for topic: prices-v1
[DEBUG] Saved refresh state for topic: prices-v1 (total active topics: 1)
[INFO ] RESET sent to all consumers for topic: reference-data-v5
[DEBUG] Saved refresh state for topic: reference-data-v5 (total active topics: 2)
[INFO ] RESET sent to all consumers for topic: non-promotable-products
[DEBUG] Saved refresh state for topic: non-promotable-products (total active topics: 3)

[INFO ] RESET ACK received from prices-v1 for topic prices-v1 (1/1)
[DEBUG] Triggering replay for 1 consumers on topic prices-v1
[INFO ] All consumers caught up for topic prices-v1, sending READY
[INFO ] READY sent to all consumers for topic: prices-v1
[INFO ] READY ACK received from prices-v1 for topic prices-v1 (1/1)
[INFO ] All READY ACKs received for topic prices-v1, refresh COMPLETE
[INFO ] Pipe calls remain PAUSED (other topic refreshes still in progress)
[INFO ] Cleared refresh state for topic: prices-v1 (remaining active topics: 2)

[INFO ] RESET ACK received from reference-data-v5 for topic reference-data-v5 (1/1)
[INFO ] READY ACK received from reference-data-v5 for topic reference-data-v5 (1/1)
[INFO ] All READY ACKs received for topic reference-data-v5, refresh COMPLETE
[INFO ] Pipe calls remain PAUSED (other topic refreshes still in progress)
[INFO ] Cleared refresh state for topic: reference-data-v5 (remaining active topics: 1)

[INFO ] READY ACK received from non-promotable-products for topic non-promotable-products (1/1)
[INFO ] All READY ACKs received for topic non-promotable-products, refresh COMPLETE
[INFO ] Pipe calls RESUMED after refresh of topic: non-promotable-products (no other active refreshes)
[INFO ] Cleared refresh state file (no active topics)
```

## Files Modified

1. **DataRefreshManager.java**
   - Lines 74-92: Fixed expected consumers to use `Set.of(topic)` instead of config
   - Lines 292-322: Smart pipe resume - only when all topics complete

2. **DataRefreshStateStore.java**
   - Lines 34-129: Multi-topic state storage with new format
   - Lines 184-233: Topic-specific clearState() method
   - Added helper methods: `loadAllStates()`, `getActiveTopics()`

## Verification Checklist

- [ ] Build succeeds
- [ ] Broker starts
- [ ] Trigger refresh for 3 topics
- [ ] Properties file shows `active.refresh.topics=prices-v1,reference-data-v5,non-promotable-products`
- [ ] Each topic has its own `topic.<name>.*` properties
- [ ] Each topic expects only 1 consumer (itself)
- [ ] Each topic completes independently
- [ ] As each topic completes, it's removed from properties file
- [ ] Pipe calls resume only after ALL topics complete
- [ ] Final properties file is empty (deleted)

## Summary

Before:
- ❌ Expected 23 consumers for each topic
- ❌ Only last topic saved in properties file
- ❌ Pipe resumed after first topic completed

After:
- ✅ Each topic expects 1 consumer (itself)
- ✅ All topics saved in properties file simultaneously
- ✅ Pipe resumes only when ALL topics complete
- ✅ Each topic can be tracked independently
- ✅ Supports concurrent multi-topic refresh
