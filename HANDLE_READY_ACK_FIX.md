# HandleReadyAck Fix - Properties File Format Matching

## Issue

After changing the properties file format from:
```properties
active.refresh.topic=prices-v1
active.refresh.state=REPLAYING
consumer.prices-v1.reset.ack.received=true
```

To new multi-topic format:
```properties
active.refresh.topics=prices-v1,reference-data-v5
topic.prices-v1.state=REPLAYING
topic.prices-v1.consumer.prices-v1.reset.ack.received=true
topic.reference-data-v5.state=RESET_SENT
```

The `loadState()` method was still trying to read the old format, causing issues on broker restart!

## Problem Details

### Old `loadState()` Method
```java
public synchronized DataRefreshContext loadState() {
    Properties props = new Properties();
    props.load(in);

    // ❌ Looking for old format keys!
    String topic = props.getProperty("active.refresh.topic");
    String stateStr = props.getProperty("active.refresh.state");
    String consumersStr = props.getProperty("active.refresh.expected.consumers");

    // ❌ Looking for old consumer prefix
    String prefix = "consumer." + consumerId;
    props.getProperty(prefix + ".reset.ack.received");
}
```

### What Happened on Restart
1. Broker saves state in new format: `topic.prices-v1.state=REPLAYING`
2. Broker restarts
3. `init()` calls `loadState()`
4. `loadState()` looks for `active.refresh.topic` (doesn't exist!)
5. Returns `null` → state not recovered ❌
6. Refresh workflow lost! ❌

## Fixes Applied

### Fix 1: New `loadAllRefreshes()` Method (DataRefreshStateStore.java:131-172)

**Purpose:** Load ALL active refreshes in new format

```java
public synchronized Map<String, DataRefreshContext> loadAllRefreshes() {
    Map<String, DataRefreshContext> contexts = new HashMap<>();
    Properties props = new Properties();
    props.load(in);

    // ✅ Read new format - list of active topics
    Set<String> activeTopics = getActiveTopics(props);

    // ✅ Load each topic's state separately
    for (String topic : activeTopics) {
        DataRefreshContext context = loadTopicState(props, topic);
        contexts.put(topic, context);
    }

    return contexts;
}
```

### Fix 2: New `loadTopicState()` Method (DataRefreshStateStore.java:174-243)

**Purpose:** Load state for ONE specific topic from new format

```java
private DataRefreshContext loadTopicState(Properties props, String topic) {
    String topicPrefix = "topic." + topic;

    // ✅ Read new format properties
    String stateStr = props.getProperty(topicPrefix + ".state");
    String consumersStr = props.getProperty(topicPrefix + ".expected.consumers");

    // ✅ Read timestamps
    String resetSentTimeStr = props.getProperty(topicPrefix + ".reset.sent.time");
    context.setResetSentTime(Instant.parse(resetSentTimeStr));

    // ✅ Read consumer state with new prefix
    for (String consumerId : expectedConsumers) {
        String consumerPrefix = topicPrefix + ".consumer." + consumerId;

        boolean resetAck = Boolean.parseBoolean(
            props.getProperty(consumerPrefix + ".reset.ack.received", "false"));

        boolean readyAck = Boolean.parseBoolean(
            props.getProperty(consumerPrefix + ".ready.ack.received", "false"));

        String offsetStr = props.getProperty(consumerPrefix + ".current.offset");

        boolean replaying = Boolean.parseBoolean(
            props.getProperty(consumerPrefix + ".replaying", "false"));
    }

    return context;
}
```

### Fix 3: Updated `loadState()` for Backward Compatibility (DataRefreshStateStore.java:245-308)

**Purpose:** Support both old and new formats (marked as @Deprecated)

```java
@Deprecated
public synchronized DataRefreshContext loadState() {
    Properties props = new Properties();
    props.load(in);

    // ✅ Try new format first
    Set<String> activeTopics = getActiveTopics(props);
    if (!activeTopics.isEmpty()) {
        String firstTopic = activeTopics.iterator().next();
        return loadTopicState(props, firstTopic);
    }

    // ✅ Fall back to old format
    String topic = props.getProperty("active.refresh.topic");
    if (topic != null) {
        // Load using old format...
    }

    return null;
}
```

### Fix 4: Updated `init()` Method (DataRefreshManager.java:58-82)

**Purpose:** Load and resume ALL active refreshes on startup

**Before:**
```java
@PostConstruct
public void init() {
    // ❌ Only loaded ONE context
    DataRefreshContext context = stateStore.loadState();
    if (context != null) {
        activeRefreshes.put(context.getTopic(), context);
        resumeRefresh(context);
    }
}
```

**After:**
```java
@PostConstruct
public void init() {
    // ✅ Load ALL contexts
    Map<String, DataRefreshContext> savedRefreshes = stateStore.loadAllRefreshes();

    if (savedRefreshes.isEmpty()) {
        log.info("No saved refresh state found, starting fresh");
        return;
    }

    log.info("Resuming {} refresh(es) from saved state", savedRefreshes.size());

    // ✅ Resume each topic independently
    for (Map.Entry<String, DataRefreshContext> entry : savedRefreshes.entrySet()) {
        String topic = entry.getKey();
        DataRefreshContext context = entry.getValue();

        log.info("Resuming refresh for topic: {}, state: {}", topic, context.getState());
        activeRefreshes.put(topic, context);
        resumeRefresh(context);
    }

    log.info("Successfully resumed {} active refresh(es)", savedRefreshes.size());
}
```

## New Properties File Format

### Structure
```properties
# Global list of active topics
active.refresh.topics=prices-v1,reference-data-v5,non-promotable-products

# Topic-specific state
topic.prices-v1.state=REPLAYING
topic.prices-v1.start.time=2025-12-25T10:00:00Z
topic.prices-v1.reset.sent.time=2025-12-25T10:00:01Z
topic.prices-v1.expected.consumers=prices-v1

# Consumer state within topic
topic.prices-v1.consumer.prices-v1.reset.ack.received=true
topic.prices-v1.consumer.prices-v1.ready.ack.received=false
topic.prices-v1.consumer.prices-v1.current.offset=0
topic.prices-v1.consumer.prices-v1.replaying=true

# Another topic
topic.reference-data-v5.state=RESET_SENT
topic.reference-data-v5.start.time=2025-12-25T10:00:00Z
topic.reference-data-v5.reset.sent.time=2025-12-25T10:00:01Z
topic.reference-data-v5.expected.consumers=reference-data-v5
topic.reference-data-v5.consumer.reference-data-v5.reset.ack.received=false
```

### Key Format
```
active.refresh.topics                                    → Comma-separated list
topic.<TOPIC>.state                                      → DataRefreshState enum
topic.<TOPIC>.start.time                                 → ISO 8601 timestamp
topic.<TOPIC>.reset.sent.time                            → ISO 8601 timestamp
topic.<TOPIC>.ready.sent.time                            → ISO 8601 timestamp
topic.<TOPIC>.expected.consumers                         → Comma-separated list
topic.<TOPIC>.consumer.<CONSUMER_ID>.reset.ack.received  → boolean
topic.<TOPIC>.consumer.<CONSUMER_ID>.ready.ack.received  → boolean
topic.<TOPIC>.consumer.<CONSUMER_ID>.current.offset      → long
topic.<TOPIC>.consumer.<CONSUMER_ID>.replaying           → boolean
```

## Testing

### Test 1: Save and Load Single Topic
```bash
# Trigger refresh
curl -X POST "http://localhost:8081/admin/refresh-topic" \
  -H "Content-Type: application/json" \
  -d '{"topics": ["prices-v1"]}'

# Check file
cat /Users/anuragmishra/Desktop/workspace/messaging/data/data-refresh-state.properties

# Should show:
# active.refresh.topics=prices-v1
# topic.prices-v1.state=REPLAYING
# topic.prices-v1.consumer.prices-v1.reset.ack.received=true

# Restart broker
docker compose restart broker

# Check logs - should see:
# [INFO] Resuming 1 refresh(es) from saved state
# [INFO] Resuming refresh for topic: prices-v1, state: REPLAYING
```

### Test 2: Save and Load Multiple Topics
```bash
# Trigger refresh for 3 topics
curl -X POST "http://localhost:8081/admin/refresh-topic" \
  -H "Content-Type: application/json" \
  -d '{"topics": ["prices-v1","reference-data-v5","non-promotable-products"]}'

# Check file
cat /Users/anuragmishra/Desktop/workspace/messaging/data/data-refresh-state.properties

# Should show all 3 topics with their state

# Restart broker
docker compose restart broker

# Check logs - should see:
# [INFO] Resuming 3 refresh(es) from saved state
# [INFO] Resuming refresh for topic: prices-v1, state: REPLAYING
# [INFO] Resuming refresh for topic: reference-data-v5, state: RESET_SENT
# [INFO] Resuming refresh for topic: non-promotable-products, state: READY_SENT
# [INFO] Successfully resumed 3 active refresh(es)
```

### Test 3: Partial Completion + Restart
```bash
# Start refresh for 3 topics
curl -X POST "http://localhost:8081/admin/refresh-topic" \
  -H "Content-Type: application/json" \
  -d '{"topics": ["prices-v1","reference-data-v5","non-promotable-products"]}'

# Wait for "prices-v1" to complete (check logs)
# File should now show only 2 topics:
# active.refresh.topics=reference-data-v5,non-promotable-products

# Restart broker
docker compose restart broker

# Check logs - should resume only the 2 remaining topics:
# [INFO] Resuming 2 refresh(es) from saved state
# [INFO] Resuming refresh for topic: reference-data-v5, state: ...
# [INFO] Resuming refresh for topic: non-promotable-products, state: ...
```

## Expected Logs

### On Save
```
[DEBUG] Saved refresh state for topic: prices-v1 (total active topics: 1)
[DEBUG] Saved refresh state for topic: reference-data-v5 (total active topics: 2)
[DEBUG] Saved refresh state for topic: non-promotable-products (total active topics: 3)
```

### On Load (Startup)
```
[INFO ] Resuming 3 refresh(es) from saved state
[INFO ] Loaded refresh state for topic: prices-v1 (state=REPLAYING)
[INFO ] Resuming refresh for topic: prices-v1, state: REPLAYING
[INFO ] Loaded refresh state for topic: reference-data-v5 (state=RESET_SENT)
[INFO ] Resuming refresh for topic: reference-data-v5, state: RESET_SENT
[INFO ] Loaded refresh state for topic: non-promotable-products (state=READY_SENT)
[INFO ] Resuming refresh for topic: non-promotable-products, state: READY_SENT
[INFO ] Successfully resumed 3 active refresh(es)
```

### On Clear (Topic Completes)
```
[INFO ] Cleared refresh state for topic: prices-v1 (remaining active topics: 2)
[INFO ] Cleared refresh state for topic: reference-data-v5 (remaining active topics: 1)
[INFO ] Cleared refresh state file (no active topics)
```

## Files Modified

1. **DataRefreshStateStore.java**
   - Lines 131-172: New `loadAllRefreshes()` method
   - Lines 174-243: New `loadTopicState()` helper method
   - Lines 245-308: Updated `loadState()` with backward compatibility

2. **DataRefreshManager.java**
   - Lines 58-82: Updated `init()` to load all refreshes

## Summary

**Before:**
- ❌ `loadState()` only read old format
- ❌ `init()` loaded only 1 context
- ❌ Broker restart lost multi-topic refresh state

**After:**
- ✅ `loadAllRefreshes()` reads new format
- ✅ `loadTopicState()` parses per-topic state correctly
- ✅ `loadState()` supports both old and new formats (backward compatible)
- ✅ `init()` loads and resumes ALL active refreshes
- ✅ Broker restart preserves all topic refresh states
- ✅ Each topic resumes from correct state (RESET_SENT, REPLAYING, READY_SENT)
