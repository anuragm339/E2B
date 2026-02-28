# Custom Exception Framework - Integration Plan

## Overview
This document outlines the plan to integrate custom exceptions throughout the messaging broker system.

## Current Status
✅ **Completed:**
- Base exception framework created (MessagingException, ErrorCode, ExceptionLogger)
- Category-specific exceptions implemented (StorageException, NetworkException, ConsumerException, DataRefreshException)
- Usage guide created (EXCEPTION_USAGE_GUIDE.md)
- Common module built and compiled successfully

## Integration Strategy

### Phase 1: Storage Layer (Highest Priority)
**Why first:** Storage is the foundation - all other components depend on it

**Files to modify:**
1. `storage/src/main/java/com/messaging/storage/segment/Segment.java`
2. `storage/src/main/java/com/messaging/storage/segment/SegmentManager.java`
3. `storage/src/main/java/com/messaging/storage/mmap/MMapStorageEngine.java`
4. `storage/src/main/java/com/messaging/storage/filechannel/FileChannelStorageEngine.java`

**Changes:**
- Replace `throws IOException` → `throws StorageException`
- Replace `new IOException(...)` → `StorageException.readFailed(...) / .writeFailed(...) / .crcMismatch(...)`
- Add `ExceptionLogger.logAndThrow()` for all exceptions
- Add context (topic, partition, offset, segmentPath) to all exceptions

**Example transformation:**
```java
// BEFORE:
public MessageRecord readRecordAtOffset(long offset) throws IOException {
    try {
        // ... reading logic ...
        if (!validateCRC(data)) {
            throw new IOException("CRC validation failed for offset " + offset);
        }
    } catch (IOException e) {
        log.error("Read failed", e);
        throw e;
    }
}

// AFTER:
public MessageRecord readRecordAtOffset(long offset) throws StorageException {
    try {
        // ... reading logic ...
        if (!validateCRC(data)) {
            throw ExceptionLogger.logAndThrow(log,
                StorageException.crcMismatch(
                    "CRC32 validation failed for offset " + offset,
                    offset)
                    .withTopic(topic)
                    .withPartition(partition)
                    .withSegmentPath(logPath.toString()));
        }
    } catch (IOException e) {
        throw ExceptionLogger.logAndThrow(log,
            StorageException.readFailed(topic, partition, offset, e));
    }
}
```

### Phase 2: Data Refresh Manager
**Files to modify:**
1. `broker/src/main/java/com/messaging/broker/refresh/DataRefreshManager.java`
2. `broker/src/main/java/com/messaging/broker/refresh/DataRefreshStateStore.java`

**Changes:**
- Replace timeout handling → `DataRefreshException.timeout(...)`
- Replace "already in progress" checks → `DataRefreshException.alreadyInProgress(...)`
- Replace consumer ready checks → `DataRefreshException.consumerNotReady(...)`
- Replace replay failures → `DataRefreshException.replayFailed(...)`

**Example transformation:**
```java
// BEFORE:
if (refreshInProgress) {
    throw new IllegalStateException("Data refresh already in progress for topic " + topic);
}

// AFTER:
if (refreshInProgress) {
    throw ExceptionLogger.logAndThrow(log,
        DataRefreshException.alreadyInProgress(topic, existingRefreshId));
}
```

### Phase 3: Consumer Management
**Files to modify:**
1. `broker/src/main/java/com/messaging/broker/consumer/RemoteConsumerRegistry.java`
2. `broker/src/main/java/com/messaging/broker/consumer/ConsumerDeliveryManager.java`
3. `broker/src/main/java/com/messaging/broker/consumer/ConsumerOffsetTracker.java`

**Changes:**
- Replace consumer not found → `ConsumerException.notRegistered(...)`
- Replace offset commit failures → `ConsumerException.offsetCommitFailed(...)`
- Replace batch delivery failures → `ConsumerException.batchDeliveryFailed(...)`

### Phase 4: Network Layer
**Files to modify:**
1. `network/src/main/java/com/messaging/network/tcp/NettyTcpClient.java`
2. `network/src/main/java/com/messaging/network/tcp/NettyTcpServer.java`
3. `network/src/main/java/com/messaging/network/codec/BinaryMessageEncoder.java`
4. `network/src/main/java/com/messaging/network/codec/BinaryMessageDecoder.java`

**Changes:**
- Replace connection failures → `NetworkException.connectionFailed(...)`
- Replace send failures → `NetworkException.sendFailed(...)`
- Replace encoding errors → `NetworkException.encodingError(...)`
- Replace decoding errors → `NetworkException.decodingError(...)`

### Phase 5: Broker Core
**Files to modify:**
1. `broker/src/main/java/com/messaging/broker/core/BrokerService.java`

**Changes:**
- Update exception handlers to use `instanceof` checks for custom exceptions
- Add conditional logging based on exception retriability
- Extract error codes and context for monitoring

**Example:**
```java
// BEFORE:
catch (IOException e) {
    log.error("Storage error", e);
    // ... error handling ...
}

// AFTER:
catch (StorageException e) {
    ExceptionLogger.logConditional(log, e);  // WARN if retriable, ERROR otherwise

    if (e.isRetriable() && retryCount < MAX_RETRIES) {
        // Retry logic
    } else {
        // Fatal error - propagate or handle
    }
}
```

## Implementation Order

### Day 1: Storage Layer Integration
- [ ] Update Segment.java exception handling
- [ ] Update SegmentManager.java exception handling
- [ ] Update StorageEngine implementations
- [ ] Build storage module: `./gradlew :storage:build`
- [ ] Fix any compilation errors

### Day 2: Data Refresh Integration
- [ ] Update DataRefreshManager.java
- [ ] Update DataRefreshStateStore.java
- [ ] Build broker module: `./gradlew :broker:build`
- [ ] Test data refresh workflow with new exceptions

### Day 3: Consumer Management Integration
- [ ] Update RemoteConsumerRegistry.java
- [ ] Update ConsumerDeliveryManager.java
- [ ] Update ConsumerOffsetTracker.java
- [ ] Build and test

### Day 4: Network Layer Integration
- [ ] Update NettyTcpClient.java
- [ ] Update NettyTcpServer.java
- [ ] Update codec classes
- [ ] Build network module: `./gradlew :network:build`

### Day 5: Broker Core & End-to-End Testing
- [ ] Update BrokerService.java
- [ ] Full system build: `./gradlew build`
- [ ] Deploy to Docker: `docker compose up -d`
- [ ] Verify structured logging in broker logs
- [ ] Trigger data refresh and verify exception logging

## Success Criteria

✅ **Compilation:**
- All modules build without errors
- No type mismatches or missing imports

✅ **Logging:**
- All exceptions use structured format: `[ERROR_CODE] category=X, code=Y, retriable=Z, context={...}`
- Retriable exceptions logged at WARN level
- Non-retriable exceptions logged at ERROR level

✅ **Runtime Behavior:**
- No change in functionality (backward compatible)
- Exceptions provide more context than before
- Easy to grep logs by error code or category

✅ **Monitoring:**
- Can parse logs for specific error codes
- Can identify retriable vs non-retriable errors
- Context fields (topic, partition, offset) available for debugging

## Rollback Plan
If issues arise:
1. Keep old code commented out for 1 week
2. Can revert to generic exceptions by uncommenting old code
3. No database or file format changes - safe to rollback

## Post-Integration Tasks
1. Add metrics for exception counts by category and code
2. Create Grafana dashboard for exception monitoring
3. Update documentation with new exception patterns
4. Train team on using custom exceptions

## Notes
- DO NOT change function signatures in public APIs (maintain backward compatibility)
- Use checked exceptions (StorageException extends Exception) for storage operations
- Use unchecked exceptions (RuntimeException) for programming errors only
- Always add context (topic, partition, offset, etc.) to exceptions
- Use ExceptionLogger for consistent logging

