# Custom Exception Framework - Complete Implementation Summary

## Overview
A comprehensive custom exception framework has been created for the messaging broker system to replace generic exceptions with structured, context-rich exceptions that provide better error tracking, debugging capabilities, and operational visibility.

## What Was Built

### 1. Core Framework Components

#### MessagingException (Base Class)
Location: `common/src/main/java/com/messaging/common/exception/MessagingException.java`

Features:
- Base exception class extending `Exception` (checked exception)
- Error code association (`ErrorCode` enum)
- Timestamp tracking (`Instant timestamp`)
- Contextual metadata (`Map<String, Object> context`)
- Retriability indicator (`boolean retriable`)
- Fluent API for adding context (`.withContext(key, value)`)
- Structured message formatting for logging

**Example structured log output:**
```
[STORAGE_CRC_MISMATCH] category=STORAGE, code=1006, retriable=false, timestamp=2026-02-11T10:30:45Z, message=CRC32 validation failed for offset 12345, context={topic=prices-v1, partition=0, offset=12345, segmentPath=/data/prices-v1/partition-0/segment.log}
```

#### ErrorCode (Enum)
Location: `common/src/main/java/com/messaging/common/exception/ErrorCode.java`

Comprehensive error code catalog with **50+ error codes** organized by category:

**Storage Errors (1xxx):**
- `STORAGE_IO_ERROR` (1001) - I/O error during storage operation [retriable]
- `STORAGE_CORRUPTION` (1002) - Storage data corruption detected
- `STORAGE_CRC_MISMATCH` (1006) - CRC32 validation failed
- `STORAGE_OFFSET_OUT_OF_RANGE` (1007) - Requested offset out of range
- `STORAGE_SEGMENT_NOT_FOUND` (1008) - Segment file not found
- `STORAGE_WRITE_FAILED` (1009) - Failed to write to storage [retriable]
- `STORAGE_READ_FAILED` (1010) - Failed to read from storage [retriable]
- And 7 more...

**Network Errors (2xxx):**
- `NETWORK_CONNECTION_FAILED` (2001) - Failed to establish connection [retriable]
- `NETWORK_SEND_FAILED` (2003) - Failed to send message [retriable]
- `NETWORK_TIMEOUT` (2005) - Network operation timed out [retriable]
- `NETWORK_ENCODING_ERROR` (2006) - Message encoding failed
- `NETWORK_DECODING_ERROR` (2007) - Message decoding failed
- And 7 more...

**Consumer Errors (3xxx):**
- `CONSUMER_NOT_REGISTERED` (3001) - Consumer not registered
- `CONSUMER_DISCONNECTED` (3003) - Consumer disconnected [retriable]
- `CONSUMER_OFFSET_COMMIT_FAILED` (3005) - Offset commit failed [retriable]
- `CONSUMER_BATCH_DELIVERY_FAILED` (3007) - Batch delivery failed [retriable]
- And 8 more...

**Data Refresh Errors (4xxx):**
- `DATA_REFRESH_ALREADY_IN_PROGRESS` (4001) - Data refresh already in progress
- `DATA_REFRESH_TIMEOUT` (4002) - Data refresh operation timed out
- `DATA_REFRESH_CONSUMER_NOT_READY` (4003) - Consumer not ready for data refresh
- `DATA_REFRESH_REPLAY_FAILED` (4005) - Message replay failed [retriable]
- And 3 more...

**Additional categories:**
- Broker errors (5xxx)
- Registry errors (6xxx)
- Pipe errors (7xxx)
- Validation errors (8xxx)
- Concurrency errors (9xxx)

### 2. Category-Specific Exceptions

#### StorageException
Location: `common/src/main/java/com/messaging/common/exception/StorageException.java`

Fields:
- `topic` - Topic name
- `partition` - Partition number
- `offset` - Message offset
- `segmentPath` - Segment file path

Factory Methods:
```java
StorageException.ioError(message, cause)
StorageException.corruption(message)
StorageException.crcMismatch(message, offset)
StorageException.offsetOutOfRange(topic, partition, offset, minOffset, maxOffset)
StorageException.segmentNotFound(segmentPath)
StorageException.writeFailed(topic, partition, cause)
StorageException.readFailed(topic, partition, offset, cause)
```

#### NetworkException
Location: `common/src/main/java/com/messaging/common/exception/NetworkException.java`

Fields:
- `address` - Network address
- `port` - Port number

Factory Methods:
```java
NetworkException.connectionFailed(address, port, cause)
NetworkException.sendFailed(address, port, cause)
NetworkException.timeout(address, port, timeoutMs)
NetworkException.encodingError(messageType, cause)
NetworkException.decodingError(details, cause)
```

#### ConsumerException
Location: `common/src/main/java/com/messaging/common/exception/ConsumerException.java`

Fields:
- `consumerId` - Consumer identifier
- `consumerGroup` - Consumer group name
- `topic` - Topic name

Factory Methods:
```java
ConsumerException.notRegistered(group, topic)
ConsumerException.offsetCommitFailed(group, topic, offset, cause)
ConsumerException.batchDeliveryFailed(consumerId, topic, batchSize, cause)
```

#### DataRefreshException
Location: `common/src/main/java/com/messaging/common/exception/DataRefreshException.java`

Fields:
- `refreshId` - Refresh operation identifier
- `topic` - Topic name
- `refreshType` - Type of refresh operation

Factory Methods:
```java
DataRefreshException.alreadyInProgress(topic, existingRefreshId)
DataRefreshException.timeout(refreshId, topic, timeoutSeconds)
DataRefreshException.consumerNotReady(refreshId, topic, consumerGroup, elapsedSeconds)
DataRefreshException.replayFailed(refreshId, topic, startOffset, endOffset, cause)
```

### 3. Logging Utility

#### ExceptionLogger
Location: `common/src/main/java/com/messaging/common/exception/ExceptionLogger.java`

Methods:
```java
// Log at ERROR level
ExceptionLogger.logError(Logger log, MessagingException ex)

// Log at WARN level
ExceptionLogger.logWarn(Logger log, MessagingException ex)

// Log based on retriability: WARN if retriable, ERROR if not
ExceptionLogger.logConditional(Logger log, MessagingException ex)

// Log and throw (for inline use in catch blocks)
<T extends MessagingException> T logAndThrow(Logger log, T ex) throws T

// Get monitoring summary (single-line format)
String getMonitoringSummary(MessagingException ex)
```

## Documentation Created

### 1. EXCEPTION_USAGE_GUIDE.md
Comprehensive usage guide with:
- Quick start examples for each exception category
- Complete error code reference
- Best practices
- Integration guide
- Log parsing examples

### 2. EXCEPTION_INTEGRATION_PLAN.md
Detailed integration strategy:
- Phase-by-phase rollout plan
- File-by-file transformation examples
- Implementation timeline
- Success criteria
- Rollback plan

### 3. CUSTOM_EXCEPTION_FRAMEWORK_SUMMARY.md (this file)
High-level overview of the entire framework

## Build Status
✅ **Common module built successfully**
```bash
./gradlew :common:build -x test
# BUILD SUCCESSFUL in 1s
```

All exception classes compiled without errors. The framework is ready for integration into the broker and storage layers.

## Key Benefits

### 1. Structured Error Information
- **Before:** `IOException: Read failed`
- **After:** `[STORAGE_READ_FAILED] category=STORAGE, code=1010, retriable=true, context={topic=prices-v1, partition=0, offset=12345, segmentPath=/data/...}`

### 2. Retriability Indicators
Exceptions marked as retriable guide retry logic:
```java
catch (StorageException e) {
    if (e.isRetriable() && retryCount < MAX_RETRIES) {
        retry();  // Safe to retry
    } else {
        alert();  // Fatal error, needs manual intervention
    }
}
```

### 3. Contextual Debugging
All exceptions carry rich context:
- Topic, partition, offset for storage errors
- Consumer ID, group for consumer errors
- Refresh ID, refresh type for data refresh errors
- Address, port for network errors

### 4. Parse-able Logs
Structured format enables easy log parsing:
```bash
# Find all non-retriable errors
grep "retriable=false" broker.log

# Find all storage errors
grep "category=STORAGE" broker.log

# Find specific error code
grep "code=1006" broker.log | grep "STORAGE_CRC_MISMATCH"

# Extract context for debugging
grep "STORAGE_OFFSET_OUT_OF_RANGE" broker.log | grep -o "context={[^}]*}"
```

### 5. Monitoring Ready
Error codes and categories enable:
- Metrics by error category (`broker.exceptions.count{category="STORAGE"}`)
- Alerting on specific error codes (e.g., alert on `STORAGE_CORRUPTION`)
- Dashboards showing retriable vs non-retriable error rates

## Next Steps

### Phase 1: Storage Layer Integration (Day 1)
Integrate `StorageException` into:
- `Segment.java` - Replace IOException with StorageException
- `SegmentManager.java` - Add context to all storage errors
- `MMapStorageEngine.java` - Use factory methods
- `FileChannelStorageEngine.java` - Structured logging

### Phase 2: Data Refresh Integration (Day 2)
Integrate `DataRefreshException` into:
- `DataRefreshManager.java` - Replace generic exceptions
- `DataRefreshStateStore.java` - Add refresh context

### Phase 3: Consumer Integration (Day 3)
Integrate `ConsumerException` into:
- `RemoteConsumerRegistry.java`
- `ConsumerDeliveryManager.java`
- `ConsumerOffsetTracker.java`

### Phase 4: Network Integration (Day 4)
Integrate `NetworkException` into:
- `NettyTcpClient.java`
- `NettyTcpServer.java`
- Codec classes

### Phase 5: Testing & Deployment (Day 5)
- Build full system
- Deploy to Docker
- Verify structured logging
- Test data refresh workflow
- Grep logs for new format

## Usage Examples

### Example 1: Storage CRC Validation

**Before:**
```java
if (!validateCRC(data)) {
    log.error("CRC validation failed for offset {}", offset);
    throw new IOException("CRC validation failed");
}
```

**After:**
```java
if (!validateCRC(data)) {
    throw ExceptionLogger.logAndThrow(log,
        StorageException.crcMismatch(
            "CRC32 validation failed for offset " + offset,
            offset)
            .withTopic(topic)
            .withPartition(partition)
            .withSegmentPath(logPath.toString()));
}
```

**Logged output:**
```
ERROR [STORAGE_CRC_MISMATCH] category=STORAGE, code=1006, retriable=false, timestamp=2026-02-11T10:30:45Z, message=CRC32 validation failed for offset 12345, context={topic=prices-v1, partition=0, offset=12345, segmentPath=/data/prices-v1/partition-0/segment.log}
```

### Example 2: Data Refresh Timeout

**Before:**
```java
if (elapsedSeconds > timeoutSeconds) {
    log.error("Data refresh timed out after {}s", elapsedSeconds);
    throw new RuntimeException("Data refresh timeout");
}
```

**After:**
```java
if (elapsedSeconds > timeoutSeconds) {
    throw ExceptionLogger.logAndThrow(log,
        DataRefreshException.timeout(refreshId, topic, timeoutSeconds)
            .withContext("expectedConsumers", expectedConsumers.size())
            .withContext("readyConsumers", readyConsumers.size()));
}
```

**Logged output:**
```
ERROR [DATA_REFRESH_TIMEOUT] category=DATA_REFRESH, code=4002, retriable=false, timestamp=2026-02-11T10:35:12Z, message=Data refresh timed out: refreshId=refresh-001 topic=prices-v1 timeout=300s, context={refreshId=refresh-001, topic=prices-v1, timeoutSeconds=300, expectedConsumers=3, readyConsumers=1}
```

### Example 3: Consumer Batch Delivery

**Before:**
```java
try {
    networkClient.send(consumerId, batch);
} catch (IOException e) {
    log.error("Batch delivery failed", e);
    throw new RuntimeException(e);
}
```

**After:**
```java
try {
    networkClient.send(consumerId, batch);
} catch (IOException e) {
    ConsumerException ex = ConsumerException.batchDeliveryFailed(
        consumerId, topic, batch.size(), e)
        .withContext("retryAttempt", retryCount)
        .withContext("batchStartOffset", batch.get(0).getOffset());

    ExceptionLogger.logConditional(log, ex);  // WARN for retriable, ERROR for fatal

    if (ex.isRetriable() && retryCount < MAX_RETRIES) {
        retryDelivery(consumerId, topic, batch, retryCount + 1);
    } else {
        throw ex;
    }
}
```

**Logged output (retriable):**
```
WARN [CONSUMER_BATCH_DELIVERY_FAILED] category=CONSUMER, code=3007, retriable=true, timestamp=2026-02-11T10:40:23Z, message=Batch delivery failed: consumerId=consumer-001 topic=prices-v1 batchSize=100, context={consumerId=consumer-001, topic=prices-v1, batchSize=100, retryAttempt=1, batchStartOffset=12345}
```

## Files Created

### Exception Framework
1. `common/src/main/java/com/messaging/common/exception/MessagingException.java` - Base exception
2. `common/src/main/java/com/messaging/common/exception/ErrorCode.java` - Error code enum (50+ codes)
3. `common/src/main/java/com/messaging/common/exception/StorageException.java` - Storage-specific
4. `common/src/main/java/com/messaging/common/exception/NetworkException.java` - Network-specific
5. `common/src/main/java/com/messaging/common/exception/ConsumerException.java` - Consumer-specific
6. `common/src/main/java/com/messaging/common/exception/DataRefreshException.java` - Data refresh-specific
7. `common/src/main/java/com/messaging/common/exception/ExceptionLogger.java` - Logging utility

### Documentation
1. `EXCEPTION_USAGE_GUIDE.md` - Usage examples and best practices
2. `EXCEPTION_INTEGRATION_PLAN.md` - Integration strategy
3. `CUSTOM_EXCEPTION_FRAMEWORK_SUMMARY.md` - This summary document

## Summary
The custom exception framework is **fully implemented, compiled, and ready for integration**. It provides:
- ✅ Structured, context-rich exceptions
- ✅ 50+ error codes organized by category
- ✅ Retriability indicators for automated retry logic
- ✅ Factory methods for common scenarios
- ✅ Fluent API for adding context
- ✅ Structured logging support
- ✅ Parse-able log format for monitoring
- ✅ Comprehensive documentation

The framework will replace generic `IOException`, `RuntimeException`, and `IllegalStateException` with specific, context-rich exceptions that make debugging faster and operational visibility better.

