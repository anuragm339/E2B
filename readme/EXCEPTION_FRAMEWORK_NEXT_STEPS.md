# Custom Exception Framework - Next Steps for Integration

## Current Status: Framework Complete, Ready for Integration

✅ **Completed:**
- All custom exception classes created and compiled
- ErrorCode enum with 50+ error codes
- Factory methods for common scenarios
- ExceptionLogger utility for structured logging
- Comprehensive documentation (3 documents, 30KB total)

## What's Ready to Use

### Exception Classes (in common/src/main/java/com/messaging/common/exception/)
1. **MessagingException.java** - Base class with error codes, timestamps, context, retriability
2. **ErrorCode.java** - 50+ categorized error codes
3. **StorageException.java** - Storage layer exceptions
4. **NetworkException.java** - Network layer exceptions
5. **ConsumerException.java** - Consumer management exceptions
6. **DataRefreshException.java** - Data refresh workflow exceptions
7. **ExceptionLogger.java** - Logging utility

### Documentation
1. **EXCEPTION_USAGE_GUIDE.md** - Examples and best practices
2. **EXCEPTION_INTEGRATION_PLAN.md** - Phase-by-phase integration strategy
3. **CUSTOM_EXCEPTION_FRAMEWORK_SUMMARY.md** - Complete overview

## Integration Roadmap

### Phase 1: Storage Layer - Segment.java (PRIORITY 1)

**File:** `storage/src/main/java/com/messaging/storage/segment/Segment.java`

**Locations to update:** 25 places where IOException is used

**Key changes:**
1. Add import: `import com.messaging.common.exception.*;`
2. Change method signatures: `throws IOException` → `throws StorageException`
3. Replace exception throws:
   - Line 119: Corruption → `StorageException.corruption(...)`
   - Line 161, 166, 179, 184: Validation → `StorageException.corruption(...)`
   - Line 307: Segment full → `StorageException.writeFailed(...)`
   - Line 517, 529: Read/CRC errors → `StorageException.readFailed(...)` or `.crcMismatch(...)`
4. Wrap IOException in catch blocks with StorageException

**Example transformation:**
```java
// BEFORE (line 529):
throw new IOException("CRC32 mismatch for offset " + offset +
    " expected=" + entry.crc32 + " actual=" + (int) crc.getValue());

// AFTER:
throw ExceptionLogger.logAndThrow(log,
    StorageException.crcMismatch(
        "CRC32 validation failed for offset " + offset,
        offset)
        .withTopic(topic)  // Need to add topic field to Segment class
        .withPartition(partition)  // Need to add partition field
        .withSegmentPath(logPath.toString())
        .withContext("expectedCrc", entry.crc32)
        .withContext("actualCrc", (int) crc.getValue()));
```

**Requirements:**
- Add `private String topic;` field to Segment class
- Add `private int partition;` field to Segment class
- Update constructor to accept topic and partition

### Phase 2: Storage Layer - SegmentManager.java

**File:** `storage/src/main/java/com/messaging/storage/segment/SegmentManager.java`

**Changes:**
- Update method signatures to throw StorageException
- Wrap any IOException from Segment calls
- Add context (topic, partition) to all exceptions

### Phase 3: Storage Engines

**Files:**
- `storage/src/main/java/com/messaging/storage/mmap/MMapStorageEngine.java`
- `storage/src/main/java/com/messaging/storage/filechannel/FileChannelStorageEngine.java`

**Changes:**
- Update method signatures
- Wrap StorageException from Segment/SegmentManager
- Add topic/partition context

### Phase 4: Data Refresh Manager

**File:** `broker/src/main/java/com/messaging/broker/refresh/DataRefreshManager.java`

**Changes:**
- Replace timeout checks → `DataRefreshException.timeout(...)`
- Replace state checks → `DataRefreshException.alreadyInProgress(...)`
- Add structured logging with ExceptionLogger

### Phase 5: Consumer Components

**Files:**
- `broker/src/main/java/com/messaging/broker/consumer/RemoteConsumerRegistry.java`
- `broker/src/main/java/com/messaging/broker/consumer/ConsumerDeliveryManager.java`

**Changes:**
- Replace consumer errors → `ConsumerException.notRegistered(...)`, `.batchDeliveryFailed(...)`, etc.
- Add conditional logging based on retriability

### Phase 6: Full Build and Test

**Commands:**
```bash
# Build storage module
./gradlew :storage:build

# Build broker module
./gradlew :broker:build

# Build full system
./gradlew build

# Deploy to Docker
cd ..
docker compose up -d

# Verify structured logging
docker logs messaging-broker 2>&1 | grep "\[STORAGE_" | head -10
docker logs messaging-broker 2>&1 | grep "\[DATA_REFRESH_" | head -10
```

## Critical Design Decisions Needed

### 1. Topic/Partition Context in Segment

Segment.java currently doesn't store topic or partition information. We have two options:

**Option A: Add topic/partition fields to Segment**
```java
public class Segment {
    private String topic;  // NEW
    private int partition;  // NEW

    public Segment(Path logPath, Path indexPath, long baseOffset, long maxSize,
                   String topic, int partition) throws StorageException {
        this.topic = topic;
        this.partition = partition;
        // ... rest of constructor
    }
}
```
- **Pros:** Exceptions have full context
- **Cons:** Changes Segment constructor signature (need to update all callers)

**Option B: Add context at caller level**
```java
// In SegmentManager or StorageEngine
try {
    segment.append(record);
} catch (StorageException e) {
    throw e.withTopic(topic).withPartition(partition);
}
```
- **Pros:** Minimal changes to Segment
- **Cons:** Less context available within Segment methods

**Recommendation:** Option A - It's cleaner and provides better context

### 2. Exception Chaining Strategy

When catching IOException and converting to StorageException:

**Option A: Always wrap**
```java
catch (IOException e) {
    throw StorageException.ioError("...", e);  // IOException as cause
}
```

**Option B: Convert based on context**
```java
catch (IOException e) {
    if (operation == READ) {
        throw StorageException.readFailed(topic, partition, offset, e);
    } else {
        throw StorageException.writeFailed(topic, partition, e);
    }
}
```

**Recommendation:** Option B - More specific error codes

## Testing Strategy

### Unit Tests (create new files)
1. **StorageExceptionTest.java** - Test factory methods
2. **ExceptionContextTest.java** - Test context propagation
3. **ExceptionLoggingTest.java** - Test structured logging output

### Integration Tests
1. Trigger CRC errors - verify `STORAGE_CRC_MISMATCH` logged
2. Trigger offset errors - verify `STORAGE_OFFSET_OUT_OF_RANGE` logged
3. Trigger data refresh timeout - verify `DATA_REFRESH_TIMEOUT` logged
4. Check log format: `[ERROR_CODE] category=X, code=Y, retriable=Z, context={...}`

### Log Verification Commands
```bash
# Find all storage errors
docker logs messaging-broker 2>&1 | grep "category=STORAGE"

# Find non-retriable errors
docker logs messaging-broker 2>&1 | grep "retriable=false"

# Find specific error code
docker logs messaging-broker 2>&1 | grep "STORAGE_CRC_MISMATCH"

# Extract context
docker logs messaging-broker 2>&1 | grep "STORAGE_" | grep -o "context={[^}]*}"
```

## Estimated Effort

- **Phase 1 (Segment.java):** 4-6 hours
- **Phase 2 (SegmentManager.java):** 2-3 hours
- **Phase 3 (Storage Engines):** 2-3 hours
- **Phase 4 (DataRefreshManager):** 2-3 hours
- **Phase 5 (Consumer Components):** 3-4 hours
- **Phase 6 (Testing & Verification):** 3-4 hours

**Total: ~16-23 hours** (2-3 days)

## Success Criteria

✅ All modules compile without errors
✅ All existing tests pass
✅ New exception tests pass
✅ Structured logging appears in broker logs
✅ Error codes are parse-able for monitoring
✅ No functional regressions

## How to Start

### Step 1: Read the documentation
```bash
# Read usage guide
cat EXCEPTION_USAGE_GUIDE.md

# Read integration plan
cat EXCEPTION_INTEGRATION_PLAN.md

# Read summary
cat CUSTOM_EXCEPTION_FRAMEWORK_SUMMARY.md
```

### Step 2: Start with a simple example
Pick one method in Segment.java (e.g., `validateFileHeaders()`) and update it:

**Before:**
```java
private void validateFileHeaders() throws IOException {
    // ... validation logic ...
    if (invalidMagic) {
        throw new IOException("Invalid log file magic bytes");
    }
}
```

**After:**
```java
private void validateFileHeaders() throws StorageException {
    // ... validation logic ...
    if (invalidMagic) {
        throw ExceptionLogger.logAndThrow(log,
            StorageException.corruption("Invalid log file magic bytes")
                .withSegmentPath(logPath.toString())
                .withContext("expectedMagic", "MLOG")
                .withContext("actualMagic", actualMagic));
    }
}
```

### Step 3: Build and test
```bash
./gradlew :storage:build
# Fix any compilation errors
# Repeat until successful
```

### Step 4: Expand to rest of file
Apply the same pattern to all IOException uses in Segment.java

### Step 5: Move to next phase
Continue with SegmentManager, then storage engines, etc.

## Quick Reference

### Common Factory Methods

**Storage:**
```java
StorageException.corruption(message)
StorageException.crcMismatch(message, offset)
StorageException.readFailed(topic, partition, offset, cause)
StorageException.writeFailed(topic, partition, cause)
StorageException.offsetOutOfRange(topic, partition, offset, minOffset, maxOffset)
```

**Data Refresh:**
```java
DataRefreshException.timeout(refreshId, topic, timeoutSeconds)
DataRefreshException.alreadyInProgress(topic, existingRefreshId)
DataRefreshException.consumerNotReady(refreshId, topic, consumerGroup, elapsedSeconds)
DataRefreshException.replayFailed(refreshId, topic, startOffset, endOffset, cause)
```

**Consumer:**
```java
ConsumerException.notRegistered(group, topic)
ConsumerException.offsetCommitFailed(group, topic, offset, cause)
ConsumerException.batchDeliveryFailed(consumerId, topic, batchSize, cause)
```

### Logging Utilities

```java
// Log and throw
throw ExceptionLogger.logAndThrow(log, exception);

// Log based on retriability
ExceptionLogger.logConditional(log, exception);  // WARN if retriable, ERROR if not

// Just log
ExceptionLogger.logError(log, exception);
ExceptionLogger.logWarn(log, exception);
```

## Support

If you encounter issues during integration:
1. Check EXCEPTION_USAGE_GUIDE.md for examples
2. Check EXCEPTION_INTEGRATION_PLAN.md for strategy
3. Review factory method signatures in exception classes
4. Check error codes in ErrorCode.java

## Summary

The custom exception framework is **fully built and ready**. Integration is a systematic process of:
1. Adding imports
2. Changing throws clauses
3. Replacing exception creation with factory methods
4. Adding context
5. Using ExceptionLogger

Start small, test frequently, and expand gradually. The framework will significantly improve debugging and operational visibility once integrated!

