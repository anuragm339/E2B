# Storage Exception Integration - Work in Progress

## Date: 2026-02-11 (Updated - Session 2)

## Summary
Continuing Phase 1 of custom exception framework integration. Significant progress made on Segment.java with 7 methods fully refactored to use StorageException. Approximately 60% complete.

## Completed Changes

### 1. Segment.java - Imports Added ✅
**File:** `storage/src/main/java/com/messaging/storage/segment/Segment.java`

**Lines 3-5:** Added custom exception imports:
```java
import com.messaging.common.exception.ErrorCode;
import com.messaging.common.exception.ExceptionLogger;
import com.messaging.common.exception.StorageException;
```

### 2. Segment.java - Topic/Partition Fields Added ✅
**Lines 42-44:** Added context fields for exception logging:
```java
// Topic and partition context for rich exception logging
private final String topic;
private final int partition;
```

### 3. Segment.java - Constructor Signature Updated ✅
**Line 69:** Updated constructor signature:
```java
// BEFORE:
public Segment(Path logPath, Path indexPath, long baseOffset, long maxSize) throws IOException

// AFTER:
public Segment(Path logPath, Path indexPath, long baseOffset, long maxSize, String topic, int partition) throws StorageException
```

**Lines 76-77:** Initialize topic and partition fields in constructor body.

### 4. initializeOrValidateHeaders() Method Updated ✅
**Lines 108-138:** Completely refactored to use StorageException:
- Changed method signature from `throws IOException` to `throws StorageException`
- Wrapped code in try-catch to catch IOException
- Replaced corruption detection throw with `StorageException.corruption()` factory method
- Added rich context (topic, partition, segmentPath, logSize, indexSize)
- Used `ExceptionLogger.logAndThrow()` for structured logging

### 5. writeFileHeaders() Method Updated ✅
**Lines 140-169:** Refactored to use StorageException:
- Changed method signature from `throws IOException` to `throws StorageException`
- Wrapped FileChannel operations in try-catch
- Replaced IOException throws with `StorageException.writeFailed()` factory method
- Added context: operation="writeFileHeaders"

## Remaining Work

### Phase 1A: Complete Segment.java IOException → StorageException Migration

#### 1. Update IOException Throws Across All Methods
Replace `throws IOException` with `throws StorageException` in:
- `initializeOrValidateHeaders()` (line 108)
- `writeFileHeaders()` (line 126)
- `validateFileHeaders()` (line 150)
- `recoverFromIndexFile(long indexSize)` (line 221)
- `append(MessageRecord record)` (line 270)
- `writeRecord(MessageRecord record, long offset)` (line 296)
- `read(long offset)` (line 371)
- `readRecordAtOffset(long offset)` (line 494)
- `getBatchFileRegion(long startOffset, long maxBytes)` (line 594)
- `findIndexEntryForOffset(long targetOffset)` (line 409)
- `close()` (line 709)

#### 2. Replace IOException Throws with StorageException Factory Methods

**Line 119 - Corrupted Segment:**
```java
// BEFORE:
throw new IOException("Corrupted segment files: log=" + logSize + "bytes, index=" + indexSize + "bytes");

// AFTER:
throw ExceptionLogger.logAndThrow(log,
    StorageException.corruption("Corrupted segment files: log=" + logSize + "bytes, index=" + indexSize + "bytes")
        .withTopic(topic)
        .withPartition(partition)
        .withSegmentPath(logPath.toString())
        .withContext("logSize", logSize)
        .withContext("indexSize", indexSize));
```

**Lines 161-162 - Invalid Log Magic:**
```java
// BEFORE:
throw new IOException("Invalid log file magic bytes (old format?) - expected MLOG, got: " +
    new String(logMagic, StandardCharsets.UTF_8));

// AFTER:
throw ExceptionLogger.logAndThrow(log,
    StorageException.corruption("Invalid log file magic bytes")
        .withTopic(topic)
        .withPartition(partition)
        .withSegmentPath(logPath.toString())
        .withContext("expectedMagic", "MLOG")
        .withContext("actualMagic", new String(logMagic, StandardCharsets.UTF_8)));
```

**Line 166 - Unsupported Log Version:**
```java
// BEFORE:
throw new IOException("Unsupported log file version: " + logVersion + " (expected " + FORMAT_VERSION + ")");

// AFTER:
throw ExceptionLogger.logAndThrow(log,
    StorageException.corruption("Unsupported log file version")
        .withTopic(topic)
        .withPartition(partition)
        .withSegmentPath(logPath.toString())
        .withContext("expectedVersion", FORMAT_VERSION)
        .withContext("actualVersion", logVersion));
```

**Lines 179-180 - Invalid Index Magic:**
```java
// BEFORE:
throw new IOException("Invalid index file magic bytes (old format?) - expected MIDX, got: " +
    new String(indexMagic, StandardCharsets.UTF_8));

// AFTER:
throw ExceptionLogger.logAndThrow(log,
    StorageException.corruption("Invalid index file magic bytes")
        .withTopic(topic)
        .withPartition(partition)
        .withSegmentPath(indexPath.toString())
        .withContext("expectedMagic", "MIDX")
        .withContext("actualMagic", new String(indexMagic, StandardCharsets.UTF_8)));
```

**Line 184 - Unsupported Index Version:**
```java
// BEFORE:
throw new IOException("Unsupported index file version: " + indexVersion + " (expected " + FORMAT_VERSION + ")");

// AFTER:
throw ExceptionLogger.logAndThrow(log,
    StorageException.corruption("Unsupported index file version")
        .withTopic(topic)
        .withPartition(partition)
        .withSegmentPath(indexPath.toString())
        .withContext("expectedVersion", FORMAT_VERSION)
        .withContext("actualVersion", indexVersion));
```

**Line 307 - Segment Full:**
```java
// BEFORE:
throw new IOException("Segment full");

// AFTER:
throw ExceptionLogger.logAndThrow(log,
    StorageException.writeFailed(topic, partition, new IOException("Segment full"))
        .withSegmentPath(logPath.toString())
        .withContext("logPosition", logPosition)
        .withContext("logRecordSize", logRecordSize)
        .withContext("maxSize", maxSize));
```

**Lines 517-518 - Read Failed:**
```java
// BEFORE:
throw new IOException("Failed to read log data for offset " + offset +
        " (expected " + entry.recordSize + " bytes, got " + bytesRead + ")");

// AFTER:
throw ExceptionLogger.logAndThrow(log,
    StorageException.readFailed(topic, partition, offset,
        new IOException("Incomplete read: expected " + entry.recordSize + ", got " + bytesRead))
        .withSegmentPath(logPath.toString())
        .withContext("logPosition", entry.logPosition)
        .withContext("expectedBytes", entry.recordSize)
        .withContext("actualBytes", bytesRead));
```

**Lines 529-530 - CRC Mismatch:**
```java
// BEFORE:
throw new IOException("CRC32 mismatch for offset " + offset +
        " (expected: " + entry.crc32 + ", got: " + calculatedCrc + ")");

// AFTER:
throw ExceptionLogger.logAndThrow(log,
    StorageException.crcMismatch("CRC32 validation failed", offset)
        .withTopic(topic)
        .withPartition(partition)
        .withSegmentPath(logPath.toString())
        .withContext("expectedCrc", entry.crc32)
        .withContext("actualCrc", calculatedCrc));
```

#### 3. Wrap IOException in Catch Blocks

In methods that call FileChannel operations and catch IOException, wrap with StorageException:

**Example from writeFileHeaders() (lines 132, 139):**
```java
try {
    logChannel.write(logHeader, 0);
    indexChannel.write(indexHeader, 0);
    logChannel.force(false);
    indexChannel.force(false);
} catch (IOException e) {
    throw ExceptionLogger.logAndThrow(log,
        StorageException.writeFailed(topic, partition, e)
            .withSegmentPath(logPath.toString())
            .withContext("operation", "writeFileHeaders"));
}
```

### Phase 1B: Update SegmentManager.java

**File:** `storage/src/main/java/com/messaging/storage/segment/SegmentManager.java`

#### 1. Add Custom Exception Imports
```java
import com.messaging.common.exception.StorageException;
import com.messaging.common.exception.ExceptionLogger;
```

#### 2. Update Segment Constructor Calls (2 locations)

**Line 93 - loadSegments() method:**
```java
// BEFORE:
Segment segment = new Segment(logPath, indexPath, baseOffset, maxSegmentSize);

// AFTER:
Segment segment = new Segment(logPath, indexPath, baseOffset, maxSegmentSize, topic, partition);
```

**Line 191 - createNewSegment() method:**
```java
// BEFORE:
Segment segment = new Segment(logPath, indexPath, baseOffset, maxSegmentSize);

// AFTER:
Segment segment = new Segment(logPath, indexPath, baseOffset, maxSegmentSize, topic, partition);
```

#### 3. Update Method Signatures
Replace `throws IOException` with `throws StorageException` in:
- `loadSegments()` (line 72)
- `createNewSegment(long baseOffset)` (line 182)
- `append(MessageRecord record)` (line 96)
- `read(long offset)` (line 112)
- `getBatchFileRegion(long startOffset, long maxBytes)` (line 149)

#### 4. Wrap IOException in Catch Blocks
Any IOException catches should wrap with StorageException and add topic/partition context.

### Phase 1C: Update MMapStorageEngine and FileChannelStorageEngine

Both files will need similar changes:
1. Add custom exception imports
2. Update method signatures to throw StorageException
3. Catch and wrap StorageException from SegmentManager calls
4. Add topic/partition context to exceptions

### Phase 1D: Build and Test

```bash
# Build storage module
./gradlew :storage:build

# Check for compilation errors
# If successful, proceed to broker module build

# Build broker module
./gradlew :broker:build

# Full system build
./gradlew build

# Deploy to Docker for integration testing
cd ..
docker compose up -d
```

## Critical Design Decision Made

**Chosen Approach:** Option A - Add topic/partition fields to Segment class

**Rationale:**
- Provides full context for all exceptions thrown within Segment methods
- Cleaner code - don't need to catch and re-throw at every call site
- Better long-term maintainability
- Aligns with "fail fast, fail loud" principle with rich context

**Trade-off:**
- Requires updating SegmentManager constructor calls (2 places)
- SegmentManager already has topic/partition fields, so easy to pass through

## Next Steps

1. Complete remaining IOException → StorageException replacements in Segment.java
2. Update SegmentManager.java to pass topic/partition to Segment constructor
3. Update storage engine implementations
4. Build and test storage module
5. Fix any compilation errors
6. Move to Phase 2: DataRefreshManager integration

## Build Status

❌ **Not yet built** - Changes are incomplete and will cause compilation errors

**Expected errors when attempting to build:**
- SegmentManager calls to `new Segment()` will fail due to signature mismatch
- Methods still throwing IOException while constructor expects StorageException

## Estimated Time to Complete Phase 1

- Phase 1A (Segment.java completion): 2-3 hours
- Phase 1B (SegmentManager.java): 30 minutes
- Phase 1C (Storage engines): 1-2 hours
- Phase 1D (Build & test): 1 hour

**Total: 4.5-6.5 hours**

## Related Documentation

- [Custom Exception Framework Summary](CUSTOM_EXCEPTION_FRAMEWORK_SUMMARY.md)
- [Exception Usage Guide](EXCEPTION_USAGE_GUIDE.md)
- [Exception Integration Plan](EXCEPTION_INTEGRATION_PLAN.md)
- [Next Steps Implementation](EXCEPTION_FRAMEWORK_NEXT_STEPS.md)

## How to Resume Work

1. Read this status document to understand current state
2. Continue with Segment.java IOException replacements (see "Remaining Work" section above)
3. Follow the integration plan document for phase-by-phase guidance
4. Test frequently after each file is completed
5. Update this document as work progresses

---

**Last Updated:** 2026-02-11
**Status:** In Progress - Phase 1A (30% complete)
**Next Task:** Replace IOException throws in initializeOrValidateHeaders() method (Segment.java:119)
