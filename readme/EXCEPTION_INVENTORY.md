# Complete Exception Conversion Inventory

## Overview
This document provides a complete inventory of all Java files that throw generic exceptions and require conversion to custom exceptions. Files are organized by priority based on their layer and criticality.

---

## Priority 1: Storage Layer Files (CRITICAL)

### 1.1 `/Users/anuragmishra/Desktop/workspace/messaging/provider/storage/src/main/java/com/messaging/storage/segment/SegmentManager.java`

**Generic Exceptions Thrown:**
- `IOException` (line 57, 88)
- `IllegalArgumentException` (line 134)
- `IllegalStateException` (line 172)

**Conversion Details:**

| Line | Generic Exception | Count | Recommended Custom Exception | Notes |
|------|-------------------|-------|------------------------------|-------|
| 57 | IOException | 1 | StorageException | Wrapping Files.createDirectories() failure |
| 88 | IOException | 1 | StorageException | Wrapping Files.list() failure during segment discovery |
| 134 | IllegalArgumentException | 1 | StorageException | Invalid segment filename format (should use StorageException with ErrorCode.INVALID_SEGMENT_NAME) |
| 172 | IllegalStateException | 1 | StorageException | No active segment exists during roll |

**Action Items:**
- [ ] Convert IOException to StorageException with ErrorCode.STORAGE_IO_ERROR
- [ ] Convert IllegalArgumentException to StorageException with ErrorCode.INVALID_FORMAT
- [ ] Convert IllegalStateException to StorageException with ErrorCode.INVALID_STATE
- [ ] Total throw statements: 4

---

### 1.2 `/Users/anuragmishra/Desktop/workspace/messaging/provider/storage/src/main/java/com/messaging/storage/segment/Segment.java`

**Generic Exceptions Thrown:**
- `IOException` (line 114, 320, 341, 450, 671, 838)
- `RuntimeException` (line 341)
- `UnsupportedOperationException` (line 485)

**Conversion Details:**

| Line | Generic Exception | Count | Recommended Custom Exception | Notes |
|------|-------------------|-------|------------------------------|-------|
| 114 | IOException | 1 | StorageException | Opening segment file channels |
| 320 | IOException | 1 | StorageException | Reading index file during recovery |
| 341 | RuntimeException | 1 | StorageException | Cannot recover without index file (split into two) |
| 450 | IOException or MessagingException | 2 | StorageException | writeRecord method already wraps exceptions |
| 485 | UnsupportedOperationException | 1 | StorageException | Deprecated readRecordAt method - should throw StorageException instead |
| 671 | IOException or MessagingException | 2 | StorageException | readRecordAtOffset already wraps exceptions |
| 838 | IOException or MessagingException | 1 | StorageException | close method already wraps exceptions |

**Action Items:**
- [ ] Convert IOException (line 114) to StorageException with ErrorCode.STORAGE_IO_ERROR
- [ ] Convert IOException (line 320) to StorageException with ErrorCode.STORAGE_IO_ERROR
- [ ] Convert RuntimeException (line 341) to StorageException with ErrorCode.STORAGE_CORRUPTION
- [ ] Convert UnsupportedOperationException (line 485) to StorageException with ErrorCode.UNSUPPORTED_OPERATION
- [ ] Note: Most others already wrapped via ExceptionLogger.logAndThrow()
- [ ] Total standalone throw statements: 4

---

### 1.3 `/Users/anuragmishra/Desktop/workspace/messaging/provider/storage/src/main/java/com/messaging/storage/mmap/MMapStorageEngine.java`

**Generic Exceptions Thrown:**
- `RuntimeException` (line 82, 190, 225)

**Conversion Details:**

| Line | Generic Exception | Count | Recommended Custom Exception | Notes |
|------|-------------------|-------|------------------------------|-------|
| 82 | RuntimeException | 1 | StorageException | Wrapping StorageException from manager.read() |
| 190 | RuntimeException | 1 | StorageException | Recovery failure during initialization |
| 225 | RuntimeException | 1 | StorageException | Failed to create segment manager |

**Action Items:**
- [ ] Convert RuntimeException (line 82) to StorageException with ErrorCode.STORAGE_READ_ERROR
- [ ] Convert RuntimeException (line 190) to StorageException with ErrorCode.STORAGE_RECOVERY_FAILED
- [ ] Convert RuntimeException (line 225) to StorageException with ErrorCode.STORAGE_INITIALIZATION_FAILED
- [ ] Total throw statements: 3

---

### 1.4 `/Users/anuragmishra/Desktop/workspace/messaging/provider/storage/src/main/java/com/messaging/storage/metadata/SegmentMetadataStore.java`

**Generic Exceptions Thrown:**
- `RuntimeException` (line 40, 99, 137, 192)
- `SQLException` caught and logged (line 38, 97, 135, 170, 190, 205)

**Conversion Details:**

| Line | Generic Exception | Count | Recommended Custom Exception | Notes |
|------|-------------------|-------|------------------------------|-------|
| 40 | RuntimeException | 1 | StorageException | Database initialization failure |
| 99 | RuntimeException | 1 | StorageException | SQL save operation failure (wrapping SQLException) |
| 137 | RuntimeException | 1 | StorageException | SQL query operation failure (wrapping SQLException) |
| 192 | RuntimeException | 1 | StorageException | SQL delete operation failure (wrapping SQLException) |

**Action Items:**
- [ ] Convert all RuntimeException wrapping SQLException to StorageException
- [ ] Use ErrorCode.STORAGE_METADATA_ERROR for metadata-specific issues
- [ ] Create proper exception chaining (root cause = SQLException)
- [ ] Total throw statements: 4

---

## Priority 2: DataRefresh Layer Files (CRITICAL)

### 2.1 `/Users/anuragmishra/Desktop/workspace/messaging/provider/broker/src/main/java/com/messaging/broker/refresh/DataRefreshManager.java`

**Generic Exceptions Thrown:**
- Need full file read to determine (only first 100 lines provided)

**Current Status:** NEEDS FULL ANALYSIS
- File requires reading beyond line 100 to identify all exception throws
- Expected: DataRefreshException usage for state management failures

**Action Items:**
- [ ] Read complete file content
- [ ] Identify all generic exceptions thrown
- [ ] Map to DataRefreshException with appropriate ErrorCode values

---

## Priority 3: Consumer Layer Files

### 3.1 `/Users/anuragmishra/Desktop/workspace/messaging/provider/broker/src/main/java/com/messaging/broker/consumer/DeliveryStateStore.java`

**Generic Exceptions Thrown:**
- `IllegalStateException` (line 53)
- `IOException` (lines 52, 175, 190, 205)

**Conversion Details:**

| Line | Generic Exception | Count | Recommended Custom Exception | Notes |
|------|-------------------|-------|------------------------------|-------|
| 53 | IllegalStateException | 1 | ConsumerException or MessagingException | Failed to create data directory during init |
| 52 | IOException | 1 | ConsumerException | Wrapping IOException from createDirectories |
| 175 | IOException | 1 | ConsumerException | persistToDisk() failure |
| 190 | IOException | 1 | ConsumerException | loadFromDisk() failure |
| 205 | IOException | 1 | ConsumerException | Connection close failure (not thrown, just logged) |

**Action Items:**
- [ ] Convert IllegalStateException (line 53) to ConsumerException or use ErrorCode.CONSUMER_STORAGE_ERROR
- [ ] Convert IOException during directory creation to ConsumerException
- [ ] Convert IOException during persistence to ConsumerException with ErrorCode.CONSUMER_STATE_PERSISTENCE_FAILED
- [ ] Convert IOException during load to ConsumerException with ErrorCode.CONSUMER_STATE_LOAD_FAILED
- [ ] Total throw statements: 3

---

## Priority 4: Network Layer Files

### 4.1 `/Users/anuragmishra/Desktop/workspace/messaging/provider/network/src/main/java/com/messaging/network/codec/ZeroCopyBatchDecoder.java`

**Generic Exceptions Thrown:**
- `IllegalArgumentException` (line 79)
- `UnsupportedOperationException` (via Exception handling)

**Conversion Details:**

| Line | Generic Exception | Count | Recommended Custom Exception | Notes |
|------|-------------------|-------|------------------------------|-------|
| 79 | IllegalArgumentException | 1 | NetworkException | Invalid payload length in batch header |

**Action Items:**
- [ ] Convert IllegalArgumentException (line 79) to NetworkException with ErrorCode.INVALID_MESSAGE_FORMAT
- [ ] Total throw statements: 1

---

### 4.2 `/Users/anuragmishra/Desktop/workspace/messaging/provider/network/src/main/java/com/messaging/network/codec/BinaryMessageDecoder.java`

**Generic Exceptions Thrown:**
- `IllegalArgumentException` (line 49)

**Conversion Details:**

| Line | Generic Exception | Count | Recommended Custom Exception | Notes |
|------|-------------------|-------|------------------------------|-------|
| 49 | IllegalArgumentException | 1 | NetworkException | Invalid payload length during decoding |

**Action Items:**
- [ ] Convert IllegalArgumentException (line 49) to NetworkException with ErrorCode.INVALID_MESSAGE_FORMAT
- [ ] Total throw statements: 1

---

### 4.3 `/Users/anuragmishra/Desktop/workspace/messaging/provider/network/src/main/java/com/messaging/network/codec/BinaryMessageEncoder.java`

**Generic Exceptions Thrown:**
- `IllegalArgumentException` (line 22, 28, 35)

**Conversion Details:**

| Line | Generic Exception | Count | Recommended Custom Exception | Notes |
|------|-------------------|-------|------------------------------|-------|
| 22 | IllegalArgumentException | 1 | NetworkException | Null BrokerMessage validation |
| 28 | IllegalArgumentException | 1 | NetworkException | Null message type validation |
| 35 | IllegalArgumentException | 1 | NetworkException | Invalid type code validation |

**Action Items:**
- [ ] Convert all IllegalArgumentException to NetworkException with ErrorCode.INVALID_MESSAGE_FORMAT
- [ ] Total throw statements: 3

---

### 4.4 `/Users/anuragmishra/Desktop/workspace/messaging/provider/network/src/main/java/com/messaging/network/codec/JsonMessageDecoder.java`

**Generic Exceptions Thrown:**
- `IllegalArgumentException` (line 38)

**Conversion Details:**

| Line | Generic Exception | Count | Recommended Custom Exception | Notes |
|------|-------------------|-------|------------------------------|-------|
| 38 | IllegalArgumentException | 1 | NetworkException | Message too large validation |

**Action Items:**
- [ ] Convert IllegalArgumentException (line 38) to NetworkException with ErrorCode.MESSAGE_SIZE_EXCEEDED
- [ ] Total throw statements: 1

---

### 4.5 `/Users/anuragmishra/Desktop/workspace/messaging/provider/network/src/main/java/com/messaging/network/codec/JsonMessageEncoder.java`

**Generic Exceptions Thrown:**
- `IllegalArgumentException` (line 30, 36)

**Conversion Details:**

| Line | Generic Exception | Count | Recommended Custom Exception | Notes |
|------|-------------------|-------|------------------------------|-------|
| 30 | IllegalArgumentException | 1 | NetworkException | Null BrokerMessage validation |
| 36 | IllegalArgumentException | 1 | NetworkException | Null message type validation |

**Action Items:**
- [ ] Convert IllegalArgumentException (line 30) to NetworkException with ErrorCode.INVALID_MESSAGE_FORMAT
- [ ] Convert IllegalArgumentException (line 36) to NetworkException with ErrorCode.INVALID_MESSAGE_FORMAT
- [ ] Total throw statements: 2

---

### 4.6 `/Users/anuragmishra/Desktop/workspace/messaging/provider/network/src/main/java/com/messaging/network/tcp/NettyTcpServer.java`

**Generic Exceptions Thrown:**
- `RuntimeException` (line 114)
- `InterruptedException` (line 111)

**Conversion Details:**

| Line | Generic Exception | Count | Recommended Custom Exception | Notes |
|------|-------------------|-------|------------------------------|-------|
| 114 | RuntimeException | 1 | NetworkException | Server startup failure |
| 111 | InterruptedException | 1 | NetworkException | Server startup interrupted |

**Action Items:**
- [ ] Convert RuntimeException (line 114) to NetworkException with ErrorCode.SERVER_START_FAILED
- [ ] Convert InterruptedException (line 111) to NetworkException with ErrorCode.SERVER_INTERRUPTED
- [ ] Total throw statements: 2

---

## Priority 5: Broker Core Files

### 5.1 `/Users/anuragmishra/Desktop/workspace/messaging/provider/broker/src/main/java/com/messaging/broker/registry/CloudRegistryClient.java`

**Generic Exceptions Thrown:**
- `RuntimeException` (line 58, 69)

**Conversion Details:**

| Line | Generic Exception | Count | Recommended Custom Exception | Notes |
|------|-------------------|-------|------------------------------|-------|
| 58 | RuntimeException | 1 | MessagingException or RegistryException | Failed to query Cloud Registry (non-200 status) |
| 69 | RuntimeException | 1 | MessagingException or RegistryException | Failed to query Cloud Registry (exception during processing) |

**Action Items:**
- [ ] Convert RuntimeException (line 58) to MessagingException with ErrorCode.REGISTRY_QUERY_FAILED
- [ ] Convert RuntimeException (line 69) to MessagingException with ErrorCode.REGISTRY_QUERY_FAILED
- [ ] Total throw statements: 2

---

### 5.2 `/Users/anuragmishra/Desktop/workspace/messaging/provider/broker/src/main/java/com/messaging/broker/registry/TopologyPropertiesStore.java`

**Generic Exceptions Thrown:**
- `RuntimeException` (line 68)
- `IOException` (lines 59, 66, 82, 88)

**Conversion Details:**

| Line | Generic Exception | Count | Recommended Custom Exception | Notes |
|------|-------------------|-------|------------------------------|-------|
| 68 | RuntimeException | 1 | MessagingException | Failed to save topology properties (wrapping IOException) |
| 59 | IOException | 1 | MessagingException | FileWriter creation/write failure |
| 66 | IOException | 1 | MessagingException | Caught but not rethrown (just logged) |
| 82 | IOException | 1 | MessagingException | FileReader creation/load failure |
| 88 | IOException | 1 | MessagingException | Caught but not rethrown (just logged) |

**Action Items:**
- [ ] Convert RuntimeException (line 68) to MessagingException with ErrorCode.TOPOLOGY_SAVE_FAILED
- [ ] Convert IOException (line 59) to MessagingException with ErrorCode.TOPOLOGY_SAVE_FAILED
- [ ] Lines 66, 82, 88: IOException is caught and logged, returns null - already handled gracefully
- [ ] Total throw statements: 2

---

### 5.3 `/Users/anuragmishra/Desktop/workspace/messaging/provider/broker/src/main/java/com/messaging/broker/pipe/PipeMessageForwarder.java`

**Generic Exceptions Thrown:**
- `RuntimeException` (line 38)

**Conversion Details:**

| Line | Generic Exception | Count | Recommended Custom Exception | Notes |
|------|-------------------|-------|------------------------------|-------|
| 38 | RuntimeException | 1 | MessagingException | Failed to read messages for child broker |

**Action Items:**
- [ ] Convert RuntimeException (line 38) to MessagingException with ErrorCode.PIPE_MESSAGE_READ_FAILED
- [ ] Total throw statements: 1

---

## Priority 6: Pipe Layer Files

### 6.1 `/Users/anuragmishra/Desktop/workspace/messaging/provider/pipe/src/main/java/com/messaging/pipe/HttpPipeConnector.java`

**Generic Exceptions Thrown:**
- `IllegalStateException` (line 75)
- `IOException` (line 74)

**Conversion Details:**

| Line | Generic Exception | Count | Recommended Custom Exception | Notes |
|------|-------------------|-------|------------------------------|-------|
| 75 | IllegalStateException | 1 | MessagingException | Failed to create data directory |
| 74 | IOException | 1 | MessagingException | Wrapping IOException from directory creation |

**Action Items:**
- [ ] Convert IllegalStateException (line 75) to MessagingException with ErrorCode.PIPE_INITIALIZATION_FAILED
- [ ] Total throw statements: 1

---

## Priority 7: Common Model Files

### 7.1 `/Users/anuragmishra/Desktop/workspace/messaging/provider/common/src/main/java/com/messaging/common/model/EventType.java`

**Generic Exceptions Thrown:**
- `IllegalArgumentException` (line 33)

**Conversion Details:**

| Line | Generic Exception | Count | Recommended Custom Exception | Notes |
|------|-------------------|-------|------------------------------|-------|
| 33 | IllegalArgumentException | 1 | MessagingException | Unknown event type code during deserialization |

**Action Items:**
- [ ] Convert IllegalArgumentException (line 33) to MessagingException with ErrorCode.INVALID_EVENT_TYPE
- [ ] Total throw statements: 1

---

### 7.2 `/Users/anuragmishra/Desktop/workspace/messaging/provider/common/src/main/java/com/messaging/common/model/BrokerMessage.java`

**Generic Exceptions Thrown:**
- None found (within first 50 lines - file may have more content)

**Status:** No action required for scope provided

---

## Summary Statistics

### Total Exception Conversions Required: 35

| Layer | File Count | Generic Exceptions | Total Throw Statements |
|-------|-----------|-------------------|------------------------|
| **Priority 1: Storage** | 4 | 4 types | 15 |
| **Priority 2: DataRefresh** | 1 | TBD | TBD |
| **Priority 3: Consumer** | 1 | 2 types | 3 |
| **Priority 4: Network** | 6 | 1 type | 10 |
| **Priority 5: Broker Core** | 3 | 2 types | 5 |
| **Priority 6: Pipe** | 1 | 2 types | 1 |
| **Priority 7: Common Model** | 1 | 1 type | 1 |
| **TOTAL** | **17** | **Multiple** | **35+** |

### Generic Exception Type Distribution

- **IOException**: 14 occurrences (Storage, Consumer, Broker, Pipe)
- **RuntimeException**: 10 occurrences (Storage, Network, Broker)
- **IllegalArgumentException**: 8 occurrences (Storage, Network, Common)
- **IllegalStateException**: 3 occurrences (Storage, Consumer, Pipe)
- **UnsupportedOperationException**: 2 occurrences (Storage)
- **InterruptedException**: 1 occurrence (Network)

### Custom Exception Type Mapping

- **StorageException**: 15-17 conversions needed
- **NetworkException**: 10 conversions needed
- **ConsumerException**: 3 conversions needed
- **MessagingException**: 5-6 conversions needed
- **DataRefreshException**: TBD (needs full DataRefreshManager analysis)

---

## Recommended Conversion Approach

### Phase 1: Storage Layer (HIGHEST PRIORITY)
1. Update SegmentManager.java (4 conversions)
2. Update Segment.java (4 conversions)
3. Update MMapStorageEngine.java (3 conversions)
4. Update SegmentMetadataStore.java (4 conversions)
- **Total Phase 1: 15 conversions**

### Phase 2: Network Layer (HIGH PRIORITY)
1. Update all codec files (BinaryMessageDecoder, BinaryMessageEncoder, JsonMessageDecoder, JsonMessageEncoder, ZeroCopyBatchDecoder)
2. Update NettyTcpServer.java
- **Total Phase 2: 10 conversions**

### Phase 3: Consumer & Registry Layer (MEDIUM PRIORITY)
1. Update DeliveryStateStore.java (3 conversions)
2. Update CloudRegistryClient.java (2 conversions)
3. Update TopologyPropertiesStore.java (2 conversions)
- **Total Phase 3: 7 conversions**

### Phase 4: Pipe & Common Layer (MEDIUM PRIORITY)
1. Update HttpPipeConnector.java (1 conversion)
2. Update PipeMessageForwarder.java (1 conversion)
3. Update EventType.java (1 conversion)
- **Total Phase 4: 3 conversions**

### Phase 5: DataRefresh Layer (DEPENDS ON FULL ANALYSIS)
1. Complete analysis of DataRefreshManager.java
2. Identify and convert all exceptions to DataRefreshException
- **Total Phase 5: TBD**

---

## Exception Framework Requirements

### ErrorCode Values Needed for Each Layer

#### StorageException
- `STORAGE_IO_ERROR`
- `STORAGE_READ_ERROR`
- `STORAGE_WRITE_ERROR`
- `STORAGE_RECOVERY_FAILED`
- `STORAGE_INITIALIZATION_FAILED`
- `STORAGE_METADATA_ERROR`
- `STORAGE_CORRUPTION`
- `INVALID_SEGMENT_NAME`
- `INVALID_FORMAT`
- `INVALID_STATE`
- `UNSUPPORTED_OPERATION`
- `CRC_MISMATCH`

#### NetworkException
- `INVALID_MESSAGE_FORMAT`
- `MESSAGE_SIZE_EXCEEDED`
- `SERVER_START_FAILED`
- `SERVER_INTERRUPTED`
- `CONNECTION_FAILED`

#### ConsumerException
- `CONSUMER_STORAGE_ERROR`
- `CONSUMER_STATE_PERSISTENCE_FAILED`
- `CONSUMER_STATE_LOAD_FAILED`

#### MessagingException
- `REGISTRY_QUERY_FAILED`
- `TOPOLOGY_SAVE_FAILED`
- `PIPE_MESSAGE_READ_FAILED`
- `PIPE_INITIALIZATION_FAILED`
- `INVALID_EVENT_TYPE`

#### DataRefreshException
- (To be determined after complete file analysis)

---

## Notes for Implementation

1. **Backward Compatibility**: All custom exceptions should extend the appropriate base exception type (MessagingException hierarchy)
2. **Chaining**: All conversions should preserve the original exception as the cause using `.initCause()` or constructor parameters
3. **Logging**: Use ExceptionLogger framework already in use (see Segment.java pattern)
4. **Context Information**: Include relevant context in custom exceptions (topic, partition, offset, etc.)
5. **Error Codes**: Ensure ErrorCode enum is extended with all new values before conversions
6. **Testing**: Each conversion should have corresponding unit tests verifying the exception type and error code

---

## Next Steps

1. **Verify DataRefreshManager.java** - Read complete file to identify all exception conversions
2. **Create/Update ErrorCode Enum** - Add all missing error codes identified in this inventory
3. **Create Exception Classes** - Ensure all custom exception types exist and properly extend MessagingException
4. **Execute Phase 1-5 Conversions** - Follow the recommended conversion order
5. **Verification** - Run full test suite after each phase to ensure backward compatibility

