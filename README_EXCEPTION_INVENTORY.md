# Exception Conversion Inventory - Complete Reference

## Quick Start

This directory contains a comprehensive inventory of all Java files that throw generic exceptions and need to be converted to custom exceptions.

### Three Main Documents:

1. **EXCEPTION_INVENTORY.md** (502 lines)
   - Detailed analysis of all 17 files
   - Organized by priority and layer
   - Includes conversion rationale and context
   - **Start here for understanding the scope**

2. **EXCEPTION_INVENTORY_SUMMARY.txt** (279 lines)
   - Quick reference with file paths and line numbers
   - One-page overview of each conversion
   - Checklist format
   - **Use this for quick lookups during implementation**

3. **EXCEPTION_CONVERSION_MAPPING.txt** (630 lines)
   - Detailed line-by-line conversion guide
   - Current code vs. target code
   - Implementation priority and impact assessment
   - **Reference this while making actual code changes**

---

## Executive Summary

### Scope
- **Files to convert**: 17 Java files
- **Throw statements**: 35+ (excluding already-converted code)
- **Estimated effort**: 2-3 days
- **Risk level**: Medium (mostly validation/error handling)

### Distribution by Layer

| Layer | Files | Throw Statements | Status |
|-------|-------|------------------|--------|
| Storage | 4 | 15 | 2 files already done ✓ |
| Network | 6 | 10 | All pending |
| Consumer | 1 | 3 | All pending |
| Broker Core | 3 | 5 | All pending |
| Pipe | 1 | 1 | All pending |
| Common | 1 | 1 | All pending |
| DataRefresh | 1 | TBD | Needs full analysis |
| **TOTAL** | **17** | **35+** | 2 complete |

### Exception Type Mapping

```
IOException              14 → StorageException / MessagingException / ConsumerException
RuntimeException         10 → StorageException / NetworkException / MessagingException
IllegalArgumentException  8 → NetworkException / StorageException / MessagingException
IllegalStateException     3 → StorageException / ConsumerException / MessagingException
UnsupportedOperationException 2 → StorageException
InterruptedException      1 → NetworkException
```

### Priority Order

1. **PRIORITY 1: Storage Layer** (15 conversions)
   - Critical path for all I/O operations
   - Status: 2 files already converted
   - Action: Convert remaining 2 files

2. **PRIORITY 2: Network Layer** (10 conversions)
   - High impact on message protocol
   - All 6 codec/server files need conversion
   - Status: All pending

3. **PRIORITY 3: Consumer & Registry** (7 conversions)
   - Medium priority
   - Consumer state management and topology
   - Status: All pending

4. **PRIORITY 4: Pipe & Common** (2 conversions)
   - Low priority, isolated components
   - Status: All pending

5. **PRIORITY 5: DataRefresh** (TBD)
   - Requires additional analysis
   - Depends on DataRefreshManager.java full review

---

## File Locations

### Storage Layer
- `/Users/anuragmishra/Desktop/workspace/messaging/provider/storage/src/main/java/com/messaging/storage/segment/SegmentManager.java`
- `/Users/anuragmishra/Desktop/workspace/messaging/provider/storage/src/main/java/com/messaging/storage/segment/Segment.java`
- `/Users/anuragmishra/Desktop/workspace/messaging/provider/storage/src/main/java/com/messaging/storage/mmap/MMapStorageEngine.java`
- `/Users/anuragmishra/Desktop/workspace/messaging/provider/storage/src/main/java/com/messaging/storage/metadata/SegmentMetadataStore.java`

### Network Layer
- `/Users/anuragmishra/Desktop/workspace/messaging/provider/network/src/main/java/com/messaging/network/codec/BinaryMessageDecoder.java`
- `/Users/anuragmishra/Desktop/workspace/messaging/provider/network/src/main/java/com/messaging/network/codec/BinaryMessageEncoder.java`
- `/Users/anuragmishra/Desktop/workspace/messaging/provider/network/src/main/java/com/messaging/network/codec/JsonMessageDecoder.java`
- `/Users/anuragmishra/Desktop/workspace/messaging/provider/network/src/main/java/com/messaging/network/codec/JsonMessageEncoder.java`
- `/Users/anuragmishra/Desktop/workspace/messaging/provider/network/src/main/java/com/messaging/network/codec/ZeroCopyBatchDecoder.java`
- `/Users/anuragmishra/Desktop/workspace/messaging/provider/network/src/main/java/com/messaging/network/tcp/NettyTcpServer.java`

### Consumer & Registry
- `/Users/anuragmishra/Desktop/workspace/messaging/provider/broker/src/main/java/com/messaging/broker/consumer/DeliveryStateStore.java`
- `/Users/anuragmishra/Desktop/workspace/messaging/provider/broker/src/main/java/com/messaging/broker/registry/CloudRegistryClient.java`
- `/Users/anuragmishra/Desktop/workspace/messaging/provider/broker/src/main/java/com/messaging/broker/registry/TopologyPropertiesStore.java`

### Pipe & Core
- `/Users/anuragmishra/Desktop/workspace/messaging/provider/pipe/src/main/java/com/messaging/pipe/HttpPipeConnector.java`
- `/Users/anuragmishra/Desktop/workspace/messaging/provider/broker/src/main/java/com/messaging/broker/pipe/PipeMessageForwarder.java`
- `/Users/anuragmishra/Desktop/workspace/messaging/provider/common/src/main/java/com/messaging/common/model/EventType.java`

### Data Refresh
- `/Users/anuragmishra/Desktop/workspace/messaging/provider/broker/src/main/java/com/messaging/broker/refresh/DataRefreshManager.java`

---

## How to Use These Documents

### For Project Managers
1. Read the Executive Summary (above)
2. Reference the **EXCEPTION_INVENTORY_SUMMARY.txt** for status tracking
3. Use the Priority Order to plan sprints

### For Development
1. Start with **EXCEPTION_INVENTORY.md** for detailed context
2. Reference **EXCEPTION_CONVERSION_MAPPING.txt** while coding
3. Check each file location to understand current implementation
4. Use the "Change To" sections for exact replacement code

### For Code Review
1. Verify each conversion matches the mapping document
2. Ensure exception chaining is preserved (original exception as cause)
3. Check context information is included (topic, partition, offset, path, etc.)
4. Validate ErrorCode values are used correctly
5. Confirm logging still happens (via ExceptionLogger or log statements)

---

## Already Completed

### Storage Layer - Segment.java
Lines 114, 320, 450, 671, 838 - Already use StorageException with ExceptionLogger

### Storage Layer - SegmentManager.java
Lines 57, 88 - Already use StorageException

**Status**: These files demonstrate the desired pattern. Follow their implementation style for other conversions.

---

## Implementation Checklist

### Pre-Implementation
- [ ] Ensure all custom exception classes exist (StorageException, NetworkException, etc.)
- [ ] Verify ErrorCode enum has all required values
- [ ] Review existing implementations (Segment.java, SegmentManager.java) for pattern consistency

### Phase 1: Storage Layer (4 files)
- [ ] SegmentManager.java (4 conversions - 2 done, 2 remaining)
- [ ] Segment.java (4 conversions - 4 done, 0 remaining) ✓
- [ ] MMapStorageEngine.java (3 conversions)
- [ ] SegmentMetadataStore.java (4 conversions)
- [ ] Run storage tests
- [ ] Verify no regression in segment operations

### Phase 2: Network Layer (6 files)
- [ ] BinaryMessageDecoder.java (1 conversion)
- [ ] BinaryMessageEncoder.java (3 conversions)
- [ ] JsonMessageDecoder.java (1 conversion)
- [ ] JsonMessageEncoder.java (2 conversions)
- [ ] ZeroCopyBatchDecoder.java (1 conversion)
- [ ] NettyTcpServer.java (2 conversions)
- [ ] Run network tests
- [ ] Verify protocol handshakes

### Phase 3: Consumer & Registry (3 files)
- [ ] DeliveryStateStore.java (2 conversions)
- [ ] CloudRegistryClient.java (2 conversions)
- [ ] TopologyPropertiesStore.java (1 conversion)
- [ ] Run topology and consumer tests

### Phase 4: Pipe & Common (2 files)
- [ ] HttpPipeConnector.java (1 conversion)
- [ ] PipeMessageForwarder.java (1 conversion)
- [ ] EventType.java (1 conversion)
- [ ] Run pipe tests

### Phase 5: DataRefresh (1 file)
- [ ] Complete analysis of DataRefreshManager.java
- [ ] Identify and convert all exceptions
- [ ] Run data refresh tests

### Post-Implementation
- [ ] Run full test suite
- [ ] Check for any compilation errors
- [ ] Verify no breaking changes to public APIs
- [ ] Update any affected documentation
- [ ] Create PR with detailed description
- [ ] Review against ExceptionLogger pattern

---

## Custom Exception Classes Required

### StorageException
- Used by: Storage layer (SegmentManager, Segment, MMapStorageEngine, SegmentMetadataStore)
- ErrorCodes needed: STORAGE_IO_ERROR, STORAGE_READ_ERROR, STORAGE_WRITE_ERROR, STORAGE_RECOVERY_FAILED, STORAGE_INITIALIZATION_FAILED, STORAGE_METADATA_ERROR, STORAGE_CORRUPTION, INVALID_FORMAT, INVALID_STATE, UNSUPPORTED_OPERATION, CRC_MISMATCH

### NetworkException
- Used by: Network layer (codecs, NettyTcpServer)
- ErrorCodes needed: INVALID_MESSAGE_FORMAT, MESSAGE_SIZE_EXCEEDED, SERVER_START_FAILED, SERVER_INTERRUPTED

### ConsumerException
- Used by: Consumer layer (DeliveryStateStore)
- ErrorCodes needed: CONSUMER_STORAGE_ERROR, CONSUMER_STATE_PERSISTENCE_FAILED, CONSUMER_STATE_LOAD_FAILED

### MessagingException
- Used by: Broker core, Pipe, Common models, Registry
- ErrorCodes needed: REGISTRY_QUERY_FAILED, TOPOLOGY_SAVE_FAILED, PIPE_MESSAGE_READ_FAILED, PIPE_INITIALIZATION_FAILED, INVALID_EVENT_TYPE

### DataRefreshException
- Used by: DataRefresh layer
- ErrorCodes: TBD (pending full analysis)

---

## Implementation Notes

### Exception Chaining
Always preserve the original exception:
```java
// GOOD
throw new StorageException(ErrorCode.STORAGE_IO_ERROR, "Failed to...", e);

// BAD - loses original exception
throw new StorageException(ErrorCode.STORAGE_IO_ERROR, "Failed to...", null);
```

### Context Information
Include relevant context in custom exceptions:
```java
// GOOD
throw new StorageException(ErrorCode.STORAGE_METADATA_ERROR,
    "Failed to save segment for topic=" + topic + ", partition=" + partition, e)
    .withTopic(topic)
    .withPartition(partition)
    .withSegmentPath(logPath.toString());

// LESS GOOD - missing context
throw new StorageException(ErrorCode.STORAGE_METADATA_ERROR, "Failed to save segment", e);
```

### Logging Pattern
Follow the ExceptionLogger pattern already in use:
```java
// Pattern from Segment.java
throw ExceptionLogger.logAndThrow(log,
    StorageException.ioError("Error message", e)
        .withTopic(topic)
        .withPartition(partition)
        .withSegmentPath(logPath.toString()));
```

### Validation Exceptions
For validation failures, use the ErrorCode that best describes the validation:
```java
// GOOD - specific error type
throw new NetworkException(ErrorCode.INVALID_MESSAGE_FORMAT,
    "Invalid payload length: " + payloadLength, null);

// LESS GOOD - generic error
throw new NetworkException(ErrorCode.NETWORK_ERROR,
    "Invalid payload length: " + payloadLength, null);
```

---

## Testing Strategy

### Unit Tests
Each converted method should have tests for:
1. Normal operation (no exception)
2. Error case (exception thrown)
3. Verify exception type is correct
4. Verify ErrorCode is set correctly
5. Verify context information is present

### Integration Tests
After each phase:
1. Run layer-specific tests
2. Verify error handling paths
3. Check that exceptions propagate correctly
4. Validate recovery mechanisms still work

### Regression Tests
Before committing:
1. Run full test suite
2. Check no new failures introduced
3. Verify existing error handling still works
4. Confirm backward compatibility

---

## Common Pitfalls to Avoid

1. **Losing exception chain**: Always pass original exception as cause
2. **Missing context**: Include all relevant info (topic, partition, offset, path, etc.)
3. **Wrong ErrorCode**: Use the most specific ErrorCode, not generic INTERNAL_ERROR
4. **Inconsistent patterns**: Follow ExceptionLogger pattern from Segment.java
5. **Incomplete logging**: Don't remove log statements when converting exceptions
6. **Changing behavior**: Conversions should not change the execution flow
7. **Forgetting imports**: Add imports for custom exception classes
8. **Missing documentation**: Update javadoc for changed exception types

---

## References

### Files with Correct Implementation Patterns
- `/Users/anuragmishra/Desktop/workspace/messaging/provider/storage/src/main/java/com/messaging/storage/segment/Segment.java` - Lines 114-120, 320-327, 838-843
- `/Users/anuragmishra/Desktop/workspace/messaging/provider/storage/src/main/java/com/messaging/storage/segment/SegmentManager.java` - Lines 57, 88

### Exception Framework Classes
- `com.messaging.common.exception.StorageException`
- `com.messaging.common.exception.NetworkException`
- `com.messaging.common.exception.ConsumerException`
- `com.messaging.common.exception.MessagingException`
- `com.messaging.common.exception.ErrorCode`
- `com.messaging.common.exception.ExceptionLogger`

### Related Documentation
- CLAUDE.md - System architecture and guidelines
- Build instructions in CLAUDE.md for testing changes

---

## Support & Questions

For questions about this inventory:
1. Check the detailed analysis in EXCEPTION_INVENTORY.md
2. Review the line-by-line mappings in EXCEPTION_CONVERSION_MAPPING.txt
3. Reference the implementation patterns in Segment.java and SegmentManager.java
4. Consult CLAUDE.md for architecture context

---

## Document Version

- Created: 2026-02-15
- Status: Complete
- Completeness: 94% (DataRefresh layer pending full analysis)
- Last Updated: 2026-02-15

**Total Documentation**: 1,411 lines across 4 documents
- EXCEPTION_INVENTORY.md: 502 lines (detailed analysis)
- EXCEPTION_INVENTORY_SUMMARY.txt: 279 lines (quick reference)
- EXCEPTION_CONVERSION_MAPPING.txt: 630 lines (line-by-line guide)
- README_EXCEPTION_INVENTORY.md: This file

