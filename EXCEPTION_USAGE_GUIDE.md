# Custom Exception Framework - Usage Guide

This guide explains how to use the custom exception framework integrated into the messaging system.

## Table of Contents
1. [Overview](#overview)
2. [Exception Hierarchy](#exception-hierarchy)
3. [Quick Start Examples](#quick-start-examples)
4. [Error Codes](#error-codes)
5. [Best Practices](#best-practices)
6. [Integration Guide](#integration-guide)

## Overview

The custom exception framework provides:
- **Structured exceptions** with error codes and rich context
- **Category-based organization** (Storage, Network, Consumer, DataRefresh, etc.)
- **Retriability indicators** (auto-retry vs manual intervention)
- **Contextual information** for debugging
- **Structured logging** for monitoring and alerting

## Exception Hierarchy

```
MessagingException (base)
├── StorageException
├── NetworkException
├── ConsumerException
└── DataRefreshException
```

## Quick Start Examples

### Example 1: Storage Layer - CRC Validation Failure

```java
import com.messaging.common.exception.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class Segment {
    private static final Logger log = LoggerFactory.getLogger(Segment.class);

    public MessageRecord readRecordAtOffset(long offset) throws StorageException {
        try {
            // Read from storage
            ByteBuffer data = readFromLog(offset);

            // Validate CRC
            if (!validateCRC(data)) {
                throw StorageException.crcMismatch(
                    "CRC32 validation failed for offset " + offset,
                    offset)
                    .withTopic(topic)
                    .withPartition(partition)
                    .withSegmentPath(logPath.toString());
            }

            return parseRecord(data);
        } catch (IOException e) {
            throw ExceptionLogger.logAndThrow(log,
                StorageException.readFailed(topic, partition, offset, e));
        }
    }
}
```

**Logged output:**
```
ERROR [STORAGE_CRC_MISMATCH] category=STORAGE, code=1006, retriable=false, timestamp=2026-02-11T10:30:45Z, message=CRC32 validation failed for offset 12345, context={topic=prices-v1, partition=0, offset=12345, segmentPath=/data/prices-v1/partition-0/segment.log}
```

### Example 2: Consumer - Batch Delivery Failure

```java
import com.messaging.common.exception.*;

public class ConsumerDeliveryManager {
    private static final Logger log = LoggerFactory.getLogger(ConsumerDeliveryManager.class);

    public void deliverBatch(String consumerId, String topic, List<MessageRecord> batch) {
        try {
            networkClient.send(consumerId, batch);
        } catch (IOException e) {
            ConsumerException ex = ConsumerException.batchDeliveryFailed(
                consumerId, topic, batch.size(), e)
                .withContext("retryAttempt", retryCount)
                .withContext("batchStartOffset", batch.get(0).getOffset());

            // Log conditionally based on retriability
            ExceptionLogger.logConditional(log, ex);

            // Retry logic for retriable errors
            if (ex.isRetriable() && retryCount < MAX_RETRIES) {
                retryDelivery(consumerId, topic, batch, retryCount + 1);
            } else {
                throw ex;
            }
        }
    }
}
```

### Example 3: Data Refresh - Timeout Handling

```java
import com.messaging.common.exception.*;

public class DataRefreshManager {
    private static final Logger log = LoggerFactory.getLogger(DataRefreshManager.class);

    public void waitForConsumersReady(String refreshId, String topic, long timeoutSeconds)
            throws DataRefreshException {
        long startTime = System.currentTimeMillis();

        while (!allConsumersReady(topic)) {
            long elapsedSeconds = (System.currentTimeMillis() - startTime) / 1000;

            if (elapsedSeconds > timeoutSeconds) {
                throw ExceptionLogger.logAndThrow(log,
                    DataRefreshException.timeout(refreshId, topic, timeoutSeconds)
                        .withContext("expectedConsumers", expectedConsumers.size())
                        .withContext("readyConsumers", readyConsumers.size()));
            }

            Thread.sleep(100);
        }
    }
}
```

### Example 4: Network Layer - Connection Failure with Retry

```java
import com.messaging.common.exception.*;

public class NettyTcpClient {
    private static final Logger log = LoggerFactory.getLogger(NettyTcpClient.class);

    public void connect(String host, int port) throws NetworkException {
        int attempt = 0;

        while (attempt < MAX_RETRIES) {
            try {
                bootstrap.connect(host, port).sync();
                log.info("Connected to {}:{}", host, port);
                return;
            } catch (Exception e) {
                attempt++;

                NetworkException ex = NetworkException.connectionFailed(host, port, e)
                    .withContext("attemptNumber", attempt)
                    .withContext("maxRetries", MAX_RETRIES);

                if (attempt >= MAX_RETRIES) {
                    throw ExceptionLogger.logAndThrow(log, ex);
                }

                ExceptionLogger.logWarn(log, ex);
                Thread.sleep(RETRY_DELAY_MS * attempt); // Exponential backoff
            }
        }
    }
}
```

## Error Codes

### Storage Errors (1xxx)
- `STORAGE_IO_ERROR` (1001) - I/O error during storage operation [retriable]
- `STORAGE_CORRUPTION` (1002) - Storage data corruption detected
- `STORAGE_CRC_MISMATCH` (1006) - CRC32 validation failed
- `STORAGE_OFFSET_OUT_OF_RANGE` (1007) - Requested offset out of range
- `STORAGE_WRITE_FAILED` (1008) - Failed to write to storage [retriable]
- `STORAGE_READ_FAILED` (1009) - Failed to read from storage [retriable]

### Network Errors (2xxx)
- `NETWORK_CONNECTION_FAILED` (2001) - Failed to establish connection [retriable]
- `NETWORK_SEND_FAILED` (2003) - Failed to send message [retriable]
- `NETWORK_TIMEOUT` (2005) - Network operation timed out [retriable]
- `NETWORK_ENCODING_ERROR` (2006) - Message encoding failed
- `NETWORK_DECODING_ERROR` (2007) - Message decoding failed

### Consumer Errors (3xxx)
- `CONSUMER_NOT_REGISTERED` (3001) - Consumer not registered
- `CONSUMER_DISCONNECTED` (3003) - Consumer disconnected [retriable]
- `CONSUMER_OFFSET_COMMIT_FAILED` (3005) - Offset commit failed [retriable]
- `CONSUMER_BATCH_DELIVERY_FAILED` (3007) - Batch delivery failed [retriable]

### Data Refresh Errors (4xxx)
- `DATA_REFRESH_ALREADY_IN_PROGRESS` (4001) - Data refresh already in progress
- `DATA_REFRESH_TIMEOUT` (4002) - Data refresh operation timed out
- `DATA_REFRESH_CONSUMER_NOT_READY` (4003) - Consumer not ready for data refresh
- `DATA_REFRESH_REPLAY_FAILED` (4005) - Message replay failed [retriable]

## Best Practices

### 1. Always Add Context

```java
// GOOD
throw StorageException.readFailed(topic, partition, offset, cause)
    .withContext("segmentPath", segmentPath)
    .withContext("filePosition", filePosition);

// BAD
throw new StorageException(ErrorCode.STORAGE_READ_FAILED, "Read failed", cause);
```

### 2. Use Factory Methods

```java
// GOOD - Uses factory method with proper context
throw StorageException.offsetOutOfRange(topic, partition, offset, minOffset, maxOffset);

// BAD - Manual construction, missing context
throw new StorageException(ErrorCode.STORAGE_OFFSET_OUT_OF_RANGE,
    "Offset out of range");
```

### 3. Use ExceptionLogger for Consistent Logging

```java
// GOOD - Structured logging
ExceptionLogger.logError(log, ex);

// BAD - Manual logging loses context
log.error("Error occurred: {}", ex.getMessage(), ex);
```

### 4. Check Retriability Before Retry

```java
catch (MessagingException ex) {
    if (ex.isRetriable() && retryCount < MAX_RETRIES) {
        retry();
    } else {
        throw ex; // Fatal error, don't retry
    }
}
```

### 5. Use Conditional Logging

```java
// Automatically logs WARN for retriable, ERROR for non-retriable
ExceptionLogger.logConditional(log, ex);
```

## Integration Guide

### Step 1: Add Exception Handling to Existing Code

Replace generic exceptions:
```java
// BEFORE
catch (IOException e) {
    log.error("Storage read failed", e);
    throw new RuntimeException(e);
}

// AFTER
catch (IOException e) {
    throw ExceptionLogger.logAndThrow(log,
        StorageException.readFailed(topic, partition, offset, e));
}
```

### Step 2: Update Method Signatures

```java
// BEFORE
public MessageRecord read(long offset) throws IOException

// AFTER
public MessageRecord read(long offset) throws StorageException
```

### Step 3: Add Monitoring Metrics (Future Enhancement)

```java
// Example: Count exceptions by category and code
Counter exceptionCounter = Counter.builder("messaging.exceptions.total")
    .tag("category", ex.getCategory())
    .tag("code", String.valueOf(ex.getCode()))
    .tag("retriable", String.valueOf(ex.isRetriable()))
    .register(meterRegistry);

exceptionCounter.increment();
```

## Log Parsing for Monitoring

The structured format allows easy parsing:

```bash
# Find all non-retriable errors
grep "retriable=false" broker.log

# Find all storage errors
grep "category=STORAGE" broker.log

# Find specific error code
grep "code=1006" broker.log | grep "STORAGE_CRC_MISMATCH"

# Extract context for specific error
grep "STORAGE_OFFSET_OUT_OF_RANGE" broker.log | grep -o "context={[^}]*}"
```

## Summary

- **Use specific exception types** (StorageException, NetworkException, etc.)
- **Always add context** using `.withContext()` or `.withTopic()`, `.withPartition()`, etc.
- **Use factory methods** for common error scenarios
- **Check `isRetriable()`** before implementing retry logic
- **Use ExceptionLogger** for consistent structured logging
- **Monitor exception metrics** by category, code, and retriability

The framework makes it easy to identify "what went wrong" at a glance, enabling faster debugging and better operational visibility.
