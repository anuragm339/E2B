# Structured Logging Guide

This guide demonstrates how to use the structured logging framework in the broker.

## Overview

The structured logging layer provides semantic events with consistent context to replace ad-hoc log statements.

**Benefits:**
- Consistent log format across all components
- Easy to parse and search programmatically
- Rich contextual information (topic, consumer, refresh ID, etc.)
- Type-safe semantic methods instead of string concatenation

## Components

### 1. LogContext
Builder for structured log context:

```java
LogContext context = LogContext.builder()
    .topic("prices-v1")
    .clientId("client-123")
    .consumerGroup("price-quote-group")
    .offset(100L)
    .custom("errorCode", "ERR_001")
    .build();
```

### 2. Event Logger Interfaces
- `BrokerEventLogger` - Broker lifecycle events
- `ConsumerEventLogger` - Consumer registration/delivery events
- `RefreshEventLogger` - Refresh workflow events

### 3. Default Implementations
- `DefaultBrokerEventLogger`
- `DefaultConsumerEventLogger`
- `DefaultRefreshEventLogger`

## Usage Examples

### Before (Ad-hoc Logging)

```java
log.info("Consumer registered: clientId={}, topic={}, group={}, offset={}",
        clientId, topic, group, offset);

log.warn("Received RESET ACK from {} for topic {} but no active refresh",
        consumerGroupTopic, topic);
```

### After (Structured Logging)

```java
// Inject event logger
@Inject
private ConsumerEventLogger consumerLogger;

// Log with context
LogContext context = LogContext.builder()
    .clientId(clientId)
    .topic(topic)
    .consumerGroup(group)
    .offset(offset)
    .build();

consumerLogger.logConsumerRegistered(context);

// Refresh events
LogContext refreshContext = LogContext.builder()
    .topic(topic)
    .refreshId(refreshId)
    .custom("consumer", consumerGroupTopic)
    .build();

refreshLogger.logResetAckReceived(refreshContext);
```

## Log Output Format

Structured logs use consistent prefixes:

```
[BROKER] Initialized
[CONSUMER] Registered: topic=prices-v1, clientId=client-123, consumerGroup=price-quote-group, offset=100
[REFRESH] Started: topic=prices-v1, refreshId=1234567890, consumerCount=3
[DELIVERY] Batch succeeded: topic=prices-v1, clientId=client-123, messageCount=50, durationMs=12
```

## Migration Strategy

The framework is available incrementally. Full migration is optional:

1. New code SHOULD use structured logging
2. Existing code CAN be migrated incrementally
3. Both styles can coexist during transition

## Example: Full Migration Pattern

```java
@Singleton
public class MyService {
    private final ConsumerEventLogger consumerLogger;
    private final RefreshEventLogger refreshLogger;

    @Inject
    public MyService(
            ConsumerEventLogger consumerLogger,
            RefreshEventLogger refreshLogger) {
        this.consumerLogger = consumerLogger;
        this.refreshLogger = refreshLogger;
    }

    public void registerConsumer(String clientId, String topic, String group) {
        // Build context once, reuse for multiple log statements
        LogContext context = LogContext.builder()
            .clientId(clientId)
            .topic(topic)
            .consumerGroup(group)
            .build();

        // Use semantic methods instead of log.info()
        consumerLogger.logConsumerRegistered(context);

        // Add more context as needed
        LogContext offsetContext = LogContext.builder()
            .clientId(clientId)
            .topic(topic)
            .consumerGroup(group)
            .offset(restoredOffset)
            .custom("source", "disk")
            .build();

        consumerLogger.logConsumerOffsetUpdated(offsetContext);
    }
}
```

## Adding New Event Types

To add new event types:

1. Add method to appropriate `*EventLogger` interface
2. Implement method in `Default*EventLogger`
3. Use LogContext.builder() for rich context

Example:

```java
// In ConsumerEventLogger.java
void logConsumerReconnected(LogContext context);

// In DefaultConsumerEventLogger.java
@Override
public void logConsumerReconnected(LogContext context) {
    log.info("[CONSUMER] Reconnected: {}", context);
}
```

## Best Practices

1. **Build context once per operation** - Reuse for multiple log statements
2. **Use semantic methods** - logConsumerRegistered() vs log.info("Consumer registered...")
3. **Include relevant IDs** - topic, clientId, refreshId for correlation
4. **Add custom fields** - Use .custom() for operation-specific data
5. **Consistent prefixes** - [BROKER], [CONSUMER], [REFRESH], [DELIVERY]

## Future Enhancements

Potential future improvements:
- JSON log format option
- MDC integration for thread-local context
- Log aggregation metadata
- Metric extraction from structured logs
