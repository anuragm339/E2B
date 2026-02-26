# Pipe Performance Metrics - Implementation Guide

## ✅ Metrics Added to DataRefreshMetrics.java

### 1. Pipe Fetch Latency (Target: < 200ms)
**Metric**: `pipe_fetch_latency_seconds{topic, refresh_id}`
**Type**: Timer (Histogram with p50, p95, p99)
**Purpose**: Measure how long each pipe fetch takes

**Usage**:
```java
// In DataRefreshManager where you call pipe.fetch()
Timer.Sample sample = metrics.startPipeFetchTimer();
try {
    PipeResponse response = pipe.fetch(topic, offset, batchSize);
    // ... process response ...
} finally {
    metrics.stopPipeFetchTimer(sample, topic, refreshId);
}
```

### 2. Pipe Data Received
**Metrics**:
- `pipe_bytes_received_total{topic, refresh_id}` - Total bytes from pipe
- `pipe_messages_received_total{topic, refresh_id}` - Total messages from pipe

**Usage**:
```java
// After fetching from pipe
metrics.recordPipeBytesReceived(topic, refreshId, response.getTotalBytes());
metrics.recordPipeMessagesReceived(topic, refreshId, response.getMessageCount());
```

### 3. Consumer Data ACKed
**Metrics**:
- `consumer_bytes_acked_total{topic, refresh_id}` - Total bytes ACKed
- `consumer_messages_acked_total{topic, refresh_id}` - Total messages ACKed

**Usage**:
```java
// When consumer sends ACK (BATCH_ACK or READY_ACK)
metrics.recordConsumerBytesAcked(topic, refreshId, bytesInBatch);
metrics.recordConsumerMessagesAcked(topic, refreshId, messagesInBatch);
```

### 4. Pipe Lag (Auto-calculated)
**Metrics**:
- `pipe_lag_bytes{topic, refresh_id}` - Bytes received - bytes ACKed
- `pipe_lag_messages{topic, refresh_id}` - Messages received - messages ACKed

**Automatically updated** when you call recordPipeBytesReceived or recordConsumerBytesAcked

## 📊 Dashboard Queries

### Panel 1: Pipe Fetch Latency
**Query**:
```promql
# P95 latency
pipe_fetch_latency_seconds{quantile="0.95"}

# With 200ms threshold line
200 / 1000
```

**Visualization**: Time series graph with threshold line at 200ms

### Panel 2: Data Received vs ACKed
**Query**:
```promql
# Bytes received from pipe
pipe_bytes_received_total

# Bytes ACKed by consumers
consumer_bytes_acked_total

# Lag (in-flight data)
pipe_lag_bytes
```

**Visualization**: Multi-line time series

### Panel 3: Message Lag
**Query**:
```promql
pipe_lag_messages
```

**Visualization**: Stat panel with color thresholds:
- Green: < 100
- Yellow: 100-1000
- Red: > 1000

### Panel 4: Pipe Throughput
**Query**:
```promql
# Messages per second from pipe
rate(pipe_messages_received_total[1m])

# Bytes per second from pipe
rate(pipe_bytes_received_total[1m])
```

## 🔧 Next Steps

### Step 1: Instrument Pipe Fetches in DataRefreshManager
**File**: `broker/src/main/java/com/messaging/broker/refresh/DataRefreshManager.java`

Find where pipe.fetch() is called and wrap it:
```java
// Before:
PipeResponse response = pipe.fetch(topic, offset, batchSize);

// After:
Timer.Sample sample = metrics.startPipeFetchTimer();
PipeResponse response = pipe.fetch(topic, offset, batchSize);
metrics.stopPipeFetchTimer(sample, topic, currentRefreshId);
metrics.recordPipeBytesReceived(topic, currentRefreshId, response.getTotalBytes());
metrics.recordPipeMessagesReceived(topic, currentRefreshId, response.getRecordCount());
```

### Step 2: Instrument Consumer ACKs
**File**: `broker/src/main/java/com/messaging/broker/consumer/RemoteConsumerRegistry.java`

When receiving BATCH_ACK or READY_ACK, record the ACKed data:
```java
// When consumer ACKs a batch
metrics.recordConsumerBytesAcked(topic, refreshId, bytesInBatch);
metrics.recordConsumerMessagesAcked(topic, refreshId, messagesInBatch);
```

### Step 3: Reset Metrics on New Refresh
**File**: `broker/src/main/java/com/messaging/broker/refresh/DataRefreshManager.java`

At the start of a refresh:
```java
// In startRefresh()
metrics.resetPipeMetrics(topic, currentRefreshId);
```

### Step 4: Create Grafana Dashboard
Create a new dashboard with these panels:
1. Pipe Fetch Latency (Time series)
2. Pipe Lag (Stat panel)
3. Data Flow (Received vs ACKed)
4. Pipe Throughput

## 🎯 What This Solves

### Problem 1: Is pipe slow?
**Answer**: Look at `pipe_fetch_latency_seconds` (p95)
- If > 200ms → Pipe/source is slow
- If < 200ms → Pipe is fine, look elsewhere

### Problem 2: Is there a backlog?
**Answer**: Look at `pipe_lag_messages`
- If growing → Data piling up, consumers can't keep up
- If stable/decreasing → Consumers keeping pace

### Problem 3: How much data waiting?
**Answer**: `pipe_bytes_received_total` - `consumer_bytes_acked_total`
- Shows exact bytes in-flight
- Helps size buffers/memory

## 🧪 Testing

### Test 1: Normal Operation
1. Start refresh
2. Check `pipe_fetch_latency_seconds` < 200ms
3. Check `pipe_lag_messages` stays low

### Test 2: Slow Pipe
1. Add artificial delay to pipe.fetch()
2. Watch `pipe_fetch_latency_seconds` increase
3. Confirm lag builds up

### Test 3: Slow Consumer
1. Add delay to consumer processing
2. Watch `pipe_lag_messages` grow
3. Pipe latency should stay normal

This proves you can distinguish pipe slowness from consumer slowness!
