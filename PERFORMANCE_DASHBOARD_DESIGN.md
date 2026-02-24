# Performance Bottleneck Dashboard Design

## Goal
Identify whether slowness is due to:
1. **Pipe (Source)** - Slow data fetching from upstream
2. **Broker** - Slow processing/delivery
3. **Specific Consumer** - Which consumer is lagging

## Metrics Needed

### 1. Pipe Performance Metrics (NEW - TO ADD)

```java
// In DataRefreshMetrics.java or new PipeMetrics.java
pipe_fetch_latency_seconds{topic="prices-v1"}          // How long each fetch takes
pipe_fetch_records_count{topic="prices-v1"}            // Records per fetch
pipe_total_records_available{topic="prices-v1"}        // Total at source
pipe_records_sent{topic="prices-v1"}                   // Already sent
pipe_lag_records{topic="prices-v1"}                    // Waiting = available - sent
pipe_fetch_failures_total{topic="prices-v1"}           // Failed fetches
```

### 2. Enhanced Consumer Metrics (EXISTING - VERIFY USAGE)

```java
// Already exists in BrokerMetrics.java
broker.consumer.delivery.latency{consumer_id, topic, group}  // ✅ Line 391
broker.consumer.lag{consumer_id, topic, group}               // ✅ Line 363
broker.consumer.messages.sent{consumer_id, topic, group}     // ✅ Line 272
broker.consumer.bytes.sent{consumer_id, topic, group}        // ✅ Line 282
broker.consumer.acks{consumer_id, topic, group}              // ✅ Line 300
```

### 3. Consumer Batch Processing Metrics (NEW - TO ADD)

```java
// Per-consumer batch processing time
consumer_batch_processing_seconds{consumer_id, topic, group}    // Time to process batch
consumer_batch_ack_delay_seconds{consumer_id, topic, group}     // Time from send to ACK
```

## Dashboard Panels

### Panel 1: Pipeline Overview (Top Row)
**Type**: Stat panels in a row
- **Pipe Fetch Latency** (p95) - Is source slow?
- **Broker Delivery Latency** (p95) - Is broker slow?
- **Slowest Consumer** (max consumer latency) - Which consumer is slow?
- **Total Lag** (pipe lag + consumer lag) - How far behind are we?

### Panel 2: Pipe Performance (Second Row Left)
**Type**: Graph (Time series)
**Metrics**:
- `pipe_fetch_latency_seconds` (p50, p95, p99 lines)
- `pipe_fetch_records_count` (bar chart overlay)

**Purpose**: See if pipe is consistently slow or has spikes

### Panel 3: Pipe Lag (Second Row Right)
**Type**: Graph (Time series)
**Metrics**:
- `pipe_total_records_available` (line)
- `pipe_records_sent` (line)
- `pipe_lag_records` (area fill between)

**Purpose**: See how many records are waiting at source

### Panel 4: Consumer Latency Comparison (Third Row)
**Type**: Graph (Time series) with multiple series
**Metrics**:
- `broker.consumer.delivery.latency{consumer_id=~".*"}` (one line per consumer)

**Purpose**: Identify which consumer is slowest

### Panel 5: Consumer Lag Comparison (Fourth Row Left)
**Type**: Bar gauge (horizontal)
**Metrics**:
- `broker.consumer.lag{consumer_id=~".*"}` (one bar per consumer)

**Purpose**: See which consumer has highest lag

### Panel 6: Consumer Throughput (Fourth Row Right)
**Type**: Graph (Time series)
**Metrics**:
- `rate(broker.consumer.messages.sent[1m])` (msgs/sec per consumer)

**Purpose**: See consumption rate per consumer

### Panel 7: Bottleneck Identification Table (Fifth Row)
**Type**: Table
**Columns**:
- Consumer ID
- Topic
- Avg Latency (ms)
- P95 Latency (ms)
- Lag (records)
- Throughput (msgs/sec)
- Last ACK (seconds ago)

**Purpose**: Quickly identify slow consumers

### Panel 8: Alerts / Thresholds (Bottom)
**Type**: Stat panels with color thresholds
- **Pipe Lag > 1000 records** → Yellow/Red
- **Consumer Latency > 500ms** → Yellow/Red
- **Consumer Lag > 100 records** → Yellow/Red

## Color Coding
- **Green**: < 100ms latency, < 10 lag
- **Yellow**: 100-500ms latency, 10-100 lag
- **Red**: > 500ms latency, > 100 lag

## Workflow: Finding the Bottleneck

1. **Look at Pipeline Overview**
   - If "Pipe Fetch Latency" is high → Pipe is slow
   - If "Broker Delivery Latency" is high → Broker is slow
   - If "Slowest Consumer" is high → Specific consumer is slow

2. **Check Pipe Performance**
   - If fetch latency is spiking → Source system issue
   - If fetch size is decreasing → Source running out of data

3. **Check Consumer Latency Comparison**
   - Find which consumer has highest latency line
   - That's your bottleneck

4. **Check Consumer Lag**
   - Consumer with highest lag is falling behind
   - May need scaling or optimization

## Implementation Tasks

1. ✅ Review existing metrics usage
2. ❌ Add Pipe performance metrics to DataRefreshMetrics
3. ❌ Instrument pipe fetch calls in DataRefreshManager
4. ❌ Add consumer batch processing metrics
5. ❌ Create Grafana dashboard JSON
6. ❌ Test with slow pipe scenario
7. ❌ Test with slow consumer scenario
8. ❌ Add alerting rules (optional)
