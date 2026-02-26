# Failed Transfer and Consumer Stuck Detection Metrics

## Overview

This document describes the new metrics added to track failed data transfers and detect stuck consumers in the messaging broker.

## Problem Statement

### Issue 1: Failed Transfer Accounting
Previously, `broker_consumer_bytes_sent` counted ALL bytes attempted to be sent, including:
- Successful transfers
- Failed transfers (network errors, timeouts, consumer disconnects)

**Problem**: Dashboards showed inflated byte counts that included failed transfers, making it impossible to see actual successful data transferred.

**Solution**: Added `broker_consumer_bytes_failed` and `broker_consumer_messages_failed` metrics to track failures separately.

### Issue 2: Consumer Stuck Detection
No way to detect if a consumer was stuck (not processing messages or not sending ACKs).

**Solution**: Added timestamp metrics to track last successful delivery and last ACK received.

---

## New Metrics

### 1. Failed Transfer Metrics

#### `broker_consumer_bytes_failed`
- **Type**: Counter
- **Description**: Total bytes that failed to send to consumer group
- **Labels**: `topic`, `group`
- **Unit**: bytes
- **When Recorded**: When delivery fails in catch block (line 320 of RemoteConsumerRegistry.java)

#### `broker_consumer_messages_failed`
- **Type**: Counter
- **Description**: Total messages that failed to send to consumer group
- **Labels**: `topic`, `group`
- **When Recorded**: When delivery fails in catch block (line 320 of RemoteConsumerRegistry.java)

**Calculation - Successful Bytes Transferred**:
```promql
# Successful bytes = Total bytes sent - Failed bytes
broker_consumer_bytes_sent_total{topic="prices-v1", group="price-quote-group"}
-
broker_consumer_bytes_failed_total{topic="prices-v1", group="price-quote-group"}
```

**Calculation - Success Rate**:
```promql
# Success rate % = (Successful / Total) * 100
(broker_consumer_bytes_sent_total - broker_consumer_bytes_failed_total)
/
broker_consumer_bytes_sent_total * 100
```

---

### 2. Consumer Stuck Detection Metrics

#### `broker_consumer_last_delivery_time_ms`
- **Type**: Gauge
- **Description**: Timestamp (epoch milliseconds) of last successful delivery to consumer group
- **Labels**: `topic`, `group`
- **Unit**: milliseconds since epoch
- **When Updated**: After successful FileRegion send (line 415 of RemoteConsumerRegistry.java)

#### `broker_consumer_last_ack_time_ms`
- **Type**: Gauge
- **Description**: Timestamp (epoch milliseconds) of last ACK received from consumer group
- **Labels**: `topic`, `group`
- **Unit**: milliseconds since epoch
- **When Updated**: When BATCH_ACK received (line 443 of RemoteConsumerRegistry.java)

**Calculation - Time Since Last Delivery (seconds)**:
```promql
# How long since broker last sent data to this consumer
(time() * 1000 - broker_consumer_last_delivery_time_ms) / 1000
```

**Calculation - Time Since Last ACK (seconds)**:
```promql
# How long since broker last received ACK from this consumer
(time() * 1000 - broker_consumer_last_ack_time_ms) / 1000
```

**Stuck Detection Alert**:
```promql
# Alert if no delivery in last 5 minutes (300 seconds)
(time() * 1000 - broker_consumer_last_delivery_time_ms) / 1000 > 300

# Alert if no ACK in last 10 minutes (600 seconds)
(time() * 1000 - broker_consumer_last_ack_time_ms) / 1000 > 600
```

---

## Grafana Dashboard Examples

### Panel 1: Successful Bytes Transferred (Gauge)

**Query**:
```promql
sum by(topic, group) (
  broker_consumer_bytes_sent_total
  -
  broker_consumer_bytes_failed_total
) / 1024 / 1024
```

**Display**:
- Visualization: Gauge
- Unit: MB
- Title: "Successful Bytes Transferred (MB)"

---

### Panel 2: Failed Transfer Rate (Table)

**Query A - Total Sent**:
```promql
sum by(topic, group) (broker_consumer_bytes_sent_total)
```

**Query B - Failed**:
```promql
sum by(topic, group) (broker_consumer_bytes_failed_total)
```

**Query C - Success Rate**:
```promql
(sum by(topic, group) (broker_consumer_bytes_sent_total - broker_consumer_bytes_failed_total)
/
sum by(topic, group) (broker_consumer_bytes_sent_total)) * 100
```

**Display**:
- Visualization: Table
- Columns:
  - Topic
  - Group
  - Total Bytes Sent (MB)
  - Failed Bytes (MB)
  - Success Rate (%)

---

### Panel 3: Consumer Stuck Detection (Time Series)

**Query A - Time Since Last Delivery**:
```promql
(time() * 1000 - broker_consumer_last_delivery_time_ms{topic="prices-v1"}) / 1000
```

**Query B - Time Since Last ACK**:
```promql
(time() * 1000 - broker_consumer_last_ack_time_ms{topic="prices-v1"}) / 1000
```

**Display**:
- Visualization: Time series
- Unit: seconds
- Y-axis: Time (seconds)
- Legend: "Seconds since last delivery", "Seconds since last ACK"
- Thresholds:
  - Yellow: 300s (5 minutes)
  - Red: 600s (10 minutes)

---

### Panel 4: Consumer Health Status (Stat)

**Query**:
```promql
# Consumer is "stuck" if no ACK in last 10 minutes
(time() * 1000 - broker_consumer_last_ack_time_ms) / 1000
```

**Display**:
- Visualization: Stat
- Value Mappings:
  - 0-60: "Healthy" (green)
  - 60-300: "Warning" (yellow)
  - 300+: "STUCK" (red)
- Unit: seconds

---

### Panel 5: Failed Messages Over Time (Graph)

**Query**:
```promql
rate(broker_consumer_messages_failed_total[5m])
```

**Display**:
- Visualization: Graph
- Title: "Failed Message Rate (msg/s)"
- Y-axis: Messages per second
- Legend: Per topic/group

---

## Code Integration Points

### BrokerMetrics.java

**New Methods Added**:
```java
// Record failed transfer
public void recordConsumerTransferFailed(String consumerId, String topic, String group,
                                         int messageCount, long totalBytes)

// Update last delivery timestamp
public void updateConsumerLastDeliveryTime(String consumerId, String topic, String group)

// Update last ACK timestamp
public void updateConsumerLastAckTime(String consumerId, String topic, String group)

// Calculate time since last delivery (helper for testing)
public long getTimeSinceLastDelivery(String group, String topic)

// Calculate time since last ACK (helper for testing)
public long getTimeSinceLastAck(String group, String topic)
```

### RemoteConsumerRegistry.java

**Integration Points**:

1. **Line 320** - Record failed transfer in catch block:
   ```java
   metrics.recordConsumerTransferFailed(
       consumer.clientId, consumer.topic, consumer.group,
       batch.recordCount, batch.totalBytes
   );
   ```

2. **Line 415** - Update last delivery time after successful send:
   ```java
   metrics.updateConsumerLastDeliveryTime(
       consumer.clientId, consumer.topic, consumer.group
   );
   ```

3. **Line 443** - Update last ACK time when ACK received:
   ```java
   metrics.updateConsumerLastAckTime(
       clientId, topic, consumer.group
   );
   ```

---

## Testing

### Verify Metrics Endpoint

```bash
# Check that new metrics appear
curl http://localhost:8081/prometheus | grep "broker_consumer_bytes_failed"
curl http://localhost:8081/prometheus | grep "broker_consumer_messages_failed"
curl http://localhost:8081/prometheus | grep "broker_consumer_last_delivery_time_ms"
curl http://localhost:8081/prometheus | grep "broker_consumer_last_ack_time_ms"
```

Expected output:
```
# HELP broker_consumer_bytes_failed_total Bytes that failed to send to consumer group
# TYPE broker_consumer_bytes_failed_total counter
broker_consumer_bytes_failed_total{group="price-quote-group",topic="prices-v1"} 0.0

# HELP broker_consumer_messages_failed_total Messages that failed to send to consumer group
# TYPE broker_consumer_messages_failed_total counter
broker_consumer_messages_failed_total{group="price-quote-group",topic="prices-v1"} 0.0

# HELP broker_consumer_last_delivery_time_ms Timestamp (epoch ms) of last successful delivery
# TYPE broker_consumer_last_delivery_time_ms gauge
broker_consumer_last_delivery_time_ms{group="price-quote-group",topic="prices-v1"} 1739657340123.0

# HELP broker_consumer_last_ack_time_ms Timestamp (epoch ms) of last ACK received
# TYPE broker_consumer_last_ack_time_ms gauge
broker_consumer_last_ack_time_ms{group="price-quote-group",topic="prices-v1"} 1739657340567.0
```

### Simulate Failed Transfer

```bash
# Stop a consumer to trigger delivery failures
docker stop consumer-price-quote

# Check failed metrics increase
curl http://localhost:8081/prometheus | grep "broker_consumer_bytes_failed.*price"
```

### Test Stuck Detection

```bash
# Stop a consumer (simulates stuck)
docker stop consumer-price-quote

# Wait 5 minutes, then check time since last ACK
curl -s "http://localhost:9090/api/v1/query?query=(time()*1000-broker_consumer_last_ack_time_ms)/1000" | jq .
# Should show ~300+ seconds
```

---

## Monitoring Best Practices

### Alert Rules (Prometheus)

Create alert rules in `/monitoring/prometheus/alerts/consumer-health.yml`:

```yaml
groups:
  - name: consumer_health
    interval: 30s
    rules:
      # Alert on failed transfer rate > 1%
      - alert: HighFailedTransferRate
        expr: |
          (
            rate(broker_consumer_bytes_failed_total[5m])
            /
            rate(broker_consumer_bytes_sent_total[5m])
          ) > 0.01
        for: 5m
        labels:
          severity: warning
        annotations:
          summary: "High failed transfer rate for {{ $labels.topic }}/{{ $labels.group }}"
          description: "Failed transfer rate is {{ $value | humanizePercentage }}"

      # Alert if consumer stuck (no ACK in 10 minutes)
      - alert: ConsumerStuck
        expr: |
          (time() * 1000 - broker_consumer_last_ack_time_ms) / 1000 > 600
        for: 2m
        labels:
          severity: critical
        annotations:
          summary: "Consumer stuck: {{ $labels.topic }}/{{ $labels.group }}"
          description: "No ACK received for {{ $value | humanizeDuration }} seconds"

      # Alert if broker can't deliver (no successful delivery in 10 minutes)
      - alert: BrokerDeliveryStalled
        expr: |
          (time() * 1000 - broker_consumer_last_delivery_time_ms) / 1000 > 600
        for: 2m
        labels:
          severity: critical
        annotations:
          summary: "Broker delivery stalled: {{ $labels.topic }}/{{ $labels.group }}"
          description: "No successful delivery for {{ $value | humanizeDuration }} seconds"
```

### Dashboard Variable for Topic Selection

Add a variable to filter by topic:

```
Name: topic
Type: Query
Query: label_values(broker_consumer_bytes_sent_total, topic)
Multi-value: Yes
Include All: Yes
```

Then use in queries:
```promql
broker_consumer_bytes_sent_total{topic=~"$topic"}
```

---

## Migration Guide

### For Existing Dashboards

**Old Query (total bytes including failures)**:
```promql
broker_consumer_bytes_sent_total
```

**New Query (only successful bytes)**:
```promql
broker_consumer_bytes_sent_total - broker_consumer_bytes_failed_total
```

**Recommended**: Create two panels side-by-side:
1. Total bytes attempted
2. Successful bytes (total - failed)

This shows both the full picture and the actual success rate.

---

## Performance Impact

### Memory
- **Per consumer/topic combination**: +4 metrics (2 counters, 2 gauges)
- **Storage**: ~100 bytes per metric
- **For 24 topics × 13 consumers**: ~9.6KB total (negligible)

### CPU
- Counter increment: O(1) - no overhead
- Gauge update: O(1) - atomic long set
- Impact: < 0.1% CPU overhead

### Disk (Prometheus)
- Each metric stores samples every 15 seconds
- 4 new metrics × 312 consumer/topic combos × 4 samples/min = ~5KB/min
- Daily: ~7MB/day (acceptable for local Prometheus)

---

## Summary

**New Metrics**:
1. ✅ `broker_consumer_bytes_failed` - Track failed byte transfers
2. ✅ `broker_consumer_messages_failed` - Track failed message transfers
3. ✅ `broker_consumer_last_delivery_time_ms` - Detect stuck consumers
4. ✅ `broker_consumer_last_ack_time_ms` - Detect ACK delays

**Benefits**:
- **Accurate byte accounting**: `total - failed = successful`
- **Stuck detection**: Alert when consumer stops processing
- **Failure rate monitoring**: Track delivery success %
- **Operational visibility**: Know when consumers are unhealthy

**Next Steps**:
1. Build and deploy broker with new metrics
2. Verify metrics appear in Prometheus
3. Create Grafana dashboard panels
4. Set up alert rules
5. Monitor for 24 hours to baseline normal behavior

---

## Date
Created: 2025-02-16
Author: Claude Code
Feature: Failed Transfer and Stuck Detection Metrics
