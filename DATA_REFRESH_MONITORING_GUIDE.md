# Data Refresh Monitoring Dashboard Guide

## Overview

The Data Refresh Monitoring dashboard provides comprehensive visibility into the LocalDataRefresh workflow, tracking metrics across the entire refresh lifecycle from initiation to completion.

## Metrics Tracked

### 1. Refresh Lifecycle Metrics

#### `data_refresh_started_total`
- **Type:** Counter
- **Description:** Total number of data refresh workflows started
- **Labels:** None
- **Use Case:** Track how many refreshes have been initiated

#### `data_refresh_completed_total`
- **Type:** Counter
- **Description:** Total number of data refresh workflows completed
- **Labels:**
  - `topic` - Topic being refreshed
  - `refresh_type` - Type of refresh (LOCAL, GLOBAL, etc.)
  - `status` - Completion status (SUCCESS, FAILED, TIMEOUT)
- **Use Case:** Track successful vs failed refreshes by topic

#### `data_refresh_duration_seconds`
- **Type:** Histogram
- **Description:** Total duration of data refresh workflow (from start to completion)
- **Labels:**
  - `topic` - Topic being refreshed
  - `refresh_type` - Type of refresh (LOCAL)
- **Percentiles:** p50, p95, p99
- **Use Case:** Identify slow refreshes, track performance trends

### 2. Consumer ACK Timing Metrics

#### `data_refresh_reset_ack_duration_seconds`
- **Type:** Histogram
- **Description:** Time taken for consumer to ACK RESET message
- **Labels:**
  - `topic` - Topic being refreshed
  - `consumer` - Consumer identifier
- **Measured:** From RESET sent to RESET ACK received
- **Use Case:** Identify slow consumers, detect network issues

#### `data_refresh_ready_ack_duration_seconds`
- **Type:** Histogram
- **Description:** Time taken from RESET ACK to READY ACK (total replay time)
- **Labels:**
  - `topic` - Topic being refreshed
  - `consumer` - Consumer identifier
- **Measured:** From RESET ACK received to READY ACK received
- **Use Case:** Track replay performance, identify data processing bottlenecks

#### `data_refresh_replay_duration_seconds`
- **Type:** Histogram
- **Description:** Pure replay duration (from replay start to READY ACK)
- **Labels:**
  - `topic` - Topic being refreshed
  - `consumer` - Consumer identifier
- **Measured:** From replay start trigger to READY ACK received
- **Use Case:** Measure actual data transfer and processing time

### 3. Data Transfer Metrics

#### `data_refresh_bytes_transferred_total`
- **Type:** Counter
- **Description:** Total bytes transferred during data refresh
- **Labels:**
  - `topic` - Topic being refreshed
  - `consumer` - Consumer identifier
- **Unit:** bytes
- **Use Case:** Track data volume, estimate network usage

#### `data_refresh_messages_transferred_total`
- **Type:** Counter
- **Description:** Total messages transferred during data refresh
- **Labels:**
  - `topic` - Topic being refreshed
  - `consumer` - Consumer identifier
- **Use Case:** Track message count, calculate average message size

#### `data_refresh_transfer_rate_bytes_per_second`
- **Type:** Gauge
- **Description:** Current data transfer rate during refresh
- **Labels:**
  - `topic` - Topic being refreshed
  - `consumer` - Consumer identifier
- **Unit:** bytes_per_second
- **Use Case:** Monitor real-time transfer speed, detect network throttling

## Dashboard Panels

### Panel 1: Total Refreshes Started
- **Type:** Stat
- **Query:** `sum(data_refresh_started_total)`
- **Shows:** Total count of refreshes initiated
- **Use:** Quick overview of refresh activity

### Panel 2: Total Refreshes Completed (Success)
- **Type:** Stat
- **Query:** `sum(data_refresh_completed_total{status="SUCCESS"})`
- **Shows:** Total successful completions
- **Use:** Success rate monitoring

### Panel 3: Refresh Duration by Topic (p50/p95/p99)
- **Type:** Time Series
- **Queries:**
  - `histogram_quantile(0.99, sum(rate(data_refresh_duration_seconds_bucket{refresh_type="LOCAL"}[5m])) by (le, topic))`
  - `histogram_quantile(0.95, ...)`
  - `histogram_quantile(0.50, ...)`
- **Shows:** Refresh duration percentiles per topic
- **Use:** Identify slow topics, track performance trends

### Panel 4: RESET ACK Duration by Consumer (p95)
- **Type:** Time Series
- **Query:** `histogram_quantile(0.95, sum(rate(data_refresh_reset_ack_duration_seconds_bucket[5m])) by (le, topic, consumer))`
- **Shows:** Time to acknowledge RESET
- **Use:** Detect slow consumers, network latency issues

### Panel 5: READY ACK Duration (Replay Time) by Consumer (p95)
- **Type:** Time Series
- **Query:** `histogram_quantile(0.95, sum(rate(data_refresh_ready_ack_duration_seconds_bucket[5m])) by (le, topic, consumer))`
- **Shows:** Total replay time per consumer
- **Use:** Monitor data processing performance

### Panel 6: Total Bytes Transferred by Consumer
- **Type:** Time Series
- **Query:** `sum by(topic, consumer) (data_refresh_bytes_transferred_total)`
- **Shows:** Cumulative bytes sent to each consumer
- **Use:** Track data volume, estimate storage requirements

### Panel 7: Total Messages Transferred by Consumer
- **Type:** Time Series
- **Query:** `sum by(topic, consumer) (data_refresh_messages_transferred_total)`
- **Shows:** Cumulative message count per consumer
- **Use:** Track message volume, detect anomalies

### Panel 8: Transfer Rate (Bytes/Second)
- **Type:** Time Series
- **Query:** `data_refresh_transfer_rate_bytes_per_second`
- **Shows:** Real-time transfer speed
- **Use:** Monitor network performance, detect throttling

### Panel 9: Refresh Type Distribution
- **Type:** Pie Chart
- **Query:** `sum by(refresh_type) (data_refresh_started_total)`
- **Shows:** Distribution of LOCAL vs GLOBAL refreshes
- **Use:** Understand refresh patterns

### Panel 10: Refresh Summary Table
- **Type:** Table
- **Columns:**
  - Topic
  - Consumer
  - RESET ACK (p95) - Time to ACK RESET
  - READY ACK (p95) - Total replay time
  - Bytes Transferred
  - Messages Transferred
- **Use:** Comprehensive overview of all refreshes

## Setup Instructions

### 1. Deploy Code Changes
```bash
cd /Users/anuragmishra/Desktop/workspace/messaging/provider

# Build with new metrics
./gradlew clean build

# Restart broker
docker compose restart broker
```

### 2. Verify Metrics Endpoint
```bash
# Check that new metrics are exposed
curl http://localhost:8081/prometheus | grep data_refresh

# Should show metrics like:
# data_refresh_started_total 5.0
# data_refresh_completed_total{topic="prices-v1",refresh_type="LOCAL",status="SUCCESS"} 3.0
# data_refresh_duration_seconds_bucket{topic="prices-v1",refresh_type="LOCAL",le="0.5"} 0.0
```

### 3. Restart Grafana
```bash
# Restart Grafana to load new dashboard
docker compose restart grafana

# Wait 10 seconds
sleep 10
```

### 4. Access Dashboard
1. Open Grafana: http://localhost:3000
2. Login: admin/admin
3. Navigate to Dashboards → Data Refresh Monitoring
4. Or direct URL: http://localhost:3000/d/data-refresh-monitoring

## Usage Examples

### Example 1: Trigger Refresh and Monitor
```bash
# Trigger refresh for 3 topics
curl -X POST "http://localhost:8081/admin/refresh-topic" \
  -H "Content-Type: application/json" \
  -d '{
    "topics": ["prices-v1", "reference-data-v5", "non-promotable-products"]
  }'

# Open dashboard in browser
open http://localhost:3000/d/data-refresh-monitoring

# Observe:
# - Total Refreshes Started increases by 3
# - Refresh Duration chart shows active refreshes
# - RESET ACK Duration shows consumer response times
# - READY ACK Duration shows replay progress
# - Transfer metrics show data volume and speed
```

### Example 2: Compare Topic Performance
```bash
# Filter dashboard by time range (last 1 hour)
# Look at Panel 3: Refresh Duration by Topic

# Questions to answer:
# - Which topic takes longest to refresh?
# - Are refresh times increasing over time?
# - Are there outliers (spikes)?

# Check Panel 10: Summary Table
# - Sort by "READY ACK (p95)" to find slowest consumers
# - Compare "Bytes Transferred" across topics
```

### Example 3: Diagnose Slow Refresh
```bash
# If a refresh takes too long, check:

# 1. RESET ACK Duration (Panel 4)
#    - High values = slow consumer startup or network issues
#    - Expected: < 100ms

# 2. READY ACK Duration (Panel 5)
#    - High values = slow data processing or large dataset
#    - Expected: varies by data size

# 3. Transfer Rate (Panel 8)
#    - Low values = network bottleneck
#    - Expected: > 10 MB/s on local network

# 4. Summary Table (Panel 10)
#    - Identify specific consumer causing delay
#    - Check bytes/messages ratio (avg message size)
```

## Metric Correlations

### Total Refresh Duration Formula
```
Total Duration ≈ RESET ACK Duration + READY ACK Duration + Overhead

Where:
- RESET ACK Duration = Time for consumer to clear data
- READY ACK Duration = Time for consumer to replay all data
- Overhead = Network latency + Processing time
```

### Transfer Speed Calculation
```
Average Transfer Speed = Bytes Transferred / READY ACK Duration

Example:
- Bytes: 500 MB
- Duration: 10 seconds
- Speed: 50 MB/s
```

### Expected Values (Local Network)

| Metric | Expected | Warning | Critical |
|--------|----------|---------|----------|
| RESET ACK Duration | < 100ms | > 500ms | > 1s |
| READY ACK Duration | Varies by data | > 60s | > 300s |
| Transfer Rate | > 10 MB/s | < 5 MB/s | < 1 MB/s |
| Total Duration | < 30s | > 120s | > 300s |

## Alerting Rules (Optional)

You can create Prometheus alerts for these metrics:

```yaml
# Example: Alert if refresh takes > 5 minutes
groups:
  - name: data_refresh
    rules:
      - alert: DataRefreshSlow
        expr: histogram_quantile(0.95, rate(data_refresh_duration_seconds_bucket[5m])) > 300
        for: 1m
        labels:
          severity: warning
        annotations:
          summary: "Data refresh taking too long"
          description: "Topic {{ $labels.topic }} refresh duration p95 is {{ $value }}s"

      - alert: DataRefreshFailed
        expr: increase(data_refresh_completed_total{status="FAILED"}[5m]) > 0
        for: 1m
        labels:
          severity: critical
        annotations:
          summary: "Data refresh failed"
          description: "Topic {{ $labels.topic }} refresh failed"
```

## Troubleshooting

### Metrics Not Appearing
```bash
# 1. Check broker logs
docker logs messaging-broker | grep DataRefreshMetrics

# 2. Verify metrics registration
curl http://localhost:8081/prometheus | grep data_refresh

# 3. Check Prometheus targets
# Open: http://localhost:9090/targets
# Verify broker endpoint is UP

# 4. Check Prometheus logs
docker logs prometheus | grep broker
```

### Dashboard Not Loading
```bash
# 1. Check Grafana logs
docker logs grafana | grep provision

# 2. Verify dashboard file
ls -la /Users/anuragmishra/Desktop/workspace/messaging/monitoring/grafana/dashboard-files/data-refresh-monitoring.json

# 3. Restart Grafana
docker compose restart grafana

# 4. Check Grafana provisioning
docker exec grafana ls -la /var/lib/grafana/dashboards/
```

### No Data in Panels
```bash
# 1. Trigger a refresh to generate data
curl -X POST "http://localhost:8081/admin/refresh-topic" \
  -H "Content-Type: application/json" \
  -d '{"topics": ["prices-v1"]}'

# 2. Wait for completion (check logs)
docker logs -f messaging-broker

# 3. Refresh Grafana dashboard
# Click refresh icon in top-right

# 4. Check time range
# Set to "Last 15 minutes" or "Last 1 hour"
```

## Key Insights from Dashboard

### What You Can Learn:

1. **Performance Trends**
   - Are refreshes getting slower over time?
   - Which topics take longest to refresh?
   - Is there a correlation between data size and duration?

2. **Consumer Behavior**
   - Which consumers are slowest to acknowledge?
   - Which consumers process data slowest?
   - Are there outliers or consistent slow performers?

3. **Data Characteristics**
   - Average message size per topic
   - Total data volume per topic
   - Transfer speed consistency

4. **System Health**
   - Success rate of refreshes
   - Network performance (transfer rates)
   - Consumer responsiveness (ACK times)

5. **Capacity Planning**
   - How much data is transferred during refresh?
   - How long does a full refresh take?
   - Can the system handle more topics/consumers?

## Files Created

1. **broker/src/main/java/com/messaging/broker/metrics/DataRefreshMetrics.java**
   - Metrics collection class
   - Tracks all refresh lifecycle events
   - Exposes Prometheus metrics

2. **monitoring/grafana/dashboard-files/data-refresh-monitoring.json**
   - Grafana dashboard definition
   - 10 panels covering all aspects
   - Auto-provisioned on Grafana startup

3. **broker/src/main/java/com/messaging/broker/refresh/DataRefreshManager.java** (Modified)
   - Integrated metrics calls
   - Tracks refresh events at key points

## Next Steps

1. **Trigger a test refresh:**
   ```bash
   curl -X POST "http://localhost:8081/admin/refresh-topic" \
     -H "Content-Type: application/json" \
     -d '{"topics": ["prices-v1"]}'
   ```

2. **Open the dashboard:**
   http://localhost:3000/d/data-refresh-monitoring

3. **Observe the metrics** as the refresh progresses

4. **Analyze the results** using the Summary Table

5. **Set up alerts** if needed for production monitoring

## Summary

The Data Refresh Monitoring dashboard provides complete visibility into:
- ✅ How many refreshes were initiated
- ✅ What type of refresh (LOCAL)
- ✅ How long each refresh took
- ✅ Which consumers were refreshed
- ✅ How long consumers took to ACK RESET
- ✅ How long consumers took to ACK READY (replay time)
- ✅ How much data was transferred
- ✅ What was the transfer speed

All metrics are tracked per-topic and per-consumer with time-series visualization for trend analysis!
