# Runtime Configuration

Related: [Overview](01-system-overview.md), [Module map](02-module-map.md), [APIs](04-api-catalog.md), [POS state](07-pos-machine-state.md), [Risks](13-risk-and-edge-cases.md).

Primary broker config: `broker/src/main/resources/application.yml`.  
Consumer config: `test-consumer/src/main/resources/application.yml`.  
Container config: `Dockerfile`.

## Ports And Identity

| Setting | Application default | Docker default |
|---|---:|---:|
| Broker TCP | `19092` | `9092` |
| Broker HTTP | `8082` | `8081` |
| Node ID | `local-001` | `broker-001` |

Environment variables make Docker values override YAML. Documentation and operator commands must not assume one set.

## Storage

- Type: `filechannel`
- Data dir: `${DATA_DIR:/Users/anuragmishra/Desktop/workspace/messaging/data/}`
- Segment size: 100 MB
- Flush interval: 1 second
- Cache tail retained: 4 MB

Code property paths use both `data-dir` and `dataDir`; Micronaut relaxed binding is expected, but `ContainerResourceMonitor.java` specifically injects `broker.storage.dataDir`. Tests comment on supporting both forms.

Implementations are selected by `broker.storage.type` in `FileChannelStorageEngine.java` and `MMapStorageEngine.java`.

## Network And Delivery

- Netty boss threads: 1
- Netty worker threads: 4
- ACK timeout: 120 seconds
- Send timeout: `base 1s + floor(batchMB) * 2s`
- Storage read timeout: 30 seconds
- Batch byte limit: 256 KB
- Adaptive poll: 200 ms to 5 seconds
- Fair scheduler: 2 threads, 1 in-flight task per topic

Sources: `application.yml`, `BatchDeliveryService.java`, `TopicFairScheduler.java`.

## Executors

Configured:

- ACK: 2 threads, queue 1000
- ACK storage: 2 threads, queue 100
- consumer scheduler: 2 threads
- storage: 2 threads, queue 100

Factory also creates compaction, registry, refresh, and flush executors with code defaults in `broker/src/main/java/com/messaging/broker/config/ExecutorFactory.java`.

## Topology And Pipe

- Registry URL: `http://localhost:8080`
- Node ID sent as query parameter
- Pipe min/max interval: 500 ms / 20 seconds
- Pipe limit: 5
- Topology registry poll interval: 30 seconds in `TopologyManager.java`
- Parent probe: `/health`, 2-second connect and 3-second request timeout in `TopologyManager.java`

Persistent files: `topology.properties`, `pipe-offset.properties`.

## ACK Store

- Backend: RocksDB
- Live replay: enabled
- Startup seeding: enabled
- Path: `${DATA_DIR}/ack-store`
- Shared block cache: 16 MB
- Reconciliation: every 5 minutes, first after 2 minutes
- Auto-sync: disabled

Sources: `application.yml`, ACK package, `SharedRocksDb.java`.

## Compaction

- Backend: RocksDB index
- Enabled
- Tombstone retention: 7 days
- Window: 10 sealed segments
- Max topics/run: unlimited integer default
- Minimum sealed segments/topic: 1
- CPU guard: 70%
- Heap guard: 70%
- Schedule: initial 5 minutes, then every 24 hours

Sources: `application.yml`, `CompactionScheduler.java`.

## Legacy Service Topic Map

Source: `broker/src/main/resources/application.yml`.

| Service key | Topics |
|---|---|
| `price-quote` | `prices-v1`, `reference-data-v5`, `non-promotable-products`, `prices-v4`, `minimum-price`, `deposit` |
| `product-svc-lite` | `product-base-document` |
| `search-enterprise` | `search-product` |
| `tesco-location` | `location`, `location-clusters` |
| `customer-order-on-till` | `selling-restrictions` |
| `colleague-facts` | `colleague-facts-jobs`, `colleague-facts-legacy` |
| `loss-prevention-api` | four loss-prevention topics |
| `stored-value-services` | banned/active promotion topics |
| `colleague-identity` | `colleague-card-pin` |
| `distributed-identity` | `colleague-card-pin-v2` |
| `dcxp-content` | `dcxp-content` |
| `restriction-service-on-tills` | `restriction-rules` |
| `dcxp-ugc` | `dcxp-ugc` |

Mismatch: test-consumer defaults legacy service name to `price-quote-service`, which is not a key in this map.

## Security And Authentication

Broker YAML defines:

```text
broker.registry.auth.bearer-token
```

Outbound filter requires:

```text
broker.http.auth.token
```

Source: `common/src/main/java/com/messaging/common/http/AuthTokenClientFilter.java`.

No adapter between the keys was found, so setting only `AUTH_BEARER_TOKEN` from current YAML may not activate the filter.

No inbound server filter or controller `@Secured` annotation was found. Management endpoint sensitivity is configured, but enforcement is not confirmed.

## Monitoring

Prometheus, JVM, processor, uptime, file, and logback metrics are enabled. Health/prometheus/metrics are marked non-sensitive; other management endpoints default sensitive.

Scheduled monitors:

- container resources: 10 seconds;
- memory: 10 seconds;
- storage segments: 15 seconds;
- threads: 30 seconds.

Sources: monitoring package and `application.yml`.

## Consumer Runtime

`test-consumer` defaults:

- HTTP `8081`
- modern topics from `CONSUMER_TOPICS`/`CONSUMER_TOPIC`
- group from `CONSUMER_GROUP`
- broker host `localhost`, port `9092`
- legacy mode disabled
- legacy service default `price-quote-service`

Source: `test-consumer/src/main/resources/application.yml`.

## Container

`Dockerfile`:

- builds with Gradle 8.5/JDK 17;
- runs Eclipse Temurin 17 JRE;
- imports a repository CA certificate at build time;
- creates non-root `broker`;
- installs SQLite and curl;
- defaults heap to 512 MB initial / 2 GB max with G1;
- health-checks `/health`.

The image builds `:broker:build -x test`, so image construction does not execute tests.

## Static Analysis

`qodana.yaml` selects `qodana.starter`, JDK 21, and `jetbrains/qodana-jvm:latest`. No Qodana CLI was installed during this analysis and no CI file describing invocation was found.

## Environment-Specific Test Config

- Broker integration: `broker/src/integrationTest/resources/application-test.yml`
- Journey: `broker/src/systemTest/resources/application-system-test.yml`
- Module integration configs under each module's `src/integrationTest/resources/`

Tests disable or delay pipe/compaction/reconciliation where isolation requires it.

## Pipe Consistency

`pipe.consistency.*` in `application.yml` (all env-overridable, see file for env names):

| Setting | Default | Meaning |
|---|---:|---|
| `enabled` | `false` | fail-closed master switch; endpoints 404 and admin trigger 503 when off |
| `buckets` | `64` | digest buckets; raise for multi-million-key topics |
| `scan-yield-every` | `10000` | cooperative `Thread.yield()` cadence during index scans |
| `max-drilldown-buckets` | `8` | above this, report divergence without key-level drill-down |
| `max-classify-entries` | `1000` | per-check classification batch cap |
| `max-bucket-entries` | `100000` | server-side cap per bucket response (HTTP 413 above) |
| `max-concurrent-scans` | `2` | parent-side scan semaphore; excess children get 429 |
| `schedule.enabled` | `true` | auto-detection kill-switch (feature flag still gates everything) |
| `schedule.interval` | `6h` | cadence of automatic all-topics checks |
| `schedule.initial-delay` | `10m` | first automatic check after startup |
| `schedule.target` | `parent` | `parent` or `cloud` |
| `escalation.enabled` | `true` | clamp-persistence escalation to in-store verifiers |
| `escalation.after-clamped-checks` | `2` | consecutive clamped checks before escalating |
| `escalation.max-candidates` | `5` | head-probe budget per escalation |
