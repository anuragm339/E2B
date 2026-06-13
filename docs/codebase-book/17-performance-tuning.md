# Performance Tuning (Throughput Profile)

The broker ships with a deliberate **low-CPU / flat-memory profile** (heap pinned small, page
cache trimmed, delivery serialized per topic) so it runs inside a constrained POS device. This
chapter is the **inverse**: how to **spend memory and CPU to gain throughput and lower latency**,
using configuration only — no Java changes.

Every knob below already exists; the defaults were tuned *down*. We list the **current value**,
the **suggested** value, **why** it helps (the mechanism), and **what it costs**. Legend:
`[env]` = pure runtime override (compose environment / `JAVA_OPTS`, no image rebuild);
`[yml]` = a one-line `application.yml` edit that needs an image rebuild (still config, not Java).

Config reference: [Runtime configuration](11-runtime-config.md). The flat-memory rationale these
values reverse is in the JVM flags of the broker service (`docker-compose.yml`) and
`broker/src/main/resources/application.yml`.

---

## 0. Read this first — the memory budget rule

The flat profile is not arbitrary. Historically `-Xmx400m` produced **640MB+ RSS** and breached
the cgroup limit → the container was **OOM-killed (exit 137)**. Container RSS is not just the Java
heap; it is:

```
RSS ≈ Java heap (Xmx) + direct/Netty memory + Metaspace + thread stacks
        + glibc malloc arenas (scales with MALLOC_ARENA_MAX) + OS page cache for segment files
```

So the rule when tuning up:

> **Raise `mem_limit` (and `cpus`) FIRST, then raise `Xmx`, and keep the sum of all of the above
> comfortably under `mem_limit`.** Every tier below that adds memory (heap, page cache, RocksDB
> cache, malloc arenas) eats into the same budget. Bump one tier, watch RSS on `/prometheus` and
> `gc.log`, then proceed.

Current limits: `mem_limit: 560m`, `memswap_limit: 560m`, `MALLOC_ARENA_MAX: 2`.

---

## 1. Give the container room (do this before anything else)

| Setting | Where | Current | Suggested | Why / cost |
|---|---|---|---|---|
| `mem_limit` / `memswap_limit` | `[env]` compose | 560m | **1g–1.5g** | Headroom for a bigger heap + caches. Without this, every other tier risks exit 137. |
| `cpus` | `[env]` compose | (unset) | **2.0** | Lets the extra worker/scheduler threads actually run in parallel instead of time-slicing one core. |

Nothing else in this chapter is safe until the container has the headroom to hold it.

---

## 2. JVM heap and GC — the single biggest lever `[env: JAVA_OPTS]`

Current broker `JAVA_OPTS` pin the heap tiny and aggressively hand memory back to the OS to keep
RSS flat. Both behaviours cost throughput.

| Flag | Current | Suggested | Why it helps | Cost |
|---|---|---|---|---|
| `-Xmx` / `-Xms` | 224m / 64m | **640m / 640m** | Larger heap → garbage collected far less often → less GC CPU and fewer pauses; more room for batches and caches. `Xms=Xmx` stops repeated heap grow/shrink churn. | More committed RAM |
| `-XX:MinHeapFreeRatio` / `-XX:MaxHeapFreeRatio` | 10 / 30 | **40 / 70** | The low ratios force G1 to **shrink the heap and return pages to the OS** after every collection (great for flat RSS, bad for speed — the heap must re-expand under load). Raising them keeps the heap committed and steady. | Higher steady RSS |
| `-XX:G1PeriodicGCInterval` | 300000 (5 min) | **remove** | This exists only to run a GC every 5 minutes to trim RSS. Removing it stops periodic GC CPU and cache eviction you don't need when memory isn't scarce. | Higher steady RSS |
| `-XX:InitiatingHeapOccupancyPercent` | 45 | **65** | Starts the concurrent GC cycle later → fewer cycles → less GC CPU. | Uses more heap before collecting |
| `-XX:MaxDirectMemorySize` + `-Dio.netty.maxDirectMemory` | 32m | **128m** | Off-heap memory backs Netty's zero-copy buffers and larger batches; the 32m cap throttles how much in-flight delivery data can be buffered. | More off-heap RAM |

Keep `-XX:+UseG1GC`, `-XX:MaxGCPauseMillis=200`, `-XX:+UseCompressedOops`,
`-XX:+HeapDumpOnOutOfMemoryError`, `-XX:+ExitOnOutOfMemoryError`. (`-XX:+UseStringDeduplication`
spends a little CPU to save heap — you can drop it once heap is generous, for a small CPU win.)

---

## 3. Spend memory directly for read/IO speed

| Key | Where | Current | Suggested | Why it helps | Cost |
|---|---|---|---|---|---|
| `STORAGE_CACHE_DROP_TAIL_KEEP_BYTES` | `[env]` | 4MB | **64MB or `-1`** | After each fsync the broker calls `posix_fadvise(DONTNEED)` to **evict already-written log pages from the OS page cache** (keeps Docker's reported memory flat). Raising it (or `-1` to disable) keeps recently-written records in RAM, so consumer delivery reads hit page cache instead of re-reading disk. **High impact for delivery throughput.** | Higher (real, useful) page-cache RSS |
| `ACK_STORE_BLOCK_CACHE_BYTES` | `[env]` | 16MB | **64–128MB** | RocksDB block cache for the ACK/compaction column families; bigger cache → fewer disk reads during ACK lookups and reconciliation. | More native RAM |
| `MALLOC_ARENA_MAX` | `[env]` | 2 | **8** (or unset) | glibc uses per-arena pools to avoid lock contention between threads on `malloc`. Capping at 2 minimises RSS but serialises allocation under concurrency. Raising it removes that contention as you add threads. | More native RSS (the main RSS-inflator — raise `mem_limit` accordingly) |

---

## 4. Parallelism — CPU and threads for throughput

| Key | Where | Current | Suggested | Why it helps | Cost |
|---|---|---|---|---|---|
| `consumer.fairness.max-in-flight-per-topic` | `[yml]` | **1** | **2–3** | **The biggest throughput lever.** Today delivery is *serialized* per topic — one batch must be ACKed before the next is sent. Allowing 2–3 in flight overlaps send/process/ACK across batches. | More concurrent memory + CPU; consumer must tolerate overlapping batches |
| `consumer.fairness.threads` | `[yml]` | 2 | 4 | Larger delivery-scheduler pool → more topics/groups served concurrently. | More threads/CPU |
| `network.threads.worker` | `[yml]` | 4 | 8 | More Netty event-loop threads → more connections handled in parallel at peak. | More threads/CPU |
| `EXECUTOR_ACK_THREADS` / `EXECUTOR_STORAGE_THREADS` / `EXECUTOR_CONSUMER_SCHEDULER_THREADS` | `[env]` | 2 | 4 | Parallelise ACK persistence, storage writes, and scheduling instead of funnelling through 2 workers. | More threads/CPU |
| `EXECUTOR_ACK_QUEUE_CAPACITY` / `EXECUTOR_STORAGE_QUEUE_CAPACITY` | `[env]` | 1000 / 100 | 2–4× | Absorb bursts in the bounded queues instead of rejecting work under spikes. | More heap held in queues |

---

## 5. Latency and batch cadence

| Key | Where | Current | Suggested | Why it helps | Cost |
|---|---|---|---|---|---|
| `consumer.adaptive-polling.min-delay-ms` | `[yml]` | 200 | **50** | The watermark poller backs off between delivery attempts; a shorter floor picks up new records sooner → lower delivery latency. | More scheduler wakeups → more CPU when busy |
| `consumer.adaptive-polling.max-delay-ms` | `[yml]` | 5000 | 1000 | Shorter idle backoff → faster first delivery after a quiet period. | More idle wakeups |
| `MAX_MESSAGE_SIZE_PER_CONSUMER` | `[env]` | 256KB | 512KB–1MB | Larger batches mean fewer round-trips and ACKs per record → higher throughput. | ⚠️ Crosses G1's humongous-allocation threshold (>512KB with 1MB regions) → GC pressure. Only with a raised `Xmx`; consider `-XX:G1HeapRegionSize=2m`. |
| `PIPE_MIN_POLL_INTERVAL_MS` / `PIPE_POLL_LIMIT` | `[env]` | 500 / 5 | 100 / 20 | Poll the parent more often and pull more records per poll → faster catch-up from upstream. | More CPU + upstream network |

---

## 6. Durability/IO trade (decide deliberately)

| Key | Where | Current | Suggested | Why it helps | Cost |
|---|---|---|---|---|---|
| `STORAGE_FLUSH_INTERVAL` | `[env]` | 1s | 5s | Group-commit fsync cadence. A longer interval batches more writes per fsync → higher write throughput and less IO. | **Durability:** up to that window of just-written records can be lost on a power-cut. This is an IO/durability trade, not a memory/CPU one — choose with eyes open. |

---

## 7. What NOT to touch for performance

- **Pipe-consistency knobs** (`PIPE_CONSISTENCY_*`, buckets, drilldown, classify): these govern the
  *audit*, which runs on the `compactionExecutor` off the message hot path. They do not affect
  ingest/delivery throughput. See [Pipe consistency system design](16-pipe-consistency-system-design.md#10-scaling-to-millions-of-keys-without-burning-cpumemory).
- **Compaction CPU/heap guards** (`COMPACTION_MAX_PROCESS_CPU_USAGE`, `COMPACTION_MAX_HEAP_USAGE`,
  both 0.7): raising toward 1.0 lets compaction run during load — that reclaims *disk* faster but
  competes with delivery for CPU. Leave unless disk reclaim is the bottleneck.
- **ACK / send timeouts** (`ACK_TIMEOUT_MS`, `SEND_TIMEOUT_*`): correctness/robustness, not speed.
  Raising them only hides slowness; don't use them to "tune performance."

---

## 8. A coherent throughput profile

Runtime-only (no rebuild), drop into the broker service in `docker-compose.yml`:

```yaml
mem_limit: 1g
memswap_limit: 1g
cpus: "2.0"
environment:
  MALLOC_ARENA_MAX: "8"
  STORAGE_CACHE_DROP_TAIL_KEEP_BYTES: "67108864"   # 64MB — keep hot tail in page cache
  ACK_STORE_BLOCK_CACHE_BYTES: "67108864"          # 64MB RocksDB block cache
  MAX_MESSAGE_SIZE_PER_CONSUMER: "524288"          # 512KB batches
  EXECUTOR_ACK_THREADS: "4"
  EXECUTOR_STORAGE_THREADS: "4"
  EXECUTOR_CONSUMER_SCHEDULER_THREADS: "4"
  PIPE_MIN_POLL_INTERVAL_MS: "100"
  PIPE_POLL_LIMIT: "20"
  JAVA_OPTS: >-
    -Xms640m -Xmx640m
    -XX:MaxMetaspaceSize=96m -XX:MaxDirectMemorySize=128m -Dio.netty.maxDirectMemory=134217728
    -XX:+UseG1GC -XX:MaxGCPauseMillis=200 -XX:G1HeapRegionSize=2m
    -XX:InitiatingHeapOccupancyPercent=65 -XX:MinHeapFreeRatio=40 -XX:MaxHeapFreeRatio=70
    -XX:+UseCompressedOops -XX:+HeapDumpOnOutOfMemoryError -XX:HeapDumpPath=/data/heap-dump.hprof
    -XX:+ExitOnOutOfMemoryError -Xlog:gc*=info:file=/data/gc.log:time,uptime,level,tags
    --add-opens java.base/sun.nio.ch=ALL-UNNAMED --add-opens java.base/java.io=ALL-UNNAMED
```

Needs an `application.yml` edit + image rebuild (the highest-impact items):

```yaml
broker:
  network:
    threads:
      worker: 8
  consumer:
    adaptive-polling:
      min-delay-ms: 50
    fairness:
      threads: 4
      max-in-flight-per-topic: 2
```

---

## 9. How to roll it out and measure

1. **Raise `mem_limit`/`cpus` first** (Tier 1). Restart, confirm steady-state RSS has headroom.
2. Apply Tiers 2–3 (heap + caches). Watch `gc.log` (collection frequency/pause) and the
   `jvm_memory_used_bytes` / container RSS series on `/prometheus`.
3. Apply Tier 4 (`max-in-flight-per-topic` is the big one) and re-measure delivery throughput and
   `broker_messages_*` rates.
4. Tiers 5–6 only if latency or write throughput is still the bottleneck.
5. After each tier, verify the container has **not** crept toward `mem_limit` (exit 137 is the
   failure signature). The expected outcome is **higher steady RSS by design** — that is the
   memory you are deliberately spending — not a slow upward ratchet.

> Mirror image of the flat profile: there you trade latency/throughput for a ~440MB RSS ceiling;
> here you trade RSS and CPU for throughput and latency. Pick the profile per device class —
> a constrained till stays flat; a beefier store server or the cloud-adjacent tiers can run this.
