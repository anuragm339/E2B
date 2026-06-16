# 18. Roadmap — Local message bus (`@Producer`)

> **Status: PLANNED — not implemented.** As of 2026-06-15 nothing in this chapter
> exists in the working tree. It is a design record so the API, the refresh gating,
> and the constraints are agreed before any code is written. Existing classes cited
> below are verified against the tree; everything described as "proposed" / "new" is
> not yet built. Classes such as `@Producer`, `ClientProducerManager`, and a PRODUCE
> wire command **do not exist yet** — do not treat them as present.

## Goal

Extend the POS-local broker into a **single-host pub/sub bus** so any service container
co-located on the same POS can **publish** messages and any other can **subscribe** —
"like Kafka," hub model, with the existing local broker as the hub.

**Restriction (current iteration):** in-box only — same POS host / same Docker network.
No cross-POS, no cross-store, no cloud round-trip for bus messages. This deletes the hard
distributed problems (no cross-host consensus / ordering / partition tolerance).

**Priority:** the local bus is to be prioritized over the downstream cloud→consumer pipe,
because it works **offline / independent of cloud connectivity** — co-located services keep
exchanging events even when the cloud pipe is down.

## Two topic classes

| Class | Source of truth | Replicated downstream | Pipe-consistency audit | Compaction |
|---|---|---|---|---|
| Cloud-sourced (prices, reference data…) | Cloud | Yes | Yes ([ch.16](16-pipe-consistency-system-design.md)) | Yes |
| Locally-produced (inter-service events) | Local broker | **No** | **No** | TBD (see open questions) |

Locally-produced topics live and die on the local broker. The parent/child consistency
machinery (`TopologyManager`, verifier candidates) **does not apply** to them.

## What already exists vs what is new

Roughly 70% of a Kafka-style broker is already present and reused as-is:

- Per-topic segmented storage, sealing, FD management ([ch.5](05-data-model.md))
- Compaction = log-compaction by key ([ch.8](08-compaction-handling.md))
- Consumer subscription, batched delivery, ACKs, lag ([ch.6](06-event-kafka-flow.md))
- TCP wire protocol + HTTP admin, metrics, crash recovery

New work (all inbound):

1. A first-class **PRODUCE wire API** — today ingestion is the cloud pipe plus
   `DataHandler` (`broker/src/main/java/com/messaging/broker/handler/DataHandler.java`)
   and test injection; there is no client-facing publish command.
2. **Multi-producer concurrency** + per-partition ordering (the pipe ingest is effectively
   single-writer).
3. **Real partitions** — partition is hardcoded `0` in most paths (e.g.
   `storage.append(topic, 0, record)` in `BrokerService.handlePipeMessage`).
4. **Topic lifecycle** — dynamic create/configure/retention vs the fixed topic catalog.

## Developer API — `@Producer` annotation

Mirrors the existing consumer annotation
`provider/common/src/main/java/com/messaging/common/annotation/Consumer.java`
(`@Target(TYPE)`, `@Retention(RUNTIME)`; fields `topic()`, `group()`, a retry block,
`errorHandler()`), which is wired by `ClientConsumerManager`
(`provider/client/.../client/ClientConsumerManager.java`) and `ConsumerAnnotationProcessor`
(`provider/broker/.../consumer/ConsumerAnnotationProcessor.java`).

**DECIDED — interface + introduction advice (`@KafkaClient` style):** annotate an
*interface*; its methods become publish operations; Micronaut introduction advice generates
the implementation. The service `@Inject`s the interface and calls
`orders.publish(key, payload)`. Wired by a new client-side **`ClientProducerManager`**
(mirror of `ClientConsumerManager`).

Proposed `@Producer` fields (mirroring `@Consumer`):

| `@Consumer` field | `@Producer` | Notes |
|---|---|---|
| `topic()` | `topic()` | keep |
| `group()` | — | drop (consumer-only) |
| retry block + `errorHandler()` | retry block + `errorHandler()` | keep — publishing can fail (broker busy, disk) |
| — | `acks()` | new — delivery guarantee (see open questions) |
| — | `key()` / partition strategy | new — only meaningful once partitions are real |

Consequence of the introduction-advice choice: **every `publish()` funnels through one
generated interceptor**, giving a single chokepoint for the retry block AND the refresh gate.

## Refresh gating (DECIDED requirement)

Producing must be **OFF during a data refresh, ON only after `READY`**. It is the write-side
twin of the read-side pause already in the tree:
`HttpPipeConnector.pausePipeCalls()` / `resumePipeCalls()`
(`provider/pipe/.../HttpPipeConnector.java`) pause cloud ingestion while the refresh runs
`RESET → REPLAY → READY` ([ch.7](07-pos-machine-state.md)).

**Rationale:** a refresh replays a clean snapshot of a topic to consumers; a concurrent
`publish()` would interleave new records and tear the snapshot.

The gate must be driven by the **same refresh state** the pipe-pause uses
(`RefreshCoordinator`, `provider/broker/.../consumer/RefreshCoordinator.java`) — one source
of truth, not a second flag that can drift.

**RECOMMENDED (not yet confirmed):**
- **Reject-fast with a retriable error** while gated → the `@Producer` retry block
  auto-retries past `READY`. Avoids blocking caller threads (pileup) and buffering
  (unbounded memory — fights the flat-memory budget).
- **Per-topic gate** — only the refreshing topic is blocked, riding the per-topic refresh
  state machine; producers to other topics keep working.

## Constraints (must hold)

See [ch.11](11-runtime-config.md), [ch.13](13-risk-and-edge-cases.md), and `CLAUDE.md`:

- Flat-memory profile (Xmx224m) — local bus traffic must be bounded.
- **Must not impact the existing message flow**: the POS's primary job is delivering cloud
  data (prices, restrictions) to the tills; bus traffic must not starve that path.
- Compaction safety; safe consumer/POS state transitions.

## Open questions (resolve before v1 design)

1. Default `acks`: fire-and-forget (lightest on the POS) vs wait-for-persist (safest)?
2. Durability of local events across a POS reboot — share the segment store vs a lighter
   transient path?
3. Resource isolation — share the broker's budget (simple) vs quota'd so it provably cannot
   starve cloud→till (safer)?
4. Confirm per-topic vs global gate, and reject-and-retry vs block.

(Tracked alongside [ch.15](15-open-questions.md).)

## Proposed build order

1. Broker PRODUCE wire API.
2. Client publish capability.
3. `@Producer` annotation + `ClientProducerManager` + refresh gate.

Design the annotation up front so the wire API exposes exactly what it needs (acks levels,
key, retry hooks).
