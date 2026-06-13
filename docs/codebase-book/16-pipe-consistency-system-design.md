# Pipe Consistency: System Design

A from-scratch, beginner-to-advanced walkthrough of how the pipe-consistency audit works:
what a "keyspace digest" is, how it is built from RocksDB, how two brokers compare without
shipping their data, what "drill-down" means, how it stays cheap at millions of keys, and why
independent compaction on the cloud and on the POS does not break it.

Related chapters: [Feature catalog](03-feature-catalog.md#pipe-consistency-detection),
[API catalog](04-api-catalog.md#pipe-consistency-api), [Compaction handling](08-compaction-handling.md),
[Runtime configuration](11-runtime-config.md).

Source: `broker/src/main/java/com/messaging/broker/consistency/` (`KeyspaceDigest.java`,
`PipeConsistencyService.java`, `ParentConsistencyClient.java`) and
`broker/src/main/java/com/messaging/broker/http/PipeConsistencyController.java`.

---

## 1. The problem (beginner)

Data flows downhill through a tree of brokers. A POS broker (the **child**) pulls records over
the **pipe** from its **parent** (another broker, and ultimately the **cloud**). Both ends run
**compaction** independently — each throws away superseded versions of a key on its own schedule
to save disk.

The question this feature answers, per topic:

> *Does this child hold everything its parent/cloud holds — or did it miss a record, or keep a
> stale value, or keep something that was deleted upstream?*

It is **detect-only**. It never copies or repairs data. It produces a verdict
(`CONSISTENT` / `INCONSISTENT` / …) and, when something is wrong, recommends a **refresh**
(the existing reset-replay flow) — it does not fix anything itself.

```
            cloud  (root, authoritative, never expires keys)
              │  pipe
          parent broker
              │  pipe
          this POS (child)   ← "do I have everything my parent has?"
```

---

## 2. The naive approach, and why it fails on a POS

The obvious way to compare two sets of records is to send all of them and diff. A single topic
holds **thousands to millions** of keys. Doing that on every check, for ~24 topics, on a
memory-constrained POS, would blow the CPU / memory / network budget the system is required to
respect.

So the whole design is built around one constraint:

> **Confirm "we match" without shipping the keyspace.** Only when something differs do we pay to
> find out *what*, and even then only in bounded pieces.

---

## 3. Mental model: fingerprint the keyspace

Picture the keyspace as a chest of **64 drawers** (buckets). Every key is dropped into a drawer
by **hashing** it:

```
bucket = hash(key) mod 64
```

### What "hashing the key" means

A **hash function** turns any input — here a key string like `"product-123"` — into a fixed-size
number, in a way that is:

- **Deterministic** — the same key *always* produces the same number, on every machine, every time.
- **Scrambling / uniform** — different keys spread evenly across the whole number range; no clustering.

In this codebase that function is `hash64(key)` (FNV-1a 64-bit, then a splitmix64 mix). The number
it returns is just a deterministic fingerprint of the string — for example the real pinned value:

```
hash64("product-123")  =  -6305294276135507699        (a 64-bit number)
```

That number is meaningless on its own. To turn it into a drawer number we take the **remainder
after dividing by 64** (modulo), which always lands in 0–63 (`bucketOf` also handles the sign):

```
bucketOf(hash64("product-123"), 64)  =  13      →  "product-123" lives in drawer 13, always
```

It is like a mailroom with 64 pigeonholes: the rule "compute a number from the address, take it
mod 64" tells you which hole each letter goes in. Anyone following the same rule files the same
letter into the same hole, with no coordination.

Three properties fall out — and they are exactly what the audit needs:

- **Both sides agree without talking.** The child and the cloud run the *same* `hash64` and the
  *same* `mod 64`, so `"product-123"` independently lands in drawer 13 on both. That is the only
  reason "drawer 13 here vs drawer 13 there" is a meaningful comparison — identical keys are
  guaranteed to be grouped identically on both ends. (It also relies on the hash being byte-for-byte
  identical in the provider and cloud code — that is the pinned-test-vector guarantee from §5.)
- **Even spread.** A good hash scatters keys uniformly, so ~8,700 keys split into roughly
  8,700 ÷ 64 ≈ **136 keys per drawer** instead of piling into one — keeping each drawer small
  enough to drill into cheaply.
- **Stateless and cheap.** There is no key→bucket lookup table; you just compute the hash. It works
  identically for 10 keys or 10 million.

Two different keys *can* land in the same drawer (a **collision**) — and that is fine: the drawer's
fingerprint XORs them together, so if *either* one differs between sides the drawer still flips, and
the per-drawer **count** catches the case where one side simply has more keys in a drawer.

### Fingerprinting each drawer

For each drawer we compute **one small fingerprint (a "digest")** that mixes in every key in that
drawer *and that key's latest offset*. So an entire topic — 8,000 or 8,000,000 keys — collapses
to **64 numbers + 64 counts** (~1 KB).

The magic property:

> If even one key in a drawer changes (missing, different offset, or extra), that drawer's
> fingerprint changes. If two sides have identical data, all 64 fingerprints are identical.

That means a single ~1 KB exchange can prove that millions of records match — and pinpoint
*which drawer* to investigate when they don't. (This is a one-level Merkle/hash-tree: buckets,
not a full tree — simpler and enough for one round trip.)

---

## 4. The source of truth: the RocksDB compaction index

The digest is **not** computed from the segment log files. It is computed from the **RocksDB
compaction index** — the `compaction` column family that already exists for delivery filtering
and compaction.

It stores exactly one entry per live key:

```
key   →   [ latestOffset : 8 bytes ][ latestTimestampMs : 8 bytes ]
```

This is the perfect input for a digest because:

- It is **one row per key** (already deduplicated to the latest version) — no need to scan
  millions of historical records.
- It is written **synchronously on the same ingest path** as the segments: when a record arrives
  over the pipe, `handlePipeMessage` appends it to the segment **and** calls
  `compactionIndex.updateKey(...)` before the pipe offset advances. So the index is a faithful
  projection of "what this node has, latest-per-key."
- The same structure exists on the parent and on the cloud, so both sides can compute comparable
  digests.

> See [Compaction handling](08-compaction-handling.md#persistent-index-and-checkpoints) for the
> index itself; this chapter only uses it as the digest input.

---

## 5. Building a digest from RocksDB (with numbers)

Let's build a digest by hand. Use **4 buckets** instead of 64 so it fits on screen. Suppose this
node's compaction index holds six keys at these latest offsets:

```
A@10   B@20   C@30   D@40   E@50   F@60      (key @ latestOffset)
```

**Step 1 — assign each key to a bucket** with `hash(key) mod 4`:

```
bucket 0:  A@10, C@30
bucket 1:  B@20, E@50
bucket 2:  D@40
bucket 3:  F@60
```

**Step 2 — turn each `key@offset` into one number** (its "contribution"). The real code mixes the
key hash and the offset together with strong bit-mixing (FNV-1a 64 then splitmix64), so changing
*either* the key or its offset produces a completely different number. For the example, pretend
the contributions are:

```
A@10 → 5     C@30 → 9     B@20 → 12     E@50 → 6     D@40 → 15     F@60 → 3
```

**Step 3 — XOR the contributions in each bucket** (and keep a count):

```
bucket 0 = 5 XOR 9  = 12   (count 2)
bucket 1 = 12 XOR 6 = 10   (count 2)
bucket 2 = 15              (count 1)
bucket 3 = 3               (count 1)
```

This node's whole keyspace is now:

```
digests = [12, 10, 15, 3]      counts = [2, 2, 1, 1]
```

Two properties make XOR the right fold:

- **Order-independent** — RocksDB can stream keys in any order; the XOR is the same.
- **A single change flips exactly one bucket** — because each key lands in one bucket, and XOR is
  reversible (`x XOR y XOR y = x`). Add a key → its bucket flips. Change an offset → the old
  contribution is replaced by a new one → its bucket flips.

> Advanced: the mixing functions are byte-for-byte identical in the provider and the cloud-server
> repos (pinned test vectors), so a child and the cloud independently computing a digest for the
> same `(key, offset)` set get the same numbers. Without that, comparison would be meaningless.

---

## 6. How the two sides compare

A check is one cheap exchange:

```
1. Child scans its OWN compaction index → builds its 64 digests   (local, no network)
2. Child calls  GET /pipe/consistency/digest?topic=…&watermark=…  (one ~1 KB HTTP call)
3. Parent scans ITS OWN index the same way → returns its 64 digests + counts
4. Child compares the two arrays of 64:
       all 64 equal      →  CONSISTENT          (done — the common case)
       some buckets differ →  remember which, go drill down
```

Continuing the example: suppose the parent also has key `G@70` (contribution `3`), and `G` hashes
to **bucket 2**. The parent's bucket 2 becomes `15 XOR 3 = 12`. Compare:

```
            child   parent
bucket 0:    12  ==  12     ✓
bucket 1:    10  ==  10     ✓
bucket 2:    15  !=  12     ✗   ← something differs in this drawer
bucket 3:     3  ==   3     ✓
```

We now know **bucket 2 disagrees** — but not *which* key. That's what drill-down is for.

---

## 7. Drill-down: naming the bad keys

Drill-down means: **fetch the actual key list only for the mismatched buckets**, then compare and
classify key by key.

```
Child:  GET /pipe/consistency/bucket?topic=…&bucket=2          (only the diverged buckets)
Parent: returns the (keyHash, offset) pairs it has in bucket 2
            child bucket 2:  { D@40 }
            parent bucket 2: { D@40, G@70 }
                                      ↑ child is missing G@70
```

For each difference the child can't resolve locally, it asks the parent to **classify** it
(`POST /pipe/consistency/classify`), which answers two questions: *is the record physically
present at that offset on the parent?* and *what is the parent's latest index state for that key?*
From those answers each difference becomes one category:

| Category | Meaning | Verdict |
|---|---|---|
| **missing** | Parent has a key ≤ watermark that the child never stored, and the record is physically present | INCONSISTENT |
| **stale** | Child's latest offset for a key is older than the parent's, record present | INCONSISTENT |
| **zombie** | Parent already deleted/expired the key, but the child still holds a version | INCONSISTENT |
| **fabricated** | Child holds a key an **authoritative** verifier (the cloud) never had | INCONSISTENT |
| **lagging** | Child-extra key whose parent latest is beyond the watermark — pure lag | benign |
| **child-newer** | Child advanced a key past a clamped watermark (it is simply ahead) | benign |
| **extra** | Child holds a key a *non-authoritative* parent has no entry for (lineage) | benign |

Those classified counts (`missingKeys`, `fabricatedKeys`, …) are what populate the report and the
"Problem Keys by Topic" Grafana panel.

---

## 8. The watermark (so normal lag isn't a false alarm)

The child only asks the parent to compare **up to the child's own head offset** — the
**watermark** `W`. Anything the parent has *beyond* `W` is data the child simply hasn't pulled
yet (normal lag), not an inconsistency, so the parent excludes offsets `> W` from its digest.

One wrinkle: after a topology **reshuffle**, the new parent might itself be *behind* this child.
Then the parent can only vouch for data up to *its* head, so the comparison **clamps** to
`effectiveWatermark = min(W, parentHead)` and the verdict becomes `CONSISTENT_UP_TO` — "we match
as far as the parent can confirm; the tail above that is pending." If a clamp persists, the child
**escalates**: it asks the registry for sibling `verifierCandidates`, probes their heads
(`GET /pipe/consistency/head`), and gets a full verdict from the first one whose head covers `W`.
The cloud is never fanned into for this. (Details in
[API catalog](04-api-catalog.md#pipe-consistency-api).)

---

## 9. Why compaction does not break this (the crux)

This is the question that usually confuses people: *both the cloud and the POS compact
independently on different schedules — how can their digests ever match?*

The answer: **the digest is computed over `key → latestOffset`, and compaction does not change the
latest offset of a live key.** Compaction only deletes **older, superseded** versions and
**expired tombstones**. It never touches the *latest* surviving record of a key.

```
Cloud segments for product-42:    [#100 v1] [#140 v2]            latest = 140
POS segments for product-42:      [#100 v1] [#140 v2]            latest = 140

Cloud runs compaction → physically removes #100:
Cloud segments:                              [#140 v2]            latest = 140   ← unchanged
POS hasn't compacted yet:         [#100 v1] [#140 v2]            latest = 140   ← unchanged
```

Even though the two sides now hold **different physical bytes on disk**, their compaction
**indexes** still agree: `product-42 → 140` on both. So the **digests are identical**. The digest
is **compaction-invariant by construction** — it deliberately ignores the historical versions
that compaction is allowed to differ on, and only fingerprints the latest-per-key state that
compaction must preserve.

**Cloud compaction vs POS compaction — the one real asymmetry.** The cloud (authoritative root)
keeps keys effectively forever; a POS may have been **offline past the tombstone retention
window** and missed a `DELETE` entirely. Two cases:

- The POS missed the `DELETE` and still serves the old value → the parent's latest for that key is
  a tombstone that has **expired and been physically removed**. The drill-down's physical-presence
  check sees "parent has no record at that offset" and classifies it as a **zombie** → INCONSISTENT
  (a refresh is genuinely needed).
- The parent's *latest* for a key was compacted away but the record really is gone everywhere →
  classified as **benign**, not a false alarm.

So compaction differences never produce a false INCONSISTENT through the digest; the only
compaction-driven divergence (a missed delete) is precisely a real problem, and the classify step
names it correctly.

### Worked case: the scheduler runs while only the cloud has compacted

A common worry: *the cloud has compacted a topic, the POS (till) has not, and the scheduler runs
the check in that in-between state — won't they disagree?* No. Step through it with `product-42`,
which both sides ingested as `v1@100` then `v2@140` (so `latest = 140` on both):

```
                  disk (segments)          index = digest input
Cloud (compacted):   [ #140 ]              product-42 → 140
POS   (not yet):     [ #100 ][ #140 ]      product-42 → 140
                       ^ old version still on disk, but NOT in the index
```

When the check runs:

```
POS drawer for product-42   uses contribution(product-42, 140)
Cloud drawer                uses contribution(product-42, 140)
                                   → identical → drawer matches → CONSISTENT
```

The key realisation: **offset 100 was never the latest, so it was never in either side's index,
so it never contributed to the digest in the first place.** The cloud physically deleting `#100`
removes something the digest never looked at. And the leftover `#100` still sitting on the POS's
disk is equally invisible — the digest reads the index (`→ 140`), not the raw segment bytes.
Compaction also preserves original offsets (`140` stays `140`, no renumbering), so the
`contribution(key, offset)` is stable on both sides.

So **running the scheduler before, during, or after compaction on either side yields the same
verdict.** A difference in how far each side has compacted cannot, by itself, cause a false
INCONSISTENT:

| What the cloud compacted away | POS state | Check result |
|---|---|---|
| Old **superseded versions** of live keys (normal case) | leftover old versions sit on POS disk but aren't in the index | **CONSISTENT** — latest offset unchanged on both |
| An **expired tombstone** the POS **also received** | both indexes lack the key | **CONSISTENT** — both agree it's deleted |
| An **expired tombstone** the POS **never received** (missed delete) | POS still holds the old value | **INCONSISTENT (zombie)** — a real gap, correctly flagged |

The principle in one line: the digest fingerprints **latest-per-key** (what compaction must
preserve) and ignores **historical versions** (what compaction is allowed to differ on), so the
two sides being at different points in their compaction cycles is simply invisible to the
comparison.

---

## 10. Scaling to millions of keys without burning CPU/memory

Three independent cost dimensions, and why each stays bounded:

**Memory — O(buckets), not O(keys).** The digest is built by **streaming** the index
(`CompactionIndex.forEachEntry`) and folding each entry into fixed `long[64]` digest and `int[64]`
count accumulators. Nothing materialises a map of millions of keys. Memory is ~64 longs + 64 ints
regardless of key count.

**CPU — one paced linear scan, off the hot path.** Building the digest is O(keys): read each
index row once, mix, XOR. It runs on the single-threaded `compactionExecutor` (never on the
delivery/ingest threads), and it yields every `scan-yield-every` entries so it cannot monopolise a
core on a POS.

**Network — constant for the match case.** The healthy case (all buckets equal) ships only the
64 digests, ~1 KB, no matter how many keys. Drill-down traffic happens **only** on divergence and
is bounded by the caps below.

```
Keys        Digest exchange (match)   Local scan        Memory
10,000      ~1 KB                     read 10K rows     ~1 KB accumulators
1,000,000   ~1 KB                     read 1M rows      ~1 KB accumulators
10,000,000  ~1 KB                     read 10M rows     ~1 KB accumulators
```

**The lever for millions of keys is the bucket count, not the classify cap.** With 64 buckets,
10M keys means ~156K keys per bucket — so drilling even one diverged bucket would pull ~156K keys
(and trip the server-side `max-bucket-entries` guard). Raising the **bucket count** (`buckets`,
passed per-request so both sides stay aligned) makes each bucket small enough to drill precisely:

```
10,000,000 keys ÷ 4096 buckets ≈ 2,400 keys per bucket   (drillable, still ~tens-of-KB digest)
```

---

## 11. The budget caps (and the drill-down cap specifically)

Drill-down cost grows with **how many buckets disagree**. To protect a POS from doing
keyspace-sized work during a mass divergence, the work is capped:

| Knob | Default | Bounds |
|---|---|---|
| `buckets` | 64 | Partition granularity (keys per bucket) |
| `max-drilldown-buckets` | 8 | How many diverged buckets get opened |
| `max-classify-entries` | 1000 | How many diverged keys get classified per check |
| `max-bucket-entries` | server guard | Max entries one bucket fetch may return (else 413) |
| `scan-yield-every` | pacing | CPU yield cadence during the local scan |

The decisive one:

```
if (diverged buckets > max-drilldown-buckets):     # "a lot is wrong"
        report INCONSISTENT, refresh recommended    # but DO NOT enumerate keys
        per-key counts stay 0
else:
        drill down, classify each key, fill counts
```

Why short-circuit: if more than 8 of 64 buckets differ, that is already "refresh this topic." The
*action* is identical whether 9 or 9,000 keys are wrong, so spending CPU to name thousands of keys
buys nothing. The cost: such a topic is INCONSISTENT with **zero** per-key counts — visible on the
**state** panel (`pipe_consistency_state == 1`) but not on the per-key "Problem Keys" panel. (This
is exactly why, in fault-injection, a 6-key tamper showed `missingKeys`/`fabricatedKeys` while a
10-key tamper showed only INCONSISTENT.) At a *clamped* watermark, over-cap divergence is reported
`INCONCLUSIVE` instead, because it might be benign child-ahead data the budget can't rule out.

---

## 12. System design — components and flow

```
                          ┌─────────────────────────────────────────────┐
                          │                 POS broker (child)           │
   admin POST  ─────────► │  PipeConsistencyAdminController  /check      │
   (or scheduler tick)    │            │ single-flight                   │
                          │            ▼                                 │
                          │  PipeConsistencyService.runCheck(topic)      │
                          │     │                                        │
                          │     │ 1. watermark = storage head            │
                          │     ▼                                        │
                          │  KeyspaceDigest.compute  ◄── streams ──┐     │
                          │     │ (local 64-bucket fold)           │     │
                          │     │                          RocksDB compaction index
                          │     │ 2. fetch parent digest           (key → latestOffset)
                          │     ▼                                        │
                          │  ParentConsistencyClient ──HTTP──► parent /pipe/consistency/digest
                          │     │  equal? → CONSISTENT                   │
                          │     │  else  → /bucket (mismatched only)     │
                          │     │        → /classify (physical presence) │
                          │     ▼                                        │
                          │  PipeConsistencyReport  +  Micrometer gauges │
                          │     (state, missing, zombie, fabricated)     │
                          └─────────────────────────────────────────────┘
```

Sequence for one topic:

```
child                         parent/cloud
  │  build my 64 digests (local scan)
  │ ── GET /digest?watermark=W ─────────►│  build its 64 digests ≤ W
  │ ◄── 64 digests + counts ─────────────│
  │  compare
  │      all equal → CONSISTENT ∎
  │      mismatch in buckets {b1,b2}
  │ ── GET /bucket?bucket=b1,b2 ────────►│  one scan, returns those buckets' (hash,offset)
  │ ◄── entries ─────────────────────────│
  │  diff locally → unresolved {offsets,keys}
  │ ── POST /classify {offsets,keys} ───►│  physical-presence + index-state per item
  │ ◄── {present?, keyState, authoritative}
  │  tally missing/stale/zombie/fabricated → verdict + refreshRecommended
```

The cloud mirrors the same three read endpoints (`/head`, `/digest`, `/bucket`, `/classify`) and
sets `authoritative: true` so the child can tell a *fabricated* key (cloud never had it) from a
merely-unknown key on a freshly provisioned POS parent.

---

## 13. Failure modes

| Situation | Result |
|---|---|
| Parent offline / network down | `UNREACHABLE` (no retry storm; next scheduled check tries again) |
| Parent is an old build without these endpoints (404) | `UNSUPPORTED_PARENT` |
| Local index/storage error | `ERROR` (with message) |
| Parent behind child (reshuffle), tail unverified | `CONSISTENT_UP_TO` + `verificationPending`, then escalation |
| Over-cap divergence, watermark equal | `INCONSISTENT`, no per-key counts |
| Over-cap divergence, watermark clamped | `INCONCLUSIVE` (may be benign child-ahead) |
| Heap pressure / a check already running | scheduler skips with a logged reason |

The check is **single-flight** (one at a time per node) and the server endpoints carry a
concurrent-scan semaphore (429 above the limit), so many children auditing one parent cannot
overwhelm it.

---

## 14. End-to-end worked example

A POS was offline for two days. While it was down, on `prices-v1`:

- `product-7` got a new price (parent offset 1,000,900) — the POS never received it.
- `product-9` was **deleted** upstream; the POS still holds the old value, and the parent's
  tombstone has since expired and been compacted away.

When the POS comes back and a check runs against the cloud:

1. POS builds 64 digests from its index at its own head `W`.
2. Cloud returns its 64 digests ≤ `W`. Two buckets differ — the ones holding `product-7` and
   `product-9`. That's 2 ≤ 8, so drill-down proceeds.
3. POS fetches those two buckets:
   - `product-7` present on cloud, absent on POS → `/classify` confirms a record physically lives
     at 1,000,900 → **missing** (+1).
   - `product-9` present on POS, but the cloud has no record at the POS's offset and its latest is
     a removed tombstone → **zombie** (+1).
4. Verdict: `INCONSISTENT`, `missingKeys=1`, `zombieKeys=1`, `refreshRecommended=true`.
5. Gauges update; the operator (or a future automation) triggers a refresh of `prices-v1`, which
   resets and replays the POS from the authoritative snapshot. The consistency feature itself
   changed nothing.

---

## 15. Cheat sheet

- **Input:** RocksDB compaction index (`key → latestOffset`), not segment files.
- **Digest:** 64 buckets; each bucket = XOR of strongly-mixed `(keyHash, offset)` contributions +
  a count. Order-independent; one changed key flips exactly one bucket.
- **Compare:** one ~1 KB digest exchange. Equal → CONSISTENT. Else → which buckets differ.
- **Drill-down:** fetch only mismatched buckets, classify each diff (missing/stale/zombie/
  fabricated vs benign). Capped at `max-drilldown-buckets` and `max-classify-entries`.
- **Watermark:** compare only up to the child's head; lag is invisible; a behind-parent clamps to
  `CONSISTENT_UP_TO` and may escalate to sibling verifiers.
- **Compaction-invariant:** digest fingerprints latest-per-key, which compaction preserves; the
  only compaction asymmetry (a missed expired delete) is correctly caught as a zombie.
- **Scales** because memory is O(buckets), the match case is constant network, and the scan is
  paced off the hot path. For millions of keys, raise **bucket count**, not the classify cap.
- **Detect-only:** the output is a verdict + "refresh recommended." It never moves data.
