package com.messaging.broker.consistency;

import com.messaging.broker.compaction.CompactionIndex;
import com.messaging.broker.consistency.ParentConsistencyClient.BucketEntry;
import com.messaging.broker.consistency.ParentConsistencyClient.ClassifyResponse;
import com.messaging.broker.consistency.ParentConsistencyClient.DigestResponse;
import com.messaging.broker.consistency.ParentConsistencyClient.KeyState;
import com.messaging.broker.consistency.ParentConsistencyClient.UnsupportedParentException;
import com.messaging.broker.consistency.PipeConsistencyReport.State;
import com.messaging.broker.core.TopologyManager;
import com.messaging.common.api.StorageEngine;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.Gauge;
import io.micrometer.core.instrument.MeterRegistry;
import io.micronaut.context.annotation.Value;
import jakarta.inject.Inject;
import jakarta.inject.Singleton;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeSet;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Detect-only pipe-consistency check of this node against its current parent (or the cloud).
 *
 * <p>Compares compaction-invariant keyspace digests (see {@link KeyspaceDigest}) at the child's
 * own watermark, drilling into mismatched buckets only and classifying every difference as a
 * real inconsistency (missing / stale / zombie key) or a benign artifact (lag, reshuffle extra).
 *
 * <p>Cost profile: nothing runs unless {@link #runCheck} is invoked (admin API; scheduler later).
 * Per check: one streaming index scan per side per topic, one ~1 KB HTTP exchange per topic when
 * consistent, drill-down traffic proportional to actual divergence. Single-flight — concurrent
 * runs are rejected. Never touches delivery, ingest, or storage write paths.
 */
@Singleton
public class PipeConsistencyService {

    private static final Logger log = LoggerFactory.getLogger(PipeConsistencyService.class);
    private static final int MAX_SAMPLES = 10;
    private static final int MAX_HISTORY = 64;

    public static final String TARGET_PARENT = "parent";
    public static final String TARGET_CLOUD = "cloud";

    private final StorageEngine storage;
    private final CompactionIndex compactionIndex;
    private final ParentConsistencyClient client;
    private final TopologyManager topologyManager;
    private final MeterRegistry meterRegistry;
    private final String cloudUrl;
    private final boolean enabled;
    private final int buckets;
    private final int scanYieldEvery;
    private final int maxDrilldownBuckets;
    private final int maxClassifyEntries;
    private final boolean escalationEnabled;
    private final int escalateAfterClampedChecks;
    private final int maxEscalationCandidates;

    // Consecutive clamped outcomes per topic — escalation fires only when a clamp PERSISTS
    // (most clamps self-heal: the new parent is catching up and passes the watermark soon).
    private final Map<String, Integer> clampStreaks = new ConcurrentHashMap<>();

    private final AtomicBoolean running = new AtomicBoolean();
    private final Map<String, PipeConsistencyReport> latestByTopic = new ConcurrentHashMap<>();
    private final List<PipeConsistencyReport> history = new ArrayList<>(); // guarded by itself
    private final Map<String, AtomicInteger> stateGauges = new ConcurrentHashMap<>();
    private final Map<String, java.util.concurrent.atomic.AtomicLong> missingGauges = new ConcurrentHashMap<>();
    private final Map<String, java.util.concurrent.atomic.AtomicLong> zombieGauges = new ConcurrentHashMap<>();
    private final Map<String, java.util.concurrent.atomic.AtomicLong> fabricatedGauges = new ConcurrentHashMap<>();
    private final Map<String, java.util.concurrent.atomic.AtomicLong> lastCheckEpoch = new ConcurrentHashMap<>();
    private final Counter checksTotal;
    private final Counter inconsistentTotal;
    private final io.micrometer.core.instrument.Timer checkDuration;

    @Inject
    public PipeConsistencyService(
            StorageEngine storage,
            CompactionIndex compactionIndex,
            ParentConsistencyClient client,
            TopologyManager topologyManager,
            MeterRegistry meterRegistry,
            @Value("${broker.registry.url}") String cloudUrl,
            @Value("${pipe.consistency.enabled:false}") boolean enabled,
            @Value("${pipe.consistency.buckets:64}") int buckets,
            @Value("${pipe.consistency.scan-yield-every:10000}") int scanYieldEvery,
            @Value("${pipe.consistency.max-drilldown-buckets:8}") int maxDrilldownBuckets,
            @Value("${pipe.consistency.max-classify-entries:1000}") int maxClassifyEntries,
            @Value("${pipe.consistency.escalation.enabled:true}") boolean escalationEnabled,
            @Value("${pipe.consistency.escalation.after-clamped-checks:2}") int escalateAfterClampedChecks,
            @Value("${pipe.consistency.escalation.max-candidates:5}") int maxEscalationCandidates) {
        this.storage = storage;
        this.compactionIndex = compactionIndex;
        this.client = client;
        this.topologyManager = topologyManager;
        this.meterRegistry = meterRegistry;
        this.cloudUrl = stripTrailingSlash(cloudUrl);
        this.enabled = enabled;
        this.buckets = Math.max(1, buckets);
        this.scanYieldEvery = scanYieldEvery;
        this.maxDrilldownBuckets = Math.max(1, maxDrilldownBuckets);
        this.maxClassifyEntries = Math.max(16, maxClassifyEntries);
        this.escalationEnabled = escalationEnabled;
        this.escalateAfterClampedChecks = Math.max(1, escalateAfterClampedChecks);
        this.maxEscalationCandidates = Math.max(1, maxEscalationCandidates);
        this.checksTotal = Counter.builder("pipe.consistency.checks")
                .description("Pipe-consistency topic checks run").register(meterRegistry);
        this.inconsistentTotal = Counter.builder("pipe.consistency.inconsistent")
                .description("Topic checks that found real divergence").register(meterRegistry);
        this.checkDuration = io.micrometer.core.instrument.Timer.builder("pipe.consistency.check.duration")
                .description("Wall time of one topic consistency check").register(meterRegistry);
    }

    public boolean isEnabled() {
        return enabled;
    }

    public boolean isRunning() {
        return running.get();
    }

    /**
     * Run a check for {@code topicOrAll} ("all" = every topic in local storage, sorted) against
     * {@code target} ("parent" = current parent from TopologyManager, "cloud" = registry URL).
     *
     * @return reports in execution order, or {@code null} if another run holds the
     *         single-flight guard
     */
    public List<PipeConsistencyReport> runCheck(String topicOrAll, String target) {
        if (!running.compareAndSet(false, true)) {
            return null;
        }
        try {
            String targetUrl = resolveTarget(target);
            List<String> topics = "all".equalsIgnoreCase(topicOrAll)
                    ? new ArrayList<>(new TreeSet<>(storage.getTopicNames()))
                    : List.of(topicOrAll);

            List<PipeConsistencyReport> reports = new ArrayList<>(topics.size());
            for (String topic : topics) {
                PipeConsistencyReport report = checkTopic(topic, targetUrl);
                report = trackClampAndMaybeEscalate(topic, targetUrl, report);
                record(report);
                reports.add(report);
            }
            return reports;
        } finally {
            running.set(false);
        }
    }

    // ── Escalation: a clamped parent cannot verify our full watermark ─────────

    /**
     * A clamped outcome (CONSISTENT_UP_TO or clamped INCONCLUSIVE) means the parent's head is
     * below ours — only possible after a reshuffle assigned us a parent that is behind. The
     * tail above the clamp is unverified. Most clamps self-heal (the parent is catching up),
     * so the first {@code escalateAfterClampedChecks - 1} occurrences just mark the report
     * {@code verificationPending}. A PERSISTENT clamp escalates to the registry-provided
     * in-store verifier candidates: probe heads cheaply (no scan), run the full check against
     * the first candidate whose head covers our watermark. The store-top always qualifies
     * eventually (all in-store data descends from it), so the cloud is never involved.
     */
    private PipeConsistencyReport trackClampAndMaybeEscalate(
            String topic, String originalTarget, PipeConsistencyReport report) {
        boolean clamped = (report.state == State.CONSISTENT_UP_TO || report.state == State.INCONCLUSIVE)
                && report.effectiveWatermark < report.watermark;
        if (!clamped) {
            clampStreaks.remove(topic);
            return report;
        }

        report.verificationPending = true;
        int streak = clampStreaks.merge(topic, 1, Integer::sum);
        if (!escalationEnabled || streak < escalateAfterClampedChecks) {
            return report;
        }

        long watermark = report.watermark;
        int probed = 0;
        for (String candidate : topologyManager.getVerifierCandidates()) {
            String candidateUrl = stripTrailingSlash(candidate);
            if (candidateUrl == null || candidateUrl.equals(originalTarget)) {
                continue; // the clamped parent cannot verify — that is why we are here
            }
            if (++probed > maxEscalationCandidates) {
                break;
            }
            long candidateHead;
            try {
                candidateHead = client.fetchHead(candidateUrl, topic);
            } catch (Exception e) {
                log.info("event=pipe_consistency.escalation_candidate_skipped topic={} candidate={} reason={}",
                        topic, candidateUrl, e.getMessage());
                continue; // down or unsupported — exactly why the registry sends a LIST
            }
            if (candidateHead < watermark) {
                log.debug("event=pipe_consistency.escalation_candidate_behind topic={} candidate={} head={} watermark={}",
                        topic, candidateUrl, candidateHead, watermark);
                continue;
            }

            log.info("event=pipe_consistency.escalating topic={} from={} to={} clampStreak={}",
                    topic, originalTarget, candidateUrl, streak);
            PipeConsistencyReport escalatedReport = checkTopic(topic, candidateUrl);
            escalatedReport.escalatedFrom = originalTarget;
            if (escalatedReport.state == State.CONSISTENT || escalatedReport.state == State.INCONSISTENT) {
                clampStreaks.remove(topic); // full verdict obtained
            } else {
                escalatedReport.verificationPending = true; // candidate raced behind — keep pending
            }
            return escalatedReport;
        }

        log.info("event=pipe_consistency.escalation_exhausted topic={} watermark={} clampStreak={} " +
                "— no reachable in-store verifier covers the watermark yet (self-heals as they catch up)",
                topic, watermark, streak);
        return report;
    }

    // ── Per-topic check ───────────────────────────────────────────────────────

    private PipeConsistencyReport checkTopic(String topic, String targetUrl) {
        long startMs = System.currentTimeMillis();
        PipeConsistencyReport report = PipeConsistencyReport.of(topic, State.ERROR, targetUrl);
        report.samples = new ArrayList<>();
        try {
            if (targetUrl == null) {
                report.state = State.UNREACHABLE;
                report.error = "no parent assigned (topology not connected)";
                return report;
            }

            long watermark = storage.getCurrentOffset(topic, 0);
            report.watermark = watermark;
            report.effectiveWatermark = watermark;
            if (watermark < 0) {
                // Child holds nothing for this topic — vacuously consistent (pure lag).
                report.state = State.CONSISTENT;
                return report;
            }

            // Child scans FIRST: any ingest racing the check can then only move entries above
            // the captured watermark, which degrades to a benign drill-down classification —
            // never a false INCONSISTENT.
            KeyspaceDigest.Result mine =
                    KeyspaceDigest.compute(compactionIndex, topic, watermark, buckets, scanYieldEvery);
            report.keysScanned = mine.entriesScanned;

            DigestResponse parent = client.fetchDigest(targetUrl, topic, watermark, buckets);

            boolean clamped = parent.effectiveWatermark < watermark;
            if (clamped) {
                // Parent is behind us (fresh reshuffle) — compare only up to what it can verify.
                report.effectiveWatermark = parent.effectiveWatermark;
                mine = KeyspaceDigest.compute(
                        compactionIndex, topic, parent.effectiveWatermark, buckets, scanYieldEvery);
            }

            List<Integer> mismatched = mismatchedBuckets(mine, parent);
            if (mismatched.isEmpty()) {
                report.state = clamped ? State.CONSISTENT_UP_TO : State.CONSISTENT;
                return report;
            }

            if (mismatched.size() > maxDrilldownBuckets) {
                // Too broad to drill down within budget. At EQUAL watermarks this is real mass
                // divergence (the child claims it ingested everything <= W yet differs).
                // At a CLAMPED watermark it may be entirely benign child-ahead keys (e.g. the
                // dev cloud's loopback replay re-serving the keyspace) — be honest about it.
                report.state = clamped ? State.INCONCLUSIVE : State.INCONSISTENT;
                report.refreshRecommended = !clamped;
                report.samples.add(mismatched.size() + "/" + buckets +
                        " buckets diverged — drill-down skipped (exceeds max-drilldown-buckets)" +
                        (clamped ? "; watermark was clamped, divergence may be benign child-ahead" : ""));
                return report;
            }

            drillDown(topic, targetUrl, report.effectiveWatermark, mismatched, report);
            boolean inconsistent = report.missingKeys + report.staleKeys
                    + report.zombieKeys + report.fabricatedKeys > 0;
            report.state = inconsistent ? State.INCONSISTENT
                    : (clamped ? State.CONSISTENT_UP_TO : State.CONSISTENT);
            report.refreshRecommended = inconsistent;
            return report;

        } catch (UnsupportedParentException e) {
            report.state = State.UNSUPPORTED_PARENT;
            report.error = e.getMessage();
            return report;
        } catch (Exception e) {
            report.state = looksLikeNetworkOutage(e) ? State.UNREACHABLE : State.ERROR;
            report.error = e.getMessage();
            log.warn("event=pipe_consistency.check_failed topic={} target={} state={}",
                    topic, targetUrl, report.state, e);
            return report;
        } finally {
            report.durationMs = System.currentTimeMillis() - startMs;
        }
    }

    /**
     * A reachability failure (parent offline) → UNREACHABLE; anything else → ERROR. Walks the
     * whole cause chain so the verdict is the same whether the failure arrives raw or wrapped in
     * a {@link com.messaging.common.exception.NetworkException} from {@link ParentConsistencyClient}.
     */
    private static boolean looksLikeNetworkOutage(Throwable e) {
        for (Throwable t = e; t != null; t = t.getCause()) {
            if (t instanceof java.io.IOException) {
                return true;
            }
            String m = t.getMessage();
            if (m != null && (m.contains("Connect") || m.contains("connect")
                    || m.contains("refused") || m.contains("timed out"))) {
                return true;
            }
            if (t.getCause() == t) {
                break;  // guard against self-referential cause chains
            }
        }
        return false;
    }

    private List<Integer> mismatchedBuckets(KeyspaceDigest.Result mine, DigestResponse parent) {
        List<Integer> mismatched = new ArrayList<>();
        int n = Math.min(mine.digests.length, parent.digests.length);
        for (int i = 0; i < n; i++) {
            boolean countDiffers = i < parent.counts.length && parent.counts[i] != mine.counts[i];
            if (mine.digests[i] != parent.digests[i] || countDiffers) {
                mismatched.add(i);
            }
        }
        return mismatched;
    }

    /**
     * Compare the mismatched buckets entry-by-entry and classify every difference.
     * Memory is bounded: only entries belonging to the (≤ maxDrilldownBuckets) mismatched
     * buckets are held, and classification lists are capped at maxClassifyEntries.
     */
    private void drillDown(String topic, String targetUrl, long watermark,
                           List<Integer> mismatched, PipeConsistencyReport report) {
        // One pass over own index collecting entries for the mismatched buckets only.
        // Deliberately UNFILTERED by watermark: when the comparison runs at a clamped
        // watermark (lagging parent, or the cloud's loopback clamp), a key this child has
        // already advanced PAST the clamp is absent from the child's clamped digest but
        // present in the parent's — without the unfiltered view it would be misclassified
        // as "missing on child" (false INCONSISTENT). With it, the candidate is recognised
        // as benign child-ahead.
        Map<Integer, Map<Long, ChildEntry>> mineByBucket = new HashMap<>();
        for (int b : mismatched) mineByBucket.put(b, new HashMap<>());
        compactionIndex.forEachEntry(topic, (msgKey, latestOffset, ts) -> {
            long hash = KeyspaceDigest.hash64(msgKey);
            Map<Long, ChildEntry> bucket = mineByBucket.get(KeyspaceDigest.bucketOf(hash, buckets));
            if (bucket != null) {
                bucket.put(hash, new ChildEntry(msgKey, latestOffset));
            }
        });

        // Suspicion lists (classified with one batched call at the end).
        List<long[]> missingCandidates = new ArrayList<>(); // {keyHash, parentOffset}
        List<long[]> staleCandidates = new ArrayList<>();   // {keyHash, parentOffset, childOffset}
        List<String> extraCandidates = new ArrayList<>();   // child-only msgKeys

        // One request — the parent serves all mismatched buckets from a single index scan.
        Map<Integer, List<BucketEntry>> parentBuckets =
                client.fetchBuckets(targetUrl, topic, watermark, buckets, mismatched);

        for (int b : mismatched) {
            Map<Long, ChildEntry> mine = mineByBucket.get(b);
            for (BucketEntry parentEntry : parentBuckets.getOrDefault(b, List.of())) {
                ChildEntry childEntry = mine.remove(parentEntry.keyHash);
                if (childEntry == null) {
                    missingCandidates.add(new long[]{parentEntry.keyHash, parentEntry.offset});
                } else if (childEntry.offset > watermark) {
                    // Child already advanced this key past the (clamped) comparison watermark —
                    // it is ahead, not missing. Benign by construction.
                    report.childNewerKeys++;
                } else if (childEntry.offset < parentEntry.offset) {
                    staleCandidates.add(new long[]{
                            parentEntry.keyHash, parentEntry.offset, childEntry.offset});
                } else if (childEntry.offset > parentEntry.offset) {
                    report.childNewerKeys++;
                    addSample(report, "child-newer keyHash=" + parentEntry.keyHash
                            + " child=" + childEntry.offset + " parent=" + parentEntry.offset);
                }
                // equal offsets: same state, not a problem (count asymmetry elsewhere)
            }

            // Whatever the parent never mentioned is child-extra. Entries above the watermark
            // are outside the comparison window (both digests excluded them) — skip.
            for (ChildEntry left : mine.values()) {
                if (left.offset <= watermark) {
                    extraCandidates.add(left.msgKey);
                }
            }
        }

        classify(topic, targetUrl, watermark, missingCandidates, staleCandidates,
                extraCandidates, report);
    }

    private void classify(String topic, String targetUrl, long watermark,
                          List<long[]> missingCandidates, List<long[]> staleCandidates,
                          List<String> extraCandidates, PipeConsistencyReport report) {
        boolean truncated = missingCandidates.size() + staleCandidates.size() > maxClassifyEntries
                || extraCandidates.size() > maxClassifyEntries;
        List<long[]> missing = cap(missingCandidates, maxClassifyEntries);
        List<long[]> stale = cap(staleCandidates,
                Math.max(0, maxClassifyEntries - missing.size()));
        List<String> extras = cap(extraCandidates, maxClassifyEntries);
        if (truncated) {
            addSample(report, "classification truncated to " + maxClassifyEntries + " entries");
        }

        List<Long> offsets = new ArrayList<>(missing.size() + stale.size());
        missing.forEach(c -> offsets.add(c[1]));
        stale.forEach(c -> offsets.add(c[1]));

        if (offsets.isEmpty() && extras.isEmpty()) {
            return;
        }
        ClassifyResponse classified = client.classify(targetUrl, topic, watermark, offsets, extras);

        for (long[] candidate : missing) {
            if (Boolean.TRUE.equals(classified.offsets.get(candidate[1]))) {
                // Parent's record is physically present and the child never stored it.
                report.missingKeys++;
                addSample(report, "missing keyHash=" + candidate[0] + " parentOffset=" + candidate[1]);
            }
            // else: parent's latest record was compacted away (expired tombstone) and the child
            // never held any version — both sides agree the key is dead. Benign.
        }
        for (long[] candidate : stale) {
            if (Boolean.TRUE.equals(classified.offsets.get(candidate[1]))) {
                report.staleKeys++;
                addSample(report, "stale keyHash=" + candidate[0]
                        + " parentOffset=" + candidate[1] + " childOffset=" + candidate[2]);
            } else {
                // Parent's latest was a now-expired tombstone the child never received — the key
                // is dead upstream but the child still serves an old version: zombie.
                report.zombieKeys++;
                addSample(report, "zombie keyHash=" + candidate[0] + " childOffset=" + candidate[2]);
            }
        }
        for (String key : extras) {
            KeyState state = classified.keys.get(key);
            if (state == KeyState.PRESENT_BEYOND_WATERMARK
                    || state == KeyState.PRESENT_AT_OR_BELOW_WATERMARK) {
                // Beyond-watermark: pure lag. At-or-below: ingest raced the check — also benign
                // (the next check converges).
                report.laggingKeys++;
            } else if (classified.authoritative) {
                // An AUTHORITATIVE verifier (the cloud — complete, never-expiring keyspace)
                // has no entry at all: this key cannot legitimately exist anywhere in the
                // fleet. Fabricated / corrupted index — a real inconsistency.
                report.fabricatedKeys++;
                addSample(report, "fabricated key=" + key + " (authoritative verifier never had it)");
            } else {
                report.extraKeys++;
                addSample(report, "extra key=" + key + " (parent has no entry — lineage artifact)");
            }
        }
    }

    // ── Bookkeeping ───────────────────────────────────────────────────────────

    private void record(PipeConsistencyReport report) {
        checksTotal.increment();
        if (report.isInconsistent()) {
            inconsistentTotal.increment();
        }
        latestByTopic.put(report.topic, report);
        synchronized (history) {
            history.add(report);
            while (history.size() > MAX_HISTORY) {
                history.remove(0);
            }
        }
        stateGauges.computeIfAbsent(report.topic, topic -> {
            AtomicInteger gauge = new AtomicInteger();
            Gauge.builder("pipe.consistency.state", gauge, AtomicInteger::get)
                    .description("0=consistent 1=inconsistent 2=unreachable/unsupported/error")
                    .tag("topic", topic)
                    .register(meterRegistry);
            return gauge;
        }).set(switch (report.state) {
            case CONSISTENT, CONSISTENT_UP_TO -> 0;
            case INCONSISTENT -> 1;
            default -> 2; // INCONCLUSIVE / UNREACHABLE / UNSUPPORTED_PARENT / ERROR
        });
        checkDuration.record(java.time.Duration.ofMillis(report.durationMs));
        topicGauge(missingGauges, "pipe.consistency.missing.keys",
                "Missing+stale keys found by the last check", report.topic)
                .set(report.missingKeys + report.staleKeys);
        topicGauge(zombieGauges, "pipe.consistency.zombie.keys",
                "Zombie keys (deletes missed while offline) found by the last check", report.topic)
                .set(report.zombieKeys);
        topicGauge(fabricatedGauges, "pipe.consistency.fabricated.keys",
                "Fabricated keys (an authoritative verifier never had) found by the last check", report.topic)
                .set(report.fabricatedKeys);
        topicGauge(lastCheckEpoch, "pipe.consistency.last.check.epoch",
                "Epoch seconds of the last completed check", report.topic)
                .set(report.checkedAtMs / 1000);
        log.info("event=pipe_consistency.report topic={} state={} watermark={} effectiveWatermark={} " +
                        "missing={} stale={} zombie={} lagging={} extra={} childNewer={} durationMs={}",
                report.topic, report.state, report.watermark, report.effectiveWatermark,
                report.missingKeys, report.staleKeys, report.zombieKeys,
                report.laggingKeys, report.extraKeys, report.childNewerKeys, report.durationMs);
    }

    private java.util.concurrent.atomic.AtomicLong topicGauge(
            Map<String, java.util.concurrent.atomic.AtomicLong> holder,
            String name, String description, String topic) {
        return holder.computeIfAbsent(topic, t -> {
            java.util.concurrent.atomic.AtomicLong gauge = new java.util.concurrent.atomic.AtomicLong();
            Gauge.builder(name, gauge, java.util.concurrent.atomic.AtomicLong::get)
                    .description(description)
                    .tag("topic", t)
                    .register(meterRegistry);
            return gauge;
        });
    }

    public Map<String, PipeConsistencyReport> latestReports() {
        return new HashMap<>(latestByTopic);
    }

    public List<PipeConsistencyReport> reportHistory() {
        synchronized (history) {
            return new ArrayList<>(history);
        }
    }

    private String resolveTarget(String target) {
        if (TARGET_CLOUD.equalsIgnoreCase(target)) {
            return cloudUrl;
        }
        return stripTrailingSlash(topologyManager.getCurrentParentUrl());
    }

    private static String stripTrailingSlash(String url) {
        if (url == null) return null;
        return url.endsWith("/") ? url.substring(0, url.length() - 1) : url;
    }

    private static <T> List<T> cap(List<T> list, int max) {
        return list.size() <= max ? list : list.subList(0, max);
    }

    private static void addSample(PipeConsistencyReport report, String sample) {
        if (report.samples.size() < MAX_SAMPLES) {
            report.samples.add(sample);
        }
    }

    private record ChildEntry(String msgKey, long offset) {}
}
