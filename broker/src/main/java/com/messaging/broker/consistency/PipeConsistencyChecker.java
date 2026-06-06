package com.messaging.broker.consistency;

import com.messaging.broker.core.TopologyManager;
import com.messaging.common.api.StorageEngine;
import com.messaging.common.hash.RecordHasher;
import io.micronaut.context.annotation.Value;
import jakarta.inject.Singleton;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashSet;
import java.util.HexFormat;
import java.util.List;
import java.util.Set;

/**
 * Orchestrates HOP and DEEP audits for a single (topic, partition).
 *
 * <p>HOP — compares against the immediate parent for each lineage subrange. Cheap,
 * frequent. Catches drift introduced on the most recent pipe leg.
 *
 * <p>DEEP — walks parent-by-parent toward root/cloud, identifying the first divergent
 * hop. Catches waterfall corruption where an intermediate parent is wrong but its
 * children faithfully copied that wrongness.
 *
 * <p>This class is diagnostic only. It NEVER reassigns parents or alters polling.
 */
@Singleton
public class PipeConsistencyChecker {
    private static final Logger LOG = LoggerFactory.getLogger(PipeConsistencyChecker.class);

    private final StorageEngine storage;
    private final BrokerSegmentHashView hashView;
    private final HashCache hashCache;
    private final PipeLineageStore lineageStore;
    private final TopologyManager topology;
    private final UpstreamConsistencyClient upstream;
    private final PipeConsistencyReportStore reportStore;
    private final PipeConsistencyMetrics metrics;
    private final int maxHops;
    private final int drillDownBatch;
    private final int maxMismatchReport;
    private final int chunkSize;
    private final int maxDrilledChunksPerAudit;

    public PipeConsistencyChecker(
            StorageEngine storage,
            BrokerSegmentHashView hashView,
            HashCache hashCache,
            PipeLineageStore lineageStore,
            TopologyManager topology,
            UpstreamConsistencyClient upstream,
            PipeConsistencyReportStore reportStore,
            PipeConsistencyMetrics metrics,
            @Value("${pipe.consistency.deep.max-hops:8}") int maxHops,
            @Value("${pipe.consistency.drill-down.batch-size:1000}") int drillDownBatch,
            @Value("${pipe.consistency.drill-down.max-mismatch-report:100}") int maxMismatchReport,
            @Value("${pipe.consistency.chunk-size:10000}") int chunkSize,
            @Value("${pipe.consistency.drill-down.max-chunks-per-audit:5}") int maxDrilledChunksPerAudit
    ) {
        this.storage = storage;
        this.hashView = hashView;
        this.hashCache = hashCache;
        this.lineageStore = lineageStore;
        this.topology = topology;
        this.upstream = upstream;
        this.reportStore = reportStore;
        this.metrics = metrics;
        this.maxHops = Math.max(1, maxHops);
        this.drillDownBatch = Math.max(50, drillDownBatch);
        this.maxMismatchReport = Math.max(10, maxMismatchReport);
        this.chunkSize = Math.max(1000, chunkSize);
        this.maxDrilledChunksPerAudit = Math.max(1, maxDrilledChunksPerAudit);
    }

    public Set<String> discoverTopics() {
        return storage.getTopicNames();
    }

    /**
     * HOP audit for (topic, partition=0).
     */
    public PipeConsistencyReport runHop(String topic) {
        int partition = 0;
        PipeConsistencyReport.Builder b = PipeConsistencyReport.builder(topic, partition, PipeConsistencyReport.Mode.HOP);

        long localMax = storage.getCurrentOffset(topic, partition);
        if (localMax < 0) {
            b.status(PipeConsistencyReport.Status.CONSISTENT);
            reportStore.save(b.build());
            return b.build();
        }
        long localEarliest = storage.getEarliestOffset(topic, partition);
        if (localEarliest < 0) localEarliest = 0L;

        List<PipeLineageStore.Subrange> subranges = lineageStore.resolve(localEarliest, localMax);
        if (subranges.isEmpty()) {
            // No lineage knowledge — fall back to "compare against current parent over the whole range"
            String parent = topology.getCurrentParentUrl();
            if (parent == null) {
                b.status(PipeConsistencyReport.Status.ERROR).errorMessage("no parent assigned");
                PipeConsistencyReport r = b.build();
                reportStore.save(r);
                return r;
            }
            subranges = List.of(new PipeLineageStore.Subrange(localEarliest, localMax, parent));
        }

        boolean anyMismatch = false;
        boolean anyError = false;
        boolean anyStale = false;
        long totalMissing = 0;
        long totalExtra = 0;
        // Global cap on drill-downs so a many-chunk topic can't hammer upstream into OOM
        // even if every chunk mismatches. Mismatches beyond the cap still flip status to
        // MISMATCH but drill-down skipped; truncated=true tells operators to re-run with
        // a narrower offset window or raise the cap.
        int drilledChunks = 0;

        for (PipeLineageStore.Subrange sub : subranges) {
            // Clamp to upstream's head so we don't compare against ranges the parent hasn't produced yet.
            long upstreamMax = upstream.fetchMaxOffset(sub.parentUrl, topic);
            if (upstreamMax < 0) {
                // Upstream unreachable — mark stale rather than ERROR for the whole topic.
                anyStale = true;
                LOG.info("HOP: subrange [{},{}] parent {} unreachable, marking stale",
                        sub.fromOffsetInclusive, sub.toOffsetInclusive, sub.parentUrl);
                continue;
            }
            long clampedHi = Math.min(sub.toOffsetInclusive, upstreamMax);
            if (clampedHi < sub.fromOffsetInclusive) {
                continue; // upstream lags this entire subrange; nothing to compare yet
            }

            // Chunk the subrange so we never ask upstream for a window bigger than
            // pipe.consistency.chunk-size — keeps cloud-side memory bounded and lets
            // us short-circuit on the first chunk that differs.
            long chunkFrom = sub.fromOffsetInclusive;
            while (chunkFrom <= clampedHi) {
                long chunkTo = Math.min(clampedHi, chunkFrom + chunkSize - 1);
                BrokerSegmentHashView.Computed local = hashView.computeHash(topic, chunkFrom, chunkTo);
                String projection = local.maxCompactionEpoch > 0 ? "compacted" : "raw";

                UpstreamConsistencyClient.HashResponse up = upstream.fetchHash(
                        sub.parentUrl, topic, chunkFrom, chunkTo, projection);
                if (!up.ok) {
                    anyError = true;
                    chunkFrom = chunkTo + 1;
                    continue;
                }
                b.comparedUpstream(up.nodeId, sub.parentUrl);

                boolean countDiffers = local.recordCount != up.recordCount;
                boolean hashDiffers = !countDiffers && !java.util.Arrays.equals(local.hash, up.hash);
                if (countDiffers || hashDiffers) {
                    anyMismatch = true;
                    if (countDiffers) {
                        totalMissing += Math.max(0, up.recordCount - local.recordCount);
                        totalExtra   += Math.max(0, local.recordCount - up.recordCount);
                    }
                    if (drilledChunks < maxDrilledChunksPerAudit) {
                        drillDown(topic, chunkFrom, chunkTo, sub.parentUrl, b);
                        drilledChunks++;
                    } else {
                        // Skip drill-down for this chunk; bound on upstream load.
                        b.truncated(true);
                        b.addMismatchedSegment(new PipeConsistencyReport.SegmentSummary(
                                chunkFrom, chunkTo, null, null, local.recordCount, up.recordCount));
                    }
                }
                chunkFrom = chunkTo + 1;
            }
        }

        if (anyError) {
            b.status(PipeConsistencyReport.Status.ERROR).errorMessage("one or more subranges failed");
        } else if (anyMismatch) {
            b.status(PipeConsistencyReport.Status.MISMATCH);
        } else if (anyStale) {
            b.status(PipeConsistencyReport.Status.LINEAGE_STALE);
            metrics.incrementLineageStale(topic);
        } else {
            b.status(PipeConsistencyReport.Status.CONSISTENT);
        }
        PipeConsistencyReport r = b.build();
        reportStore.save(r);
        metrics.recordAuditResult(r.status.name().toLowerCase(), "hop", r.comparedUpstreamNodeId);
        metrics.setLastCheckTimestamp(topic, "hop", System.currentTimeMillis() / 1000L);
        metrics.setStatus(topic, "hop", statusToCode(r.status));
        metrics.addMismatchSegments(topic, "hop", r.mismatchedSegments.size());
        metrics.addMissingRecords(topic, "hop", r.missingOnBroker.size());
        metrics.addExtraRecords(topic, "hop", r.extraOnBroker.size());
        metrics.addDataMismatchRecords(topic, "hop", r.dataMismatch.size());
        metrics.setLatestBreakdown(topic, "hop",
                r.missingOnBroker.size(),
                r.extraOnBroker.size(),
                r.dataMismatch.size(),
                r.mismatchedSegments.size());
        return r;
    }

    private static int statusToCode(PipeConsistencyReport.Status s) {
        switch (s) {
            case CONSISTENT: return 0;
            case MISMATCH: return 1;
            case ERROR: return 2;
            case LINEAGE_STALE: return 3;
            case DEEP_WALK_ABORTED: return 4;
            case UPSTREAM_UNSUPPORTED: return 5;
            default: return 2;
        }
    }

    /**
     * DEEP audit walks parent-by-parent toward the chain root, then identifies the FIRST hop
     * (lowest in the chain) whose hash differs from its own upstream's hash. That is the node
     * at which divergence was introduced relative to its parent — not "which ancestor differs
     * from me", which would always blame the immediate parent under waterfall corruption.
     *
     * <p>Algorithm:
     * <ol>
     *   <li>Compute local hash at {@code [localEarliest, localMax]}.</li>
     *   <li>Walk ancestors, fetching each one's hash at the SAME range. Stop at root,
     *       {@code maxHops}, or a visited-nodeId cycle.</li>
     *   <li>Build {@code chain = [local, parent, grandparent, …, root]}.</li>
     *   <li>Scan consecutive pairs. The first {@code i} where {@code chain[i].hash != chain[i+1].hash}
     *       means {@code chain[i]} (the lower node) carries different data than its parent —
     *       so divergence was introduced at {@code chain[i]}.</li>
     * </ol>
     *
     * <p>If the lowest pair (local vs immediate parent) is the first to differ, the local broker
     * itself is the divergence point. If all pairs match, the chain is consistent.
     */
    public PipeConsistencyReport runDeep(String topic) {
        int partition = 0;
        PipeConsistencyReport.Builder b = PipeConsistencyReport.builder(topic, partition, PipeConsistencyReport.Mode.DEEP);

        long localMax = storage.getCurrentOffset(topic, partition);
        if (localMax < 0) {
            b.status(PipeConsistencyReport.Status.CONSISTENT);
            PipeConsistencyReport noop = b.build();
            reportStore.save(noop);
            return noop;
        }
        long localEarliest = storage.getEarliestOffset(topic, partition);
        if (localEarliest < 0) localEarliest = 0L;

        // DEEP runs a single hash request per hop, so we must bound the range to chunkSize.
        // Audit the MOST RECENT window — operators care more about "is the latest data
        // already corrupted in the chain?" than auditing the full history.
        long deepFrom = Math.max(localEarliest, localMax - chunkSize + 1);

        String parent = topology.getCurrentParentUrl();
        if (parent == null) {
            b.status(PipeConsistencyReport.Status.ERROR).errorMessage("no parent assigned");
            PipeConsistencyReport r = b.build();
            reportStore.save(r);
            return r;
        }

        BrokerSegmentHashView.Computed local = hashView.computeHash(topic, deepFrom, localMax);
        b.localRoot(hex(local.hash));
        String projection = local.maxCompactionEpoch > 0 ? "compacted" : "raw";

        String localNodeId = topology.getNodeId();
        java.util.List<ChainHop> chain = new java.util.ArrayList<>();
        chain.add(new ChainHop(localNodeId, /*url*/ null, local.hash));

        Set<String> visited = new HashSet<>();
        if (localNodeId != null) visited.add(localNodeId);
        String hopUrl = parent;
        int hops = 0;
        boolean aborted = false;
        String abortReason = null;

        while (hopUrl != null && hops < maxHops) {
            hops++;
            long upMax = upstream.fetchMaxOffset(hopUrl, topic);
            if (upMax < 0) {
                aborted = true;
                abortReason = "hop " + hopUrl + " unreachable";
                break;
            }
            long clampedHi = Math.min(localMax, upMax);
            if (clampedHi < deepFrom) {
                break; // upstream hasn't produced data in our recent-window yet
            }
            UpstreamConsistencyClient.HashResponse resp = upstream.fetchHash(
                    hopUrl, topic, deepFrom, clampedHi, projection);
            if (!resp.ok) {
                aborted = true;
                abortReason = "hop " + hopUrl + " hash fetch failed";
                break;
            }
            if (resp.nodeId != null && !visited.add(resp.nodeId)) {
                aborted = true;
                abortReason = "cycle detected at nodeId=" + resp.nodeId;
                break;
            }
            chain.add(new ChainHop(resp.nodeId, hopUrl, resp.hash));
            hopUrl = resp.parentUrl;
        }

        boolean hitMaxHops = (hopUrl != null && hops >= maxHops);

        // Identify the first divergent hop: walk pairs ascending, lowest pair first.
        // The LOWER node of the first differing pair introduced the divergence.
        String firstDivergentHopNodeId = null;
        String firstDivergentHopUrl = null;
        byte[] firstDivergentUpstreamHash = null;
        for (int i = 0; i < chain.size() - 1; i++) {
            byte[] lower = chain.get(i).hash;
            byte[] upper = chain.get(i + 1).hash;
            if (!java.util.Arrays.equals(lower, upper)) {
                firstDivergentHopNodeId = chain.get(i).nodeId;
                firstDivergentHopUrl = chain.get(i).url;          // null when divergence is at the local broker
                firstDivergentUpstreamHash = upper;
                break;
            }
        }

        // Surface the topmost (root-most) hop's hash on the report for human readability.
        if (chain.size() > 1) {
            b.upstreamRoot(hex(chain.get(chain.size() - 1).hash));
            ChainHop topHop = chain.get(chain.size() - 1);
            b.comparedUpstream(topHop.nodeId, topHop.url);
        }

        if (aborted) {
            b.status(PipeConsistencyReport.Status.ERROR).errorMessage(abortReason);
        } else if (hitMaxHops) {
            b.status(PipeConsistencyReport.Status.DEEP_WALK_ABORTED)
                    .errorMessage("max-hops (" + maxHops + ") reached at " + hopUrl);
        } else if (firstDivergentHopNodeId != null) {
            b.status(PipeConsistencyReport.Status.MISMATCH)
                    .firstDivergentHop(firstDivergentHopNodeId);
            if (firstDivergentUpstreamHash != null) {
                // Re-purpose upstreamRoot to point at the divergent hop's upstream so operators
                // see the exact hash boundary where data changed.
                b.upstreamRoot(hex(firstDivergentUpstreamHash));
            }
            if (firstDivergentHopUrl != null) {
                b.comparedUpstream(firstDivergentHopNodeId, firstDivergentHopUrl);
            }
        } else {
            b.status(PipeConsistencyReport.Status.CONSISTENT);
        }

        PipeConsistencyReport r = b.build();
        reportStore.save(r);
        metrics.recordAuditResult(r.status.name().toLowerCase(), "deep", r.comparedUpstreamNodeId);
        metrics.setLastCheckTimestamp(topic, "deep", System.currentTimeMillis() / 1000L);
        metrics.setStatus(topic, "deep", statusToCode(r.status));
        metrics.setLatestBreakdown(topic, "deep",
                r.missingOnBroker.size(),
                r.extraOnBroker.size(),
                r.dataMismatch.size(),
                r.mismatchedSegments.size());
        // Always set the gauge — pass null to clear it when this run is consistent,
        // errored, or aborted, so the dashboard doesn't show yesterday's bad hop forever.
        metrics.setFirstDivergentHop(topic, firstDivergentHopNodeId);
        if (r.status == PipeConsistencyReport.Status.DEEP_WALK_ABORTED) {
            metrics.incrementDeepWalkAborted(topic);
        }
        return r;
    }

    private static final class ChainHop {
        final String nodeId;
        final String url;         // null for the local broker (index 0 in the chain)
        final byte[] hash;

        ChainHop(String nodeId, String url, byte[] hash) {
            this.nodeId = nodeId;
            this.url = url;
            this.hash = hash;
        }
    }

    private void drillDown(String topic, long fromOffset, long toOffset, String parentUrl,
                            PipeConsistencyReport.Builder report) {
        // Page records from both sides and diff by (offset, msgKey + record CRC).
        int pageSize = drillDownBatch;
        long pagesNeeded = (toOffset - fromOffset + 1 + pageSize - 1) / pageSize;
        int recorded = 0;
        long localCountInChunk = 0;
        long upstreamCountInChunk = 0;
        for (long page = 0; page < pagesNeeded; page++) {
            if (recorded >= maxMismatchReport) {
                report.truncated(true);
                return;
            }
            // Per-topic drill-down is legacy/dead-code (admin endpoints route to global).
            // Pass cursor=null on every iteration — the page loop variable here is the
            // legacy semantic and would yield the same records on every call if it ever
            // ran. Keeping the method compilable; the global drill-down is the real path.
            List<UpstreamConsistencyClient.RangeRecord> upstreamPage =
                    upstream.fetchRange(parentUrl, topic, fromOffset, toOffset, (Long) null, pageSize);
            java.util.Map<Long, UpstreamConsistencyClient.RangeRecord> upstreamByOffset = new java.util.HashMap<>();
            for (UpstreamConsistencyClient.RangeRecord r : upstreamPage) upstreamByOffset.put(r.offset, r);

            long pageFrom = fromOffset + page * pageSize;
            long pageTo = Math.min(toOffset, pageFrom + pageSize - 1);
            java.util.List<com.messaging.common.model.MessageRecord> localPage;
            try {
                localPage = storage.read(topic, 0, pageFrom, (int) (pageTo - pageFrom + 1));
            } catch (Exception e) {
                LOG.warn("drill-down local read failed: {}", e.getMessage());
                return;
            }
            java.util.Map<Long, com.messaging.common.model.MessageRecord> localByOffset = new java.util.HashMap<>();
            for (com.messaging.common.model.MessageRecord r : localPage) {
                // Storage.read returns records starting at the first available offset >= pageFrom,
                // which may be FAR past pageTo when the broker has no data inside the page. Only
                // index records actually inside the page so we don't fabricate "extras" that
                // belong to a later page.
                if (r.getOffset() >= pageFrom && r.getOffset() <= pageTo) {
                    localByOffset.put(r.getOffset(), r);
                }
            }
            localCountInChunk += localByOffset.size();
            upstreamCountInChunk += upstreamByOffset.size();

            // Iterate the UNION of offsets present on either side within this page. The old
            // implementation iterated [pageFrom..pageTo] sequentially, which classified nothing
            // when both sides had data but at different offset sub-ranges inside the page.
            java.util.Set<Long> offsets = new java.util.TreeSet<>();
            offsets.addAll(upstreamByOffset.keySet());
            offsets.addAll(localByOffset.keySet());

            for (Long off : offsets) {
                if (recorded >= maxMismatchReport) break;
                UpstreamConsistencyClient.RangeRecord up = upstreamByOffset.get(off);
                com.messaging.common.model.MessageRecord loc = localByOffset.get(off);
                if (up != null && loc == null) {
                    report.addMissingOnBroker(new PipeConsistencyReport.RecordDiff(off, up.msgKey, "missing on broker"));
                    recorded++;
                } else if (loc != null && up == null) {
                    report.addExtraOnBroker(new PipeConsistencyReport.RecordDiff(off, loc.getMsgKey(), "extra on broker"));
                    recorded++;
                } else if (loc != null && up != null) {
                    int localCrc = RecordHasher.recordCrc(loc.getOffset(), loc.getMsgKey(),
                            (char) loc.getEventType().getCode(), loc.getData());
                    int upCrc = RecordHasher.recordCrc(up.offset, up.msgKey, up.eventTypeCode, up.data);
                    if (localCrc != upCrc) {
                        report.addDataMismatch(new PipeConsistencyReport.RecordDiff(off, loc.getMsgKey(),
                                "crc mismatch"));
                        recorded++;
                    }
                }
            }
        }
        // Record a chunk-level summary so the dashboard's "Segments Affected" column reflects
        // EVERY drilled chunk, even ones where the record-by-record diff classified zero records
        // (e.g. broker and upstream both empty in the page windows, but recordCount headlines differed).
        report.addMismatchedSegment(new PipeConsistencyReport.SegmentSummary(
                fromOffset, toOffset, null, null, localCountInChunk, upstreamCountInChunk));
    }

    private static String hex(byte[] bytes) {
        if (bytes == null) return null;
        return HexFormat.of().formatHex(bytes);
    }

    // ── Global (single-stream) audits ───────────────────────────────────────────
    // The pipe is one global cloud-offset stream; per-topic broker comparisons are
    // apples-to-oranges. These methods audit the broker's full storage (merged
    // across all topics) against cloud's global offset range. The synthetic
    // pseudo-topic "__global__" is used in reports/metrics so the persisted shape
    // is uniform with the older per-topic schema.

    public static final String GLOBAL_SCOPE = "__global__";

    public PipeConsistencyReport runHopGlobal() {
        long startedAtNanos = System.nanoTime();
        try {
            return runHopGlobalInner();
        } finally {
            metrics.auditTimer(GLOBAL_SCOPE, "hop")
                    .record(java.time.Duration.ofNanos(System.nanoTime() - startedAtNanos));
        }
    }

    private PipeConsistencyReport runHopGlobalInner() {
        // Reset all per-topic breakdown gauges before this audit. The drill-down
        // will repopulate gauges for topics that ACTUALLY have drift. Topics with no
        // findings stay at zero — otherwise stale values from prior audits linger
        // forever in Prometheus and the dashboard reports phantom mismatches.
        resetAllPerTopicGauges("hop");

        PipeConsistencyReport.Builder b = PipeConsistencyReport.builder(
                GLOBAL_SCOPE, 0, PipeConsistencyReport.Mode.HOP);

        long localMax = hashView.globalMaxOffset();
        if (localMax < 0) {
            b.status(PipeConsistencyReport.Status.CONSISTENT);
            PipeConsistencyReport r = b.build();
            reportStore.save(r);
            metrics.setStatus(GLOBAL_SCOPE, "hop", statusToCode(r.status));
            metrics.setLastCheckTimestamp(GLOBAL_SCOPE, "hop", System.currentTimeMillis() / 1000L);
            return r;
        }
        long localEarliest = hashView.globalEarliestOffset();
        if (localEarliest < 0) localEarliest = 0L;

        List<PipeLineageStore.Subrange> subranges = lineageStore.resolve(localEarliest, localMax);
        if (subranges.isEmpty()) {
            String parent = topology.getCurrentParentUrl();
            if (parent == null) {
                b.status(PipeConsistencyReport.Status.ERROR).errorMessage("no parent assigned");
                PipeConsistencyReport r = b.build();
                reportStore.save(r);
                return r;
            }
            subranges = List.of(new PipeLineageStore.Subrange(localEarliest, localMax, parent));
        }

        boolean anyMismatch = false;
        boolean anyError = false;
        boolean anyStale = false;
        int drilledChunks = 0;
        java.util.HashMap<String, Long> perTopicMissing = new java.util.HashMap<>();
        java.util.HashMap<String, Long> perTopicExtra = new java.util.HashMap<>();
        java.util.HashMap<String, Long> perTopicDataMismatch = new java.util.HashMap<>();

        for (PipeLineageStore.Subrange sub : subranges) {
            long upstreamMax = upstream.fetchMaxOffset(sub.parentUrl, GLOBAL_SCOPE);
            if (upstreamMax < 0) {
                anyStale = true;
                LOG.info("HOP-global: subrange [{},{}] parent {} unreachable, marking stale",
                        sub.fromOffsetInclusive, sub.toOffsetInclusive, sub.parentUrl);
                continue;
            }
            long clampedHi = Math.min(sub.toOffsetInclusive, upstreamMax);
            if (clampedHi < sub.fromOffsetInclusive) continue;

            long chunkFrom = sub.fromOffsetInclusive;
            while (chunkFrom <= clampedHi) {
                long chunkTo = Math.min(clampedHi, chunkFrom + chunkSize - 1);
                BrokerSegmentHashView.Computed local = hashView.computeGlobalHash(chunkFrom, chunkTo);
                String projection = local.maxCompactionEpoch > 0 ? "compacted" : "raw";

                UpstreamConsistencyClient.HashResponse up = upstream.fetchHash(
                        sub.parentUrl, GLOBAL_SCOPE, chunkFrom, chunkTo, projection);
                if (!up.ok) {
                    anyError = true;
                    chunkFrom = chunkTo + 1;
                    continue;
                }
                b.comparedUpstream(up.nodeId, sub.parentUrl);

                boolean countDiffers = local.recordCount != up.recordCount;
                boolean hashDiffers = !countDiffers && !java.util.Arrays.equals(local.hash, up.hash);
                if (countDiffers || hashDiffers) {
                    anyMismatch = true;
                    if (drilledChunks < maxDrilledChunksPerAudit) {
                        drillDownGlobal(chunkFrom, chunkTo, sub.parentUrl, b,
                                perTopicMissing, perTopicExtra, perTopicDataMismatch);
                        drilledChunks++;
                    } else {
                        b.truncated(true);
                        b.addMismatchedSegment(new PipeConsistencyReport.SegmentSummary(
                                chunkFrom, chunkTo, null, null, local.recordCount, up.recordCount));
                    }
                }
                chunkFrom = chunkTo + 1;
            }
        }

        if (anyError) {
            b.status(PipeConsistencyReport.Status.ERROR).errorMessage("one or more subranges failed");
        } else if (anyMismatch) {
            b.status(PipeConsistencyReport.Status.MISMATCH);
        } else if (anyStale) {
            b.status(PipeConsistencyReport.Status.LINEAGE_STALE);
            metrics.incrementLineageStale(GLOBAL_SCOPE);
        } else {
            b.status(PipeConsistencyReport.Status.CONSISTENT);
        }
        PipeConsistencyReport r = b.build();
        reportStore.save(r);

        // Global summary metrics
        metrics.recordAuditResult(r.status.name().toLowerCase(), "hop", r.comparedUpstreamNodeId);
        metrics.setLastCheckTimestamp(GLOBAL_SCOPE, "hop", System.currentTimeMillis() / 1000L);
        metrics.setStatus(GLOBAL_SCOPE, "hop", statusToCode(r.status));
        metrics.setLatestBreakdown(GLOBAL_SCOPE, "hop",
                r.missingOnBroker.size(),
                r.extraOnBroker.size(),
                r.dataMismatch.size(),
                r.mismatchedSegments.size());

        // Cumulative counters at the global scope so the over-time panels have data.
        metrics.addMissingRecords(GLOBAL_SCOPE, "hop", r.missingOnBroker.size());
        metrics.addExtraRecords(GLOBAL_SCOPE, "hop", r.extraOnBroker.size());
        metrics.addDataMismatchRecords(GLOBAL_SCOPE, "hop", r.dataMismatch.size());
        metrics.addMismatchSegments(GLOBAL_SCOPE, "hop", r.mismatchedSegments.size());

        // Per-topic breakdown from drill-down: tells operators which topics carry the
        // missing/extra/wrong-content records. Bucketed from the record's internal data->topic.
        publishPerTopicBreakdown("hop", perTopicMissing, perTopicExtra, perTopicDataMismatch);
        return r;
    }

    public PipeConsistencyReport runDeepGlobal() {
        long startedAtNanos = System.nanoTime();
        try {
            return runDeepGlobalInner();
        } finally {
            metrics.auditTimer(GLOBAL_SCOPE, "deep")
                    .record(java.time.Duration.ofNanos(System.nanoTime() - startedAtNanos));
        }
    }

    private PipeConsistencyReport runDeepGlobalInner() {
        resetAllPerTopicGauges("deep");

        PipeConsistencyReport.Builder b = PipeConsistencyReport.builder(
                GLOBAL_SCOPE, 0, PipeConsistencyReport.Mode.DEEP);

        long localMax = hashView.globalMaxOffset();
        if (localMax < 0) {
            b.status(PipeConsistencyReport.Status.CONSISTENT);
            PipeConsistencyReport r = b.build();
            reportStore.save(r);
            metrics.setStatus(GLOBAL_SCOPE, "deep", statusToCode(r.status));
            return r;
        }
        long localEarliest = hashView.globalEarliestOffset();
        if (localEarliest < 0) localEarliest = 0L;

        long deepFrom = Math.max(localEarliest, localMax - chunkSize + 1);

        String parent = topology.getCurrentParentUrl();
        if (parent == null) {
            b.status(PipeConsistencyReport.Status.ERROR).errorMessage("no parent assigned");
            PipeConsistencyReport r = b.build();
            reportStore.save(r);
            return r;
        }

        BrokerSegmentHashView.Computed local = hashView.computeGlobalHash(deepFrom, localMax);
        b.localRoot(hex(local.hash));
        String projection = local.maxCompactionEpoch > 0 ? "compacted" : "raw";

        String localNodeId = topology.getNodeId();
        java.util.List<ChainHop> chain = new java.util.ArrayList<>();
        chain.add(new ChainHop(localNodeId, null, local.hash));

        Set<String> visited = new HashSet<>();
        if (localNodeId != null) visited.add(localNodeId);
        String hopUrl = parent;
        int hops = 0;
        boolean aborted = false;
        String abortReason = null;

        while (hopUrl != null && hops < maxHops) {
            hops++;
            long upMax = upstream.fetchMaxOffset(hopUrl, GLOBAL_SCOPE);
            if (upMax < 0) {
                aborted = true;
                abortReason = "hop " + hopUrl + " unreachable";
                break;
            }
            long clampedHi = Math.min(localMax, upMax);
            if (clampedHi < deepFrom) break;
            UpstreamConsistencyClient.HashResponse resp = upstream.fetchHash(
                    hopUrl, GLOBAL_SCOPE, deepFrom, clampedHi, projection);
            if (!resp.ok) {
                aborted = true;
                abortReason = "hop " + hopUrl + " hash fetch failed";
                break;
            }
            if (resp.nodeId != null && !visited.add(resp.nodeId)) {
                aborted = true;
                abortReason = "cycle detected at nodeId=" + resp.nodeId;
                break;
            }
            chain.add(new ChainHop(resp.nodeId, hopUrl, resp.hash));
            hopUrl = resp.parentUrl;
        }

        boolean hitMaxHops = (hopUrl != null && hops >= maxHops);

        String firstDivergentHopNodeId = null;
        byte[] firstDivergentUpstreamHash = null;
        String firstDivergentHopUrl = null;
        for (int i = 0; i < chain.size() - 1; i++) {
            if (!java.util.Arrays.equals(chain.get(i).hash, chain.get(i + 1).hash)) {
                firstDivergentHopNodeId = chain.get(i).nodeId;
                firstDivergentHopUrl = chain.get(i).url;
                firstDivergentUpstreamHash = chain.get(i + 1).hash;
                break;
            }
        }

        if (chain.size() > 1) {
            b.upstreamRoot(hex(chain.get(chain.size() - 1).hash));
            ChainHop topHop = chain.get(chain.size() - 1);
            b.comparedUpstream(topHop.nodeId, topHop.url);
        }

        if (aborted) {
            b.status(PipeConsistencyReport.Status.ERROR).errorMessage(abortReason);
        } else if (hitMaxHops) {
            b.status(PipeConsistencyReport.Status.DEEP_WALK_ABORTED)
                    .errorMessage("max-hops (" + maxHops + ") reached at " + hopUrl);
        } else if (firstDivergentHopNodeId != null) {
            b.status(PipeConsistencyReport.Status.MISMATCH).firstDivergentHop(firstDivergentHopNodeId);
            if (firstDivergentUpstreamHash != null) {
                b.upstreamRoot(hex(firstDivergentUpstreamHash));
            }
            if (firstDivergentHopUrl != null) {
                b.comparedUpstream(firstDivergentHopNodeId, firstDivergentHopUrl);
            }
        } else {
            b.status(PipeConsistencyReport.Status.CONSISTENT);
        }

        PipeConsistencyReport r = b.build();
        reportStore.save(r);
        metrics.recordAuditResult(r.status.name().toLowerCase(), "deep", r.comparedUpstreamNodeId);
        metrics.setLastCheckTimestamp(GLOBAL_SCOPE, "deep", System.currentTimeMillis() / 1000L);
        metrics.setStatus(GLOBAL_SCOPE, "deep", statusToCode(r.status));
        // Always update the gauge so it clears (→ 0) on CONSISTENT / ERROR / DEEP_WALK_ABORTED.
        // Otherwise the dashboard keeps yesterday's bad nodeId hash visible indefinitely.
        metrics.setFirstDivergentHop(GLOBAL_SCOPE, firstDivergentHopNodeId);
        if (r.status == PipeConsistencyReport.Status.DEEP_WALK_ABORTED) {
            metrics.incrementDeepWalkAborted(GLOBAL_SCOPE);
        }
        return r;
    }

    /**
     * Drill-down for the global audit: pages through the chunk, reads broker records
     * from EVERY topic, classifies each offset, and buckets diffs by the record's
     * internal {@code data->>'topic'} so we can publish a per-topic breakdown.
     */
    private void drillDownGlobal(long fromOffset, long toOffset, String parentUrl,
                                  PipeConsistencyReport.Builder report,
                                  java.util.Map<String, Long> perTopicMissing,
                                  java.util.Map<String, Long> perTopicExtra,
                                  java.util.Map<String, Long> perTopicDataMismatch) {
        int pageSize = drillDownBatch;
        int recorded = 0;
        long localCountInChunk = 0;
        long upstreamCountInChunk = 0;

        // Cursor-paged: each successful fetch returns up to `pageSize` records with offsets
        // in (cursor, toOffset]. We track `brokerReadFloor` so broker-side reads cover the
        // SAME offset window cloud just returned — avoids phantom missing/extra from
        // page-boundary skew.
        long brokerReadFloor = fromOffset;
        Long cursor = null;
        while (true) {
            if (recorded >= maxMismatchReport) {
                report.truncated(true);
                break;
            }
            List<UpstreamConsistencyClient.RangeRecord> upstreamPage =
                    upstream.fetchRange(parentUrl, GLOBAL_SCOPE, fromOffset, toOffset, cursor, pageSize);
            if (upstreamPage.isEmpty()) {
                // Cloud has no more records in this chunk. There may still be broker records
                // beyond brokerReadFloor that cloud doesn't have → real "extra on broker".
                java.util.Map<Long, com.messaging.common.model.MessageRecord> tail =
                        readAllTopicsInPage(brokerReadFloor, toOffset);
                localCountInChunk += tail.size();
                for (java.util.Map.Entry<Long, com.messaging.common.model.MessageRecord> e : tail.entrySet()) {
                    if (recorded >= maxMismatchReport) break;
                    com.messaging.common.model.MessageRecord loc = e.getValue();
                    report.addExtraOnBroker(new PipeConsistencyReport.RecordDiff(e.getKey(), loc.getMsgKey(),
                            "extra on broker (topic=" + loc.getTopic() + ")"));
                    perTopicExtra.merge(nullToUnknown(loc.getTopic()), 1L, Long::sum);
                    recorded++;
                }
                break;
            }

            java.util.Map<Long, UpstreamConsistencyClient.RangeRecord> upstreamByOffset = new java.util.HashMap<>();
            for (UpstreamConsistencyClient.RangeRecord r : upstreamPage) upstreamByOffset.put(r.offset, r);

            long upstreamMax = upstreamPage.get(upstreamPage.size() - 1).offset;
            // Read broker records in the SAME window cloud just covered — from where the
            // previous page ended (or chunk start), through this page's last cloud offset.
            java.util.Map<Long, com.messaging.common.model.MessageRecord> localByOffset =
                    readAllTopicsInPage(brokerReadFloor, upstreamMax);

            localCountInChunk += localByOffset.size();
            upstreamCountInChunk += upstreamByOffset.size();

            java.util.Set<Long> offsets = new java.util.TreeSet<>();
            offsets.addAll(upstreamByOffset.keySet());
            offsets.addAll(localByOffset.keySet());

            for (Long off : offsets) {
                if (recorded >= maxMismatchReport) break;
                UpstreamConsistencyClient.RangeRecord up = upstreamByOffset.get(off);
                com.messaging.common.model.MessageRecord loc = localByOffset.get(off);
                if (up != null && loc == null) {
                    String topicOfRecord = extractTopic(up.data);
                    report.addMissingOnBroker(new PipeConsistencyReport.RecordDiff(off, up.msgKey,
                            "missing on broker (topic=" + topicOfRecord + ")"));
                    perTopicMissing.merge(nullToUnknown(topicOfRecord), 1L, Long::sum);
                    recorded++;
                } else if (loc != null && up == null) {
                    report.addExtraOnBroker(new PipeConsistencyReport.RecordDiff(off, loc.getMsgKey(),
                            "extra on broker (topic=" + loc.getTopic() + ")"));
                    perTopicExtra.merge(nullToUnknown(loc.getTopic()), 1L, Long::sum);
                    recorded++;
                } else if (loc != null && up != null) {
                    int localCrc = RecordHasher.recordCrc(loc.getOffset(), loc.getMsgKey(),
                            (char) loc.getEventType().getCode(), loc.getData());
                    int upCrc = RecordHasher.recordCrc(up.offset, up.msgKey, up.eventTypeCode, up.data);
                    if (localCrc != upCrc) {
                        report.addDataMismatch(new PipeConsistencyReport.RecordDiff(off, loc.getMsgKey(),
                                "crc mismatch (topic=" + loc.getTopic() + ")"));
                        perTopicDataMismatch.merge(nullToUnknown(loc.getTopic()), 1L, Long::sum);
                        recorded++;
                    }
                }
            }

            brokerReadFloor = upstreamMax + 1;
            cursor = upstreamMax; // server filters to offset > cursor on the next request
            if (brokerReadFloor > toOffset) break;
        }
        report.addMismatchedSegment(new PipeConsistencyReport.SegmentSummary(
                fromOffset, toOffset, null, null, localCountInChunk, upstreamCountInChunk));
    }

    private static String nullToUnknown(String s) {
        return (s == null || s.isBlank()) ? "unknown" : s;
    }

    /**
     * Read broker records from every topic in {@code [pageFrom, pageTo]}, keyed by offset.
     * Per-topic offsets are sparse but globally unique (the original cloud offset is preserved),
     * so a flat map is correct.
     */
    private java.util.Map<Long, com.messaging.common.model.MessageRecord> readAllTopicsInPage(long pageFrom, long pageTo) {
        java.util.Map<Long, com.messaging.common.model.MessageRecord> out = new java.util.HashMap<>();
        for (String topic : storage.getTopicNames()) {
            // Storage caps each call at 1 MB regardless of maxRecords. Don't use
            // batch.size() < max as a termination signal — advance by highest offset
            // seen and break on empty batch.
            long nextOffset = pageFrom;
            while (nextOffset <= pageTo) {
                int max = (int) Math.min(1000L, pageTo - nextOffset + 1);
                List<com.messaging.common.model.MessageRecord> batch;
                try {
                    batch = storage.read(topic, 0, nextOffset, max);
                } catch (Exception e) {
                    break;
                }
                if (batch == null || batch.isEmpty()) break;
                long highestSeen = nextOffset - 1;
                for (com.messaging.common.model.MessageRecord r : batch) {
                    if (r.getOffset() > pageTo) {
                        if (r.getOffset() > highestSeen) highestSeen = r.getOffset();
                        continue;
                    }
                    if (r.getOffset() < pageFrom) continue;
                    out.put(r.getOffset(), r);
                    if (r.getOffset() > highestSeen) highestSeen = r.getOffset();
                }
                long newOffset = highestSeen + 1;
                if (newOffset <= nextOffset) break;
                nextOffset = newOffset;
            }
        }
        return out;
    }

    /**
     * Crude but cheap topic extractor — assumes the payload's first JSON field is
     * {@code "topic":"NAME"}. Avoids parsing 20 KB of JSON per record.
     */
    private static String extractTopic(String data) {
        if (data == null) return "unknown";
        int i = data.indexOf("\"topic\":");
        if (i < 0) return "unknown";
        int start = data.indexOf('"', i + 8);
        if (start < 0) return "unknown";
        int end = data.indexOf('"', start + 1);
        if (end < 0) return "unknown";
        return data.substring(start + 1, end);
    }

    /**
     * Zero the breakdown gauges for every topic the broker currently knows about,
     * so an audit run that finishes CONSISTENT doesn't leave the dashboard showing
     * yesterday's drift findings. Called at the start of every global audit.
     */
    private void resetAllPerTopicGauges(String mode) {
        for (String topic : storage.getTopicNames()) {
            if (topic == null || topic.isBlank()) continue;
            metrics.setLatestBreakdown(topic, mode, 0L, 0L, 0L, 0L);
        }
    }

    private void publishPerTopicBreakdown(String mode,
                                          java.util.Map<String, Long> missing,
                                          java.util.Map<String, Long> extra,
                                          java.util.Map<String, Long> data) {
        java.util.Set<String> allTopics = new java.util.HashSet<>();
        allTopics.addAll(missing.keySet());
        allTopics.addAll(extra.keySet());
        allTopics.addAll(data.keySet());
        for (String topic : allTopics) {
            // Micrometer Tag.of rejects null tag values; skip records whose topic
            // couldn't be parsed from the payload.
            if (topic == null || topic.isBlank()) continue;
            long miss = missing.getOrDefault(topic, 0L);
            long ext  = extra.getOrDefault(topic, 0L);
            long dat  = data.getOrDefault(topic, 0L);
            metrics.setLatestBreakdown(topic, mode, miss, ext, dat, 0L);
            // Also emit per-topic counters so the over-time panels can render rates.
            metrics.addMissingRecords(topic, mode, miss);
            metrics.addExtraRecords(topic, mode, ext);
            metrics.addDataMismatchRecords(topic, mode, dat);
        }
    }
}
