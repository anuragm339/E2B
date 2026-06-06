package com.messaging.broker.consistency;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.micronaut.http.HttpResponse;
import io.micronaut.http.MediaType;
import io.micronaut.http.annotation.Controller;
import io.micronaut.http.annotation.Get;
import io.micronaut.http.annotation.PathVariable;
import io.micronaut.http.annotation.Post;
import io.micronaut.http.annotation.QueryValue;
import io.micronaut.http.annotation.Produces;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.HexFormat;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * Operator-facing endpoints for PipeConsistency. Mounted at /admin/consistency/pipe.
 *
 * <ul>
 *   <li>GET .../root          — local Merkle root + active snapshot summary</li>
 *   <li>GET .../segments      — sealed segment summaries (offset range, hash, epoch, state)</li>
 *   <li>POST .../run          — trigger an on-demand HOP audit, returns the persisted report</li>
 *   <li>POST .../run-deep     — trigger an on-demand DEEP audit, returns the persisted report</li>
 *   <li>GET .../latest-report?mode=hop|deep — load the latest persisted report</li>
 *   <li>GET /admin/consistency/pipe/lineage — dump pipe_lineage rows</li>
 * </ul>
 */
@Controller("/admin/consistency/pipe")
public class PipeConsistencyAdminController {
    private static final Logger LOG = LoggerFactory.getLogger(PipeConsistencyAdminController.class);

    private final PipeConsistencyChecker checker;
    private final BrokerSegmentHashView hashView;
    private final PipeConsistencyReportStore reportStore;
    private final PipeLineageStore lineageStore;
    private final ObjectMapper mapper = new ObjectMapper();

    public PipeConsistencyAdminController(
            PipeConsistencyChecker checker,
            BrokerSegmentHashView hashView,
            PipeConsistencyReportStore reportStore,
            PipeLineageStore lineageStore
    ) {
        this.checker = checker;
        this.hashView = hashView;
        this.reportStore = reportStore;
        this.lineageStore = lineageStore;
    }

    @Get("/{topic}/{partition}/root")
    public HttpResponse<Map<String, Object>> root(@PathVariable String topic, @PathVariable int partition) {
        long earliest = 0L;
        long max = hashView.maxOffset(topic);
        BrokerSegmentHashView.Computed local = hashView.computeHash(topic, earliest, max);
        Map<String, Object> body = new LinkedHashMap<>();
        body.put("topic", topic);
        body.put("partition", partition);
        body.put("localRoot", HexFormat.of().formatHex(local.hash));
        body.put("recordCount", local.recordCount);
        body.put("maxOffset", max);
        body.put("maxCompactionEpoch", local.maxCompactionEpoch);
        return HttpResponse.ok(body);
    }

    @Get("/{topic}/{partition}/segments")
    public HttpResponse<Map<String, Object>> segments(@PathVariable String topic, @PathVariable int partition) {
        List<BrokerSegmentHashView.SegmentSummary> summaries = hashView.sealedSummaries(topic);
        List<Map<String, Object>> rows = new ArrayList<>(summaries.size());
        for (BrokerSegmentHashView.SegmentSummary s : summaries) {
            Map<String, Object> row = new LinkedHashMap<>();
            row.put("baseOffset", s.baseOffset);
            row.put("maxOffset", s.maxOffset);
            row.put("hash", s.hash == null ? null : HexFormat.of().formatHex(s.hash));
            row.put("recordCount", s.recordCount);
            row.put("compactionEpoch", s.compactionEpoch);
            row.put("hashState", s.hashState);
            rows.add(row);
        }
        Map<String, Object> body = new LinkedHashMap<>();
        body.put("topic", topic);
        body.put("partition", partition);
        body.put("segments", rows);
        return HttpResponse.ok(body);
    }

    @Post("/{topic}/{partition}/run")
    public HttpResponse<Map<String, Object>> run(@PathVariable String topic, @PathVariable int partition) {
        // Per-topic audits are deprecated — the pipe is a single global cloud-offset
        // stream, so per-topic comparisons are not meaningful. Route to the global
        // audit so existing callers still get a useful answer; advertise the new
        // endpoint in the response.
        PipeConsistencyReport report = checker.runHopGlobal();
        Map<String, Object> body = new LinkedHashMap<>(report.toJson());
        body.put("deprecated", true);
        body.put("note", "per-topic audit replaced by global audit; use POST /admin/consistency/pipe/global/run");
        return HttpResponse.ok(body);
    }

    @Post("/{topic}/{partition}/run-deep")
    public HttpResponse<Map<String, Object>> runDeep(@PathVariable String topic, @PathVariable int partition) {
        PipeConsistencyReport report = checker.runDeepGlobal();
        Map<String, Object> body = new LinkedHashMap<>(report.toJson());
        body.put("deprecated", true);
        body.put("note", "per-topic audit replaced by global audit; use POST /admin/consistency/pipe/global/run-deep");
        return HttpResponse.ok(body);
    }

    // ── Global (single-stream) endpoints ──────────────────────────────────────

    @Post("/global/run")
    public HttpResponse<Map<String, Object>> runGlobal() {
        PipeConsistencyReport report = checker.runHopGlobal();
        return HttpResponse.ok(report.toJson());
    }

    @Post("/global/run-deep")
    public HttpResponse<Map<String, Object>> runGlobalDeep() {
        PipeConsistencyReport report = checker.runDeepGlobal();
        return HttpResponse.ok(report.toJson());
    }

    @Get("/global/root")
    public HttpResponse<Map<String, Object>> globalRoot() {
        // Summary only — do NOT compute the hash over the entire broker history.
        // At ~200K records × ~20 KB each that's ~4 GB, which OOMs the broker. The
        // audit doesn't actually need a full-range hash; it computes chunk-sized
        // hashes (bounded by pipe.consistency.chunk-size) and compares them.
        long earliest = hashView.globalEarliestOffset();
        long max = hashView.globalMaxOffset();
        Map<String, Object> body = new LinkedHashMap<>();
        body.put("scope", PipeConsistencyChecker.GLOBAL_SCOPE);
        body.put("earliestOffset", earliest);
        body.put("maxOffset", max);
        body.put("rangeRecords", (earliest >= 0 && max >= earliest) ? (max - earliest + 1) : 0);
        body.put("note", "full-range root hash not computed (would OOM); audit chunks the range and " +
                "compares hashes per chunk. Use POST /global/run to run an audit.");
        return HttpResponse.ok(body);
    }

    @Get(value = "/global/latest-report", produces = MediaType.APPLICATION_JSON)
    @Produces(MediaType.APPLICATION_JSON)
    public HttpResponse<String> globalLatestReport(@QueryValue(defaultValue = "hop") String mode) {
        PipeConsistencyReport.Mode m = "deep".equalsIgnoreCase(mode)
                ? PipeConsistencyReport.Mode.DEEP : PipeConsistencyReport.Mode.HOP;
        String json = reportStore.getLatestJson(PipeConsistencyChecker.GLOBAL_SCOPE, 0, m);
        if (json == null) {
            return HttpResponse.notFound("{\"error\":\"no report yet — trigger via POST /global/run or /global/run-deep\"}");
        }
        return HttpResponse.ok(json);
    }

    @Get(value = "/{topic}/{partition}/latest-report", produces = MediaType.APPLICATION_JSON)
    @Produces(MediaType.APPLICATION_JSON)
    public HttpResponse<String> latestReport(
            @PathVariable String topic,
            @PathVariable int partition,
            @QueryValue(defaultValue = "hop") String mode
    ) {
        PipeConsistencyReport.Mode m = "deep".equalsIgnoreCase(mode)
                ? PipeConsistencyReport.Mode.DEEP : PipeConsistencyReport.Mode.HOP;
        String json = reportStore.getLatestJson(topic, partition, m);
        if (json == null) {
            return HttpResponse.notFound("{\"error\":\"no report\"}");
        }
        return HttpResponse.ok(json);
    }

    @Get("/lineage")
    public HttpResponse<Map<String, Object>> lineage() {
        List<PipeLineageEntry> entries = lineageStore.allEntries();
        List<Map<String, Object>> rows = new ArrayList<>(entries.size());
        for (PipeLineageEntry e : entries) {
            Map<String, Object> row = new LinkedHashMap<>();
            row.put("id", e.getId());
            row.put("offsetStart", e.getOffsetStart());
            row.put("offsetEndExclusive", e.getOffsetEndExclusive());
            row.put("parentUrl", e.getParentUrl());
            row.put("recordedAt", e.getRecordedAt().toString());
            row.put("open", e.isOpen());
            rows.add(row);
        }
        Map<String, Object> body = new LinkedHashMap<>();
        body.put("lineage", rows);
        return HttpResponse.ok(body);
    }
}
