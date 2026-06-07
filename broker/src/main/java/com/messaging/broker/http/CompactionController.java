package com.messaging.broker.http;

import com.messaging.broker.compaction.CompactionScheduler;
import com.messaging.broker.monitoring.BrokerMetrics;
import com.messaging.common.api.StorageEngine;
import com.messaging.storage.segment.SegmentAccess;
import com.messaging.storage.segment.SegmentManager;
import io.micronaut.http.MediaType;
import io.micronaut.http.annotation.Controller;
import io.micronaut.http.annotation.Get;
import io.micronaut.http.annotation.Post;
import io.micronaut.http.annotation.Produces;
import jakarta.inject.Inject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Admin HTTP endpoints for compaction.
 *
 * POST /admin/compaction/trigger        — force-seal all active segments then run a full compaction sweep
 * GET  /admin/compaction/status         — segment counts and sizes per topic
 *
 * The trigger endpoint solves the catch-22 of needing sealed segments to see compaction metrics:
 * with default 1 GB segment size, segments would take hours to fill naturally.  The trigger
 * force-rolls every active segment (sealing it), giving compaction immediate candidates to work on
 * — without any memory pressure from many small segment files.
 */
@Controller("/admin/compaction")
public class CompactionController {
    private static final Logger log = LoggerFactory.getLogger(CompactionController.class);

    private final CompactionScheduler scheduler;
    private final StorageEngine storage;
    private final SegmentAccess segmentAccess;

    @Inject
    public CompactionController(CompactionScheduler scheduler,
                                StorageEngine storage,
                                SegmentAccess segmentAccess) {
        this.scheduler     = scheduler;
        this.storage       = storage;
        this.segmentAccess = segmentAccess;
    }

    /**
     * Force-seal every active segment across all topics, then run a full compaction sweep.
     * This is the recommended way to trigger compaction during development without needing
     * to wait for 1 GB of data to accumulate per topic.
     *
     * <pre>
     * curl -X POST http://localhost:8081/admin/compaction/trigger
     * </pre>
     */
    @Post("/trigger")
    @Produces(MediaType.APPLICATION_JSON)
    public Map<String, Object> trigger() {
        log.info("Manual compaction trigger: force-sealing active segments then running compaction");

        List<String> sealed  = new ArrayList<>();
        List<String> errors  = new ArrayList<>();

        boolean triggered = scheduler.triggerAsync(() -> {
            for (String topic : storage.getTopicNames()) {
                SegmentManager sm = segmentAccess.getSegmentManager(topic, 0);
                if (sm == null) {
                    continue;
                }
                try {
                    sm.forceRollActiveSegment();
                    sealed.add(topic);
                } catch (Exception e) {
                    log.error("Failed to force-roll segment for topic={}", topic, e);
                    errors.add(topic + ": " + e.getMessage());
                }
            }
            log.info("Force-sealed {} topic(s), {} error(s). Starting compaction sweep.",
                    sealed.size(), errors.size());
        });

        Map<String, Object> response = new HashMap<>();
        response.put("triggered", triggered);
        response.put("sealedTopics", sealed);
        response.put("sealErrors", errors);
        response.put("message", triggered
                ? "Active segments force-sealed and compaction sweep started. "
                    + "Check broker logs or Grafana compaction dashboard for results."
                : "Compaction is already running; active segments were not force-sealed.");
        return response;
    }

    /**
     * Report segment state per topic: how many sealed (compactable) vs active segments.
     *
     * <pre>
     * curl http://localhost:8081/admin/compaction/status
     * </pre>
     */
    @Get("/status")
    @Produces(MediaType.APPLICATION_JSON)
    public Map<String, Object> status() {
        Map<String, Object> topics = new HashMap<>();

        for (String topic : storage.getTopicNames()) {
            SegmentManager sm = segmentAccess.getSegmentManager(topic, 0);
            if (sm == null) continue;

            int sealedCount = sm.getInactiveSegments().size();
            long sealedBytes = sm.getInactiveSegments().stream()
                    .mapToLong(s -> s.getSize())
                    .sum();

            Map<String, Object> info = new HashMap<>();
            info.put("sealedSegments", sealedCount);
            info.put("sealedBytes", sealedBytes);
            info.put("readyForCompaction", sealedCount > 0);
            topics.put(topic, info);
        }

        long totalSealed = topics.values().stream()
                .mapToLong(v -> (int) ((Map<?, ?>) v).get("sealedSegments"))
                .sum();

        Map<String, Object> response = new HashMap<>();
        response.put("topics", topics);
        response.put("totalSealedSegments", totalSealed);
        response.put("tip", totalSealed == 0
                ? "No sealed segments yet. POST /admin/compaction/trigger to force-seal and compact."
                : totalSealed + " sealed segment(s) available. POST /admin/compaction/trigger to compact now.");
        return response;
    }
}
