package com.messaging.broker.http;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.fasterxml.jackson.databind.JsonNode;
import com.messaging.broker.compaction.CompactionIndex;
import com.messaging.broker.consistency.KeyspaceDigest;
import com.messaging.common.api.StorageEngine;
import com.messaging.common.model.MessageRecord;
import io.micronaut.context.annotation.Value;
import io.micronaut.http.HttpResponse;
import io.micronaut.http.HttpStatus;
import io.micronaut.http.MediaType;
import io.micronaut.http.annotation.Body;
import io.micronaut.http.annotation.Controller;
import io.micronaut.http.annotation.Get;
import io.micronaut.http.annotation.Post;
import io.micronaut.http.annotation.Produces;
import io.micronaut.http.annotation.QueryValue;
import jakarta.inject.Inject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.Semaphore;

/**
 * Consistency endpoints this broker serves to its CHILDREN (and that the cloud must mirror).
 *
 * <p>All three endpoints are read-only and bounded:
 * <ul>
 *   <li>{@code GET /pipe/consistency/digest} — one streaming index scan, ~1 KB reply</li>
 *   <li>{@code GET /pipe/consistency/bucket} — one streaming index scan serving the requested
 *       (mismatched) buckets, entry cap enforced</li>
 *   <li>{@code POST /pipe/consistency/classify} — point lookups for the few suspicious entries</li>
 * </ul>
 *
 * <p>Fail-closed: with {@code pipe.consistency.enabled=false} (default) every endpoint returns
 * 404 — indistinguishable from an older build, which children already classify as
 * {@code UNSUPPORTED_PARENT}. A semaphore caps concurrent scans so many children checking at
 * once cannot degrade this node (excess requests get 429 and the child retries another time).
 */
@Controller("/pipe/consistency")
public class PipeConsistencyController {

    private static final Logger log = LoggerFactory.getLogger(PipeConsistencyController.class);
    private static final int MAX_BUCKETS = 4096;
    private static final int MAX_CLASSIFY_BATCH = 5000;

    private final StorageEngine storage;
    private final CompactionIndex compactionIndex;
    private final ObjectMapper objectMapper = new ObjectMapper();
    private final boolean enabled;
    private final int scanYieldEvery;
    private final int maxBucketEntries;
    private final Semaphore scanPermits;

    @Inject
    public PipeConsistencyController(
            StorageEngine storage,
            CompactionIndex compactionIndex,
            @Value("${pipe.consistency.enabled:false}") boolean enabled,
            @Value("${pipe.consistency.scan-yield-every:10000}") int scanYieldEvery,
            @Value("${pipe.consistency.max-bucket-entries:100000}") int maxBucketEntries,
            @Value("${pipe.consistency.max-concurrent-scans:2}") int maxConcurrentScans) {
        this.storage = storage;
        this.compactionIndex = compactionIndex;
        this.enabled = enabled;
        this.scanYieldEvery = scanYieldEvery;
        this.maxBucketEntries = Math.max(1000, maxBucketEntries);
        this.scanPermits = new Semaphore(Math.max(1, maxConcurrentScans));
    }

    /**
     * Cheap head probe — lets a child filter verifier candidates (head >= its watermark)
     * before asking anyone to run a digest scan. O(1): in-memory storage head, no scan,
     * no semaphore.
     */
    @Get("/head")
    @Produces(MediaType.APPLICATION_JSON)
    public HttpResponse<String> head(@QueryValue String topic) {
        if (!enabled) {
            return HttpResponse.notFound();
        }
        try {
            return HttpResponse.ok("{\"head\":" + storage.getCurrentOffset(topic, 0) + "}");
        } catch (Exception e) {
            log.error("event=pipe_consistency.head_failed topic={}", topic, e);
            return HttpResponse.serverError("{\"error\":\"" + e.getMessage() + "\"}");
        }
    }

    @Get("/digest")
    @Produces(MediaType.APPLICATION_JSON)
    public HttpResponse<String> digest(
            @QueryValue String topic,
            @QueryValue long watermark,
            @QueryValue(defaultValue = "64") int buckets) {
        if (!enabled) {
            return HttpResponse.notFound();
        }
        if (buckets < 1 || buckets > MAX_BUCKETS || watermark < 0) {
            return HttpResponse.badRequest();
        }
        if (!scanPermits.tryAcquire()) {
            return HttpResponse.status(HttpStatus.TOO_MANY_REQUESTS);
        }
        try {
            long head = storage.getCurrentOffset(topic, 0);
            long effectiveWatermark = Math.min(watermark, head);

            KeyspaceDigest.Result result = effectiveWatermark < 0
                    ? new KeyspaceDigest.Result(new long[buckets], new int[buckets], 0)
                    : KeyspaceDigest.compute(compactionIndex, topic, effectiveWatermark, buckets, scanYieldEvery);

            ObjectNode json = objectMapper.createObjectNode();
            json.put("parentHead", head);
            json.put("effectiveWatermark", effectiveWatermark);
            ArrayNode digests = json.putArray("digests");
            for (long d : result.digests) digests.add(d);
            ArrayNode counts = json.putArray("counts");
            for (int c : result.counts) counts.add(c);
            return HttpResponse.ok(json.toString());
        } catch (Exception e) {
            log.error("event=pipe_consistency.digest_failed topic={}", topic, e);
            return HttpResponse.serverError("{\"error\":\"" + e.getMessage() + "\"}");
        } finally {
            scanPermits.release();
        }
    }

    /**
     * Serve the (keyHash, latestOffset) pairs of the requested buckets (comma-separated ids)
     * with one index scan, entries filtered to {@code latestOffset <= watermark}.
     */
    @Get("/bucket")
    @Produces(MediaType.APPLICATION_JSON)
    public HttpResponse<String> bucket(
            @QueryValue String topic,
            @QueryValue long watermark,
            @QueryValue(defaultValue = "64") int buckets,
            @QueryValue String bucket) {
        if (!enabled) {
            return HttpResponse.notFound();
        }
        if (buckets < 1 || buckets > MAX_BUCKETS || watermark < 0) {
            return HttpResponse.badRequest();
        }
        Set<Integer> wanted = new HashSet<>();
        for (String id : bucket.split(",")) {
            try {
                int b = Integer.parseInt(id.trim());
                if (b < 0 || b >= buckets) return HttpResponse.badRequest();
                wanted.add(b);
            } catch (NumberFormatException e) {
                return HttpResponse.badRequest();
            }
        }
        if (!scanPermits.tryAcquire()) {
            return HttpResponse.status(HttpStatus.TOO_MANY_REQUESTS);
        }
        try {
            ObjectNode json = objectMapper.createObjectNode();
            ObjectNode entriesNode = json.putObject("entries");
            for (int b : wanted) entriesNode.putArray(String.valueOf(b));

            long[] state = new long[2]; // [scanned, served]
            int bucketCount = buckets;
            compactionIndex.forEachEntry(topic, (msgKey, latestOffset, ts) -> {
                state[0]++;
                if (scanYieldEvery > 0 && state[0] % scanYieldEvery == 0) Thread.yield();
                if (latestOffset > watermark) return;
                long hash = KeyspaceDigest.hash64(msgKey);
                int b = KeyspaceDigest.bucketOf(hash, bucketCount);
                if (!wanted.contains(b)) return;
                if (++state[1] > maxBucketEntries) {
                    throw new BucketTooLargeException();
                }
                ObjectNode entry = ((ArrayNode) entriesNode.get(String.valueOf(b))).addObject();
                entry.put("h", hash);
                entry.put("o", latestOffset);
            });
            return HttpResponse.ok(json.toString());
        } catch (BucketTooLargeException e) {
            log.warn("event=pipe_consistency.bucket_too_large topic={} cap={}", topic, maxBucketEntries);
            return HttpResponse.status(HttpStatus.REQUEST_ENTITY_TOO_LARGE)
                    .body("{\"error\":\"bucket entries exceed cap " + maxBucketEntries + "\"}");
        } catch (Exception e) {
            log.error("event=pipe_consistency.bucket_failed topic={}", topic, e);
            return HttpResponse.serverError("{\"error\":\"" + e.getMessage() + "\"}");
        } finally {
            scanPermits.release();
        }
    }

    /**
     * Classify suspicious entries for a child:
     * offsets → is the record at this exact offset still physically readable here;
     * keys → this node's index state for the key relative to the watermark.
     */
    @Post("/classify")
    @Produces(MediaType.APPLICATION_JSON)
    public HttpResponse<String> classify(@Body String body) {
        if (!enabled) {
            return HttpResponse.notFound();
        }
        try {
            JsonNode request = objectMapper.readTree(body);
            String topic = request.path("topic").asText();
            long watermark = request.path("watermark").asLong(-1);
            JsonNode offsets = request.path("offsets");
            JsonNode keys = request.path("keys");
            if (topic.isEmpty() || watermark < 0
                    || offsets.size() + keys.size() > MAX_CLASSIFY_BATCH) {
                return HttpResponse.badRequest();
            }

            ObjectNode json = objectMapper.createObjectNode();
            ObjectNode offsetResults = json.putObject("offsets");
            for (JsonNode offsetNode : offsets) {
                long offset = offsetNode.asLong();
                offsetResults.put(String.valueOf(offset), recordPhysicallyPresent(topic, offset));
            }
            ObjectNode keyResults = json.putObject("keys");
            for (JsonNode keyNode : keys) {
                String msgKey = keyNode.asText();
                long[] latest = compactionIndex.getLatestOffsetAndTimestamp(topic, msgKey);
                String result;
                if (latest == null) {
                    result = "ABSENT";
                } else if (latest[0] > watermark) {
                    result = "PRESENT_BEYOND_WATERMARK";
                } else {
                    result = "PRESENT_AT_OR_BELOW_WATERMARK";
                }
                keyResults.put(msgKey, result);
            }
            return HttpResponse.ok(json.toString());
        } catch (Exception e) {
            log.error("event=pipe_consistency.classify_failed", e);
            return HttpResponse.serverError("{\"error\":\"" + e.getMessage() + "\"}");
        }
    }

    /**
     * True when the record at exactly {@code offset} is still readable from segments.
     * storage.read returns the first record AT OR AFTER the requested offset (gap handling),
     * so the offset must be compared — a gap means the record was compacted away.
     */
    private boolean recordPhysicallyPresent(String topic, long offset) {
        try {
            List<MessageRecord> records = storage.read(topic, 0, offset, 1);
            return !records.isEmpty() && records.get(0).getOffset() == offset;
        } catch (Exception e) {
            log.warn("event=pipe_consistency.classify_read_failed topic={} offset={}", topic, offset, e);
            return false;
        }
    }

    private static final class BucketTooLargeException extends RuntimeException {}
}
