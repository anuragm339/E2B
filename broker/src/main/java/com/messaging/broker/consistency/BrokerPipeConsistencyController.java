package com.messaging.broker.consistency;

import com.messaging.broker.core.TopologyManager;
import com.messaging.common.api.StorageEngine;
import com.messaging.common.model.MessageRecord;
import io.micronaut.context.annotation.Property;
import io.micronaut.context.annotation.Value;
import io.micronaut.http.HttpResponse;
import io.micronaut.http.annotation.Controller;
import io.micronaut.http.annotation.Get;
import io.micronaut.http.annotation.QueryValue;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

/**
 * Broker side of the PipeConsistency upstream contract. Mounted at /pipe/consistency
 * (sibling of /pipe/poll on PipeServer). Mirrors the cloud-server's ConsistencyController
 * so a child broker can call its parent uniformly regardless of whether the parent is
 * cloud or another broker.
 */
@Controller("/pipe/consistency")
public class BrokerPipeConsistencyController {
    private static final Logger LOG = LoggerFactory.getLogger(BrokerPipeConsistencyController.class);

    private final BrokerSegmentHashView hashView;
    private final HashCache hashCache;
    private final StorageEngine storage;
    private final TopologyManager topology;
    private final boolean enabled;
    private final String nodeId;

    public BrokerPipeConsistencyController(
            BrokerSegmentHashView hashView,
            HashCache hashCache,
            StorageEngine storage,
            TopologyManager topology,
            @Property(name = "pipe.consistency.endpoint.enabled", defaultValue = "true") boolean enabled,
            @Value("${broker.nodeId:local-001}") String nodeId
    ) {
        this.hashView = hashView;
        this.hashCache = hashCache;
        this.storage = storage;
        this.topology = topology;
        this.enabled = enabled;
        this.nodeId = nodeId;
    }

    @Get("/hash")
    public HttpResponse<Map<String, Object>> hash(
            @QueryValue String topic,
            @QueryValue long from,
            @QueryValue long to,
            @QueryValue(defaultValue = "raw") String projection
    ) {
        if (!enabled) {
            return HttpResponse.serverError(Map.of("error", "consistency endpoint disabled"));
        }
        // Brokers hold one view of their own data — projection flag is echoed back so the caller
        // can confirm the parent's epoch matches what it expects, but the broker doesn't apply
        // additional dedup beyond what compaction already did.
        String normalised = "compacted".equalsIgnoreCase(projection) ? "compacted" : "raw";
        HashCache.Key key = new HashCache.Key(topic, from, to, normalised);
        boolean[] cachedFlag = {true};
        HashCache.CachedHash result = hashCache.getOrCompute(key, () -> {
            cachedFlag[0] = false;
            BrokerSegmentHashView.Computed c = hashView.computeHash(topic, from, to);
            return new HashCache.CachedHash(c.hash, c.recordCount, normalised);
        });

        Map<String, Object> body = new LinkedHashMap<>();
        body.put("hash", toHex(result.getHash()));
        body.put("recordCount", result.recordCount);
        body.put("projection", result.projection);
        body.put("cached", cachedFlag[0]);
        body.put("source", "rolling");
        body.put("nodeId", nodeId);
        body.put("parentUrl", topology.getCurrentParentUrl());
        body.put("from", from);
        body.put("to", to);
        body.put("topic", topic);
        return HttpResponse.ok(body);
    }

    /**
     * Cursor-paged range read. The caller passes {@code cursor=lastOffsetSeen} on the
     * previous page (or omits it on the first request); the server returns up to
     * {@code pageSize} records with offsets in {@code (cursor, to]} (or {@code [from, to]}
     * on the first call). Empty response = no more records.
     *
     * <p>Because the underlying {@code SegmentManager.read} caps each call at ~1 MB
     * regardless of {@code maxRecords}, we accumulate batches until either {@code pageSize}
     * records are collected or storage stops returning data. Without this loop the page
     * would silently under-fill for topics with large records.
     */
    @Get("/range")
    public HttpResponse<Map<String, Object>> range(
            @QueryValue String topic,
            @QueryValue long from,
            @QueryValue long to,
            @QueryValue Optional<Long> cursor,
            @QueryValue(defaultValue = "200") int pageSize
    ) {
        if (!enabled) {
            return HttpResponse.serverError(Map.of("error", "consistency endpoint disabled"));
        }
        if (pageSize <= 0) pageSize = 200;

        List<Map<String, Object>> out = new ArrayList<>();
        long readFrom = cursor.isPresent() ? cursor.get() + 1 : from;
        Long lastOffsetReturned = null;

        while (out.size() < pageSize && readFrom <= to) {
            int budget = pageSize - out.size();
            List<MessageRecord> batch;
            try {
                batch = storage.read(topic, 0, readFrom, budget);
            } catch (Exception e) {
                LOG.warn("range read failed for {}: {}", topic, e.getMessage());
                break;
            }
            if (batch == null || batch.isEmpty()) break;

            long highestSeen = readFrom - 1;
            for (MessageRecord r : batch) {
                if (r.getOffset() > to) {
                    if (r.getOffset() > highestSeen) highestSeen = r.getOffset();
                    continue;
                }
                if (r.getOffset() < readFrom) continue;
                if (out.size() >= pageSize) break;
                Map<String, Object> m = new LinkedHashMap<>();
                m.put("offset", r.getOffset());
                m.put("msgKey", r.getMsgKey());
                m.put("eventType", String.valueOf((char) r.getEventType().getCode()));
                m.put("data", r.getData());
                out.add(m);
                lastOffsetReturned = r.getOffset();
                if (r.getOffset() > highestSeen) highestSeen = r.getOffset();
            }
            long newFrom = highestSeen + 1;
            if (newFrom <= readFrom) break; // no-progress backstop
            readFrom = newFrom;
        }

        Map<String, Object> body = new LinkedHashMap<>();
        body.put("topic", topic);
        body.put("from", from);
        body.put("to", to);
        body.put("cursor", cursor.orElse(null));
        body.put("pageSize", pageSize);
        body.put("records", out);
        body.put("nextCursor", lastOffsetReturned); // pass back into next request, or null = done
        body.put("nodeId", nodeId);
        return HttpResponse.ok(body);
    }

    @Get("/max-offset")
    public HttpResponse<Map<String, Object>> maxOffset(@QueryValue String topic) {
        if (!enabled) {
            return HttpResponse.serverError(Map.of("error", "consistency endpoint disabled"));
        }
        Map<String, Object> body = new LinkedHashMap<>();
        body.put("topic", topic);
        body.put("maxOffset", hashView.maxOffset(topic));
        body.put("nodeId", nodeId);
        body.put("parentUrl", topology.getCurrentParentUrl());
        return HttpResponse.ok(body);
    }

    private static String toHex(byte[] bytes) {
        StringBuilder sb = new StringBuilder(bytes.length * 2);
        for (byte b : bytes) sb.append(String.format("%02x", b));
        return sb.toString();
    }
}
