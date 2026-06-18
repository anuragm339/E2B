package com.messaging.pipe;

import com.messaging.common.api.StorageEngine;
import com.messaging.common.model.MessageRecord;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.micronaut.http.HttpResponse;
import io.micronaut.http.annotation.Controller;
import io.micronaut.http.annotation.Get;
import io.micronaut.http.annotation.Header;
import io.micronaut.http.annotation.QueryValue;
import jakarta.inject.Inject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Comparator;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.PriorityQueue;

/**
 * HTTP server for serving messages to child brokers via Pipe.
 *
 * <p>{@code GET /pipe/poll} has two modes on one endpoint, selected by the presence of the
 * {@value #CURSORS_HEADER} request header:
 *
 * <ul>
 *   <li><b>Merge mode</b> (header present, used by the download-refresh bootstrap): a Kafka-style
 *       k-way merge across ALL topics in one ~1 MB response, ordered by offset. Because every topic
 *       has its OWN offset space (topic A in the 10 000s, topic B in the 20 000s), a single cursor
 *       cannot track progress without losing/duplicating data — so the child passes a PER-TOPIC
 *       cursor map in the header and the server returns the advanced per-topic cursors in the same
 *       header. Each topic advances independently and exclusively → no duplicates across polls; a
 *       1 MB truncation simply leaves un-drained topics' cursors unchanged. Optional {@code ?topic=}
 *       restricts the merge to one topic.</li>
 *   <li><b>Legacy mode</b> (header absent): the original single-topic poll the existing steady-state
 *       {@code HttpPipeConnector} uses, unchanged for backward compatibility.</li>
 * </ul>
 */
@Controller("/pipe")
public class PipeServer {
    private static final Logger log = LoggerFactory.getLogger(PipeServer.class);

    /** Target total response size for merge mode — mirrors the cloud's ~1 MB poll batch. */
    private static final long TARGET_BATCH_BYTES = 1024 * 1024;
    /** Per-topic storage.read chunk for the merge buffers (bounds working set to CHUNK × topics). */
    private static final int CHUNK = 256;
    private static final int RECORD_OVERHEAD_BYTES = 200;
    /** Default single topic for legacy mode (preserves prior behavior). */
    private static final String LEGACY_DEFAULT_TOPIC = "price-topic";

    /** Request+response header: JSON map of topic -> last offset the child has (exclusive cursor). */
    public static final String CURSORS_HEADER = "X-Pipe-Cursors";
    /** Response header: JSON map of topic -> current head offset (progress-% denominator). */
    public static final String HEADS_HEADER = "X-Pipe-Heads";

    private static final TypeReference<Map<String, Long>> CURSOR_MAP_TYPE = new TypeReference<>() {};

    private final StorageEngine storage;
    private final ObjectMapper objectMapper;

    @Inject
    public PipeServer(StorageEngine storage) {
        this.storage = storage;
        this.objectMapper = new ObjectMapper();
        this.objectMapper.findAndRegisterModules();
        log.info("event=pipe_server.initialized");
    }

    /**
     * Poll for new messages. Merge mode when {@value #CURSORS_HEADER} is present, otherwise the
     * legacy single-topic path.
     *
     * @param cursorsHeader per-topic cursor map; presence selects merge mode. In merge mode a topic
     *                      is served strictly above its cursor; topics absent from the map start at
     *                      {@code offset} (the floor — e.g. 0 for a full bootstrap, or 10000).
     * @param offset        floor offset (merge mode) / start offset (legacy mode).
     * @param limit         legacy-mode record cap.
     * @param topic         optional single-topic filter (merge mode) / topic (legacy mode).
     */
    @Get("/poll")
    public HttpResponse<String> pollMessages(
            @Header(value = CURSORS_HEADER, defaultValue = "") String cursorsHeader,
            @QueryValue(defaultValue = "0") long offset,
            @QueryValue(defaultValue = "100") int limit,
            @QueryValue(defaultValue = "") String topic) {

        if (cursorsHeader != null && !cursorsHeader.isBlank()) {
            return mergePoll(cursorsHeader, offset, topic);
        }
        String legacyTopic = (topic == null || topic.isBlank()) ? LEGACY_DEFAULT_TOPIC : topic.trim();
        return legacyPoll(offset, limit, legacyTopic);
    }

    /** Original single-topic poll — unchanged behavior for the existing steady-state client. */
    private HttpResponse<String> legacyPoll(long offset, int limit, String topic) {
        try {
            List<MessageRecord> records = storage.read(topic, 0, offset, limit);
            if (records == null || records.isEmpty()) {
                return HttpResponse.noContent();
            }
            return HttpResponse.ok(objectMapper.writeValueAsString(records));
        } catch (Exception e) {
            log.error("Error serving messages", e);
            return HttpResponse.serverError("Error serving messages: " + e.getMessage());
        }
    }

    /** Merge mode: k-way merge across topics driven by the per-topic cursor header. */
    private HttpResponse<String> mergePoll(String cursorsHeader, long offset, String topic) {
        try {
            Map<String, Long> cursors = parseCursors(cursorsHeader);
            Collection<String> topics = (topic != null && !topic.isBlank())
                    ? List.of(topic.trim())
                    : storage.getTopicNames();

            Map<String, ArrayDeque<MessageRecord>> buffers = new HashMap<>();
            Map<String, Long> nextRead = new HashMap<>();
            Map<String, Long> heads = new LinkedHashMap<>();
            // Min-heap by the offset of each topic buffer's head record — the k-way merge frontier.
            PriorityQueue<String> heap = new PriorityQueue<>(
                    Comparator.comparingLong(t -> buffers.get(t).peek().getOffset()));

            for (String t : topics) {
                heads.put(t, storage.getCurrentOffset(t, 0));
                // Exclusive above the cursor; topics with no cursor start at the floor (inclusive).
                long start = cursors.containsKey(t) ? cursors.get(t) + 1 : offset;
                ArrayDeque<MessageRecord> buf = new ArrayDeque<>();
                long nr = fill(buf, t, start);
                if (!buf.isEmpty()) {
                    buffers.put(t, buf);
                    nextRead.put(t, nr);
                    heap.add(t);
                }
            }

            List<MessageRecord> out = new ArrayList<>();
            Map<String, Long> advanced = new LinkedHashMap<>();
            long bytes = 0;
            while (!heap.isEmpty() && bytes < TARGET_BATCH_BYTES) {
                String t = heap.poll();
                ArrayDeque<MessageRecord> buf = buffers.get(t);
                MessageRecord r = buf.poll();
                out.add(r);
                bytes += estimateSize(r);
                advanced.put(t, r.getOffset()); // increasing within a topic → last wins = max
                if (buf.isEmpty()) {
                    nextRead.put(t, fill(buf, t, nextRead.get(t)));
                }
                if (!buf.isEmpty()) {
                    heap.add(t);
                }
            }

            String cursorsOut = objectMapper.writeValueAsString(advanced);
            String headsOut = objectMapper.writeValueAsString(heads);
            if (out.isEmpty()) {
                return HttpResponse.<String>noContent()
                        .header(CURSORS_HEADER, cursorsOut)
                        .header(HEADS_HEADER, headsOut);
            }
            log.debug("Serving {} messages via k-way merge ({} topics, {} bytes)", out.size(), topics.size(), bytes);
            return HttpResponse.ok(objectMapper.writeValueAsString(out))
                    .header(CURSORS_HEADER, cursorsOut)
                    .header(HEADS_HEADER, headsOut);

        } catch (Exception e) {
            log.error("Error serving merged messages", e);
            return HttpResponse.serverError("Error serving messages: " + e.getMessage());
        }
    }

    /** Refill {@code buf} from {@code from} (inclusive); returns the next read offset. */
    private long fill(ArrayDeque<MessageRecord> buf, String topic, long from) {
        List<MessageRecord> records = storage.read(topic, 0, from, CHUNK);
        if (records == null || records.isEmpty()) {
            return from;
        }
        long last = from;
        for (MessageRecord r : records) {
            if (r.getOffset() >= from) {
                buf.add(r);
                last = r.getOffset();
            }
        }
        return last + 1;
    }

    private Map<String, Long> parseCursors(String header) {
        if (header == null || header.isBlank()) {
            return Map.of();
        }
        try {
            Map<String, Long> parsed = objectMapper.readValue(header, CURSOR_MAP_TYPE);
            return parsed != null ? parsed : Map.of();
        } catch (Exception e) {
            log.warn("Ignoring malformed {} header: {}", CURSORS_HEADER, e.getMessage());
            return Map.of();
        }
    }

    private static long estimateSize(MessageRecord r) {
        String data = r.getData();
        return (data != null ? data.length() : 0) + RECORD_OVERHEAD_BYTES;
    }
}
