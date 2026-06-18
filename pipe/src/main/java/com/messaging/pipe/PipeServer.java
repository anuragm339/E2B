package com.messaging.pipe;

import com.messaging.common.api.StorageEngine;
import com.messaging.common.model.MessageRecord;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.micronaut.http.HttpResponse;
import io.micronaut.http.annotation.Controller;
import io.micronaut.http.annotation.Get;
import io.micronaut.http.annotation.Post;
import io.micronaut.http.annotation.QueryValue;
import jakarta.inject.Inject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * HTTP server for serving messages to child brokers via Pipe
 */
@Controller("/pipe")
public class PipeServer {
    private static final Logger log = LoggerFactory.getLogger(PipeServer.class);

    // Per-topic record cap for a single multi-topic poll, to bound response size.
    private static final int MULTI_PER_TOPIC_LIMIT = 500;

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
     * Endpoint for child brokers to poll for new messages
     * GET /pipe/poll?offset=0&limit=100&topic=price-topic
     */
    @Get("/poll")
    public HttpResponse<String> pollMessages(
            @QueryValue(defaultValue = "0") long offset,
            @QueryValue(defaultValue = "100") int limit,
            @QueryValue(defaultValue = "price-topic") String topic) {

        try {
            log.debug("Poll request: topic={}, offset={}, limit={}", topic, offset, limit);

            // Read messages from storage
            List<MessageRecord> records = storage.read(topic, 0, offset, limit);

            if (records.isEmpty()) {
                // No new messages
                return HttpResponse.noContent();
            }

            // Serialize to JSON
            String json = objectMapper.writeValueAsString(records);
            log.debug("Serving {} messages to child broker: topic={}, offset={}",
                    records.size(), topic, offset);

            return HttpResponse.ok(json);

        } catch (Exception e) {
            log.error("Error serving messages", e);
            return HttpResponse.serverError("Error serving messages: " + e.getMessage());
        }
    }

    /**
     * Multi-topic bulk poll used by the download-refresh bootstrap (no-snapshot / incremental path):
     * a child broker pulls ALL its topics from a parent POS in one request, each from its own
     * per-topic offset. This is the "subscribe to every topic" pipe poll.
     *
     * <p>Unlike the live {@code /poll}, this is stateless and child-driven — the child supplies the
     * per-topic offsets it has, the parent returns the next records per topic. Records carry their
     * own topic/offset and ingestion is offset-idempotent, so simple per-topic reads (no k-way merge
     * ordering) are correct: each record lands in its own topic.
     *
     * <p>GET /pipe/poll-multi?topics=a,b,c&amp;offsets=0,5,2 — {@code offsets} aligns with
     * {@code topics} by position; a missing/short {@code offsets} defaults that topic to 0.
     */
    @Get("/poll-multi")
    public HttpResponse<String> pollMulti(
            @QueryValue String topics,
            @QueryValue(defaultValue = "") String offsets) {

        if (topics == null || topics.isBlank()) {
            return HttpResponse.badRequest("topics is required (comma-separated)");
        }

        String[] topicArr = topics.split(",");
        String[] offsetArr = offsets.isBlank() ? new String[0] : offsets.split(",");

        try {
            List<MessageRecord> merged = new ArrayList<>();
            for (int i = 0; i < topicArr.length; i++) {
                String topic = topicArr[i].trim();
                if (topic.isEmpty()) {
                    continue;
                }
                long offset = 0L;
                if (i < offsetArr.length) {
                    try {
                        offset = Long.parseLong(offsetArr[i].trim());
                    } catch (NumberFormatException nfe) {
                        return HttpResponse.badRequest("invalid offset for topic " + topic + ": " + offsetArr[i]);
                    }
                }
                List<MessageRecord> records = storage.read(topic, 0, offset, MULTI_PER_TOPIC_LIMIT);
                if (records != null && !records.isEmpty()) {
                    merged.addAll(records);
                }
            }

            if (merged.isEmpty()) {
                return HttpResponse.noContent();
            }
            String json = objectMapper.writeValueAsString(merged);
            log.debug("Serving {} merged messages across {} topics", merged.size(), topicArr.length);
            return HttpResponse.ok(json);

        } catch (Exception e) {
            log.error("Error serving multi-topic messages", e);
            return HttpResponse.serverError("Error serving multi-topic messages: " + e.getMessage());
        }
    }

    /**
     * Per-topic head offsets — the watermark a bootstrapping child targets ("bulk load done for
     * topic X when its offset reaches head") and the denominator for the POS→POS progress %.
     *
     * <p>GET /pipe/head?topics=a,b,c → {@code {"a":41,"b":-1,...}} where the value is the last
     * stored offset ({@code -1} for an empty/unknown topic).
     */
    @Get("/head")
    public HttpResponse<String> head(@QueryValue String topics) {
        if (topics == null || topics.isBlank()) {
            return HttpResponse.badRequest("topics is required (comma-separated)");
        }
        try {
            Map<String, Long> heads = new LinkedHashMap<>();
            for (String t : topics.split(",")) {
                String topic = t.trim();
                if (topic.isEmpty()) {
                    continue;
                }
                heads.put(topic, storage.getCurrentOffset(topic, 0));
            }
            return HttpResponse.ok(objectMapper.writeValueAsString(heads));
        } catch (Exception e) {
            log.error("Error serving topic heads", e);
            return HttpResponse.serverError("Error serving topic heads: " + e.getMessage());
        }
    }

}
