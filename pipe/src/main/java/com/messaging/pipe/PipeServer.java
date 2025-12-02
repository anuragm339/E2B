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

import java.util.List;

/**
 * HTTP server for serving messages to child brokers via Pipe
 */
@Controller("/pipe")
public class PipeServer {
    private static final Logger log = LoggerFactory.getLogger(PipeServer.class);

    private final StorageEngine storage;
    private final ObjectMapper objectMapper;

    @Inject
    public PipeServer(StorageEngine storage) {
        this.storage = storage;
        this.objectMapper = new ObjectMapper();
        this.objectMapper.findAndRegisterModules();
        log.info("PipeServer initialized");
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
            log.info("Serving {} messages to child broker: topic={}, offset={}",
                    records.size(), topic, offset);

            return HttpResponse.ok(json);

        } catch (Exception e) {
            log.error("Error serving messages", e);
            return HttpResponse.serverError("Error serving messages: " + e.getMessage());
        }
    }

}
