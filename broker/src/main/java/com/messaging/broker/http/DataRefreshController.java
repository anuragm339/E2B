package com.messaging.broker.http;

import com.messaging.broker.refresh.DataRefreshManager;
import com.messaging.broker.refresh.DataRefreshContext;
import com.messaging.broker.refresh.RefreshResult;
import io.micronaut.http.annotation.*;
import io.micronaut.http.MediaType;
import jakarta.inject.Inject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;

/**
 * HTTP endpoints for DataRefresh operations
 *
 * Provides admin API to trigger and monitor data refresh workflow
 */
@Controller("/admin")
public class DataRefreshController {
    private static final Logger log = LoggerFactory.getLogger(DataRefreshController.class);

    private final DataRefreshManager refreshManager;

    @Inject
    public DataRefreshController(DataRefreshManager refreshManager) {
        this.refreshManager = refreshManager;
    }

    /**
     * Trigger data refresh for a topic
     * POST /admin/refresh-topic
     *
     * Request body: {"topic": "prices-v1"}
     *
     * Response:
     * Success: {"success": true, "topic": "prices-v1", "state": "RESET_SENT", "consumersToAck": 3}
     * Error: {"success": false, "error": "Refresh already in progress"}
     */
    @Post("/refresh-topic")
    @Consumes(MediaType.APPLICATION_JSON)
    @Produces(MediaType.APPLICATION_JSON)
    public Map<String, Object> refreshTopic(@Body Map<String, Object> request) {
        // Support both formats:
        // 1. {"topic": "prices-v1,reference-data-v5"} - comma-separated string
        // 2. {"topics": ["prices-v1", "reference-data-v5"]} - JSON array

        java.util.List<String> topicsToRefresh = new ArrayList<>();

        if (request.containsKey("topics")) {
            // Handle JSON array format
            Object topicsObj = request.get("topics");
            if (topicsObj instanceof java.util.List) {
                topicsToRefresh = (java.util.List<String>) topicsObj;
            } else {
                return Map.of(
                    "success", false,
                    "error", "topics must be an array of strings"
                );
            }
        } else if (request.containsKey("topic")) {
            // Handle comma-separated string format
            String topicStr = (String) request.get("topic");
            if (topicStr == null || topicStr.isEmpty()) {
                return Map.of(
                    "success", false,
                    "error", "topic cannot be empty"
                );
            }
            String[] split = topicStr.split(",");
            for (String t : split) {
                topicsToRefresh.add(t.trim());
            }
        } else {
            return Map.of(
                "success", false,
                "error", "Either 'topic' (comma-separated string) or 'topics' (array) is required"
            );
        }

        if (topicsToRefresh.isEmpty()) {
            return Map.of(
                "success", false,
                "error", "No topics provided"
            );
        }

        log.info("Received refresh request for {} topics: {}", topicsToRefresh.size(), topicsToRefresh);

        try {
            // Trigger refresh asynchronously for each topic (don't wait for completion)
            for (String topicToRefresh : topicsToRefresh) {
                String topic = topicToRefresh.trim();
                if (topic.isEmpty()) {
                    log.warn("Skipping empty topic name");
                    continue;
                }

                refreshManager.startRefresh(topic).whenComplete((result, ex) -> {
                    if (ex != null) {
                        log.error("Error during refresh for topic: {}", topic, ex);
                    } else if (result.isSuccess()) {
                        log.info("Refresh workflow started for topic: {} with {} expected consumers",
                                topic, result.getConsumerCount());
                    } else {
                        log.warn("Failed to start refresh for topic {}: {}", topic, result.getError());
                    }
                });
            }

            // Return immediately with acknowledgment
            Map<String, Object> response = new HashMap<>();
            response.put("success", true);
            response.put("message", "Refresh triggered for " + topicsToRefresh.size() + " topic(s)");
            response.put("topics", topicsToRefresh);
            response.put("status", "INITIATED");

            log.info("Refresh request accepted for topics: {}", topicsToRefresh);
            return response;

        } catch (Exception e) {
            log.error("Failed to trigger refresh for topics: {}", topicsToRefresh, e);
            return Map.of(
                "success", false,
                "error", e.getMessage()
            );
        }
    }

    /**
     * Get refresh status for a topic
     * GET /admin/refresh-status?topic=prices-v1
     *
     * Response when refresh is active:
     * {
     *   "topic": "prices-v1",
     *   "status": "ACTIVE",
     *   "state": "REPLAYING",
     *   "expectedConsumers": 3,
     *   "receivedResetAcks": 3,
     *   "receivedReadyAcks": 1,
     *   "startTime": "2024-01-15T10:30:00Z"
     * }
     *
     * Response when no refresh:
     * {
     *   "topic": "prices-v1",
     *   "status": "NONE",
     *   "message": "No active refresh for this topic"
     * }
     */
    @Get("/refresh-status")
    @Produces(MediaType.APPLICATION_JSON)
    public Map<String, Object> getRefreshStatus(@QueryValue String topic) {
        if (topic == null || topic.isEmpty()) {
            return Map.of(
                "success", false,
                "error", "topic parameter is required"
            );
        }

        DataRefreshContext context = refreshManager.getRefreshStatus(topic);

        if (context == null) {
            return Map.of(
                "topic", topic,
                "status", "NONE",
                "message", "No active refresh for this topic"
            );
        }

        Map<String, Object> response = new HashMap<>();
        response.put("topic", topic);
        response.put("status", "ACTIVE");
        response.put("state", context.getState().toString());
        response.put("expectedConsumers", context.getExpectedConsumers().size());
        response.put("receivedResetAcks", context.getReceivedResetAcks().size());
        response.put("receivedReadyAcks", context.getReceivedReadyAcks().size());
        response.put("startTime", context.getStartTime().toString());

        if (context.getResetSentTime() != null) {
            response.put("resetSentTime", context.getResetSentTime().toString());
        }

        if (context.getReadySentTime() != null) {
            response.put("readySentTime", context.getReadySentTime().toString());
        }

        // Add consumer details
        Map<String, Object> consumerDetails = new HashMap<>();
        for (String consumerId : context.getExpectedConsumers()) {
            Map<String, Object> consumerInfo = new HashMap<>();
            consumerInfo.put("resetAckReceived", context.getReceivedResetAcks().contains(consumerId));
            consumerInfo.put("readyAckReceived", context.getReceivedReadyAcks().contains(consumerId));
            consumerInfo.put("isReplaying", context.isConsumerReplaying(consumerId));

            Long offset = context.getConsumerOffsets().get(consumerId);
            if (offset != null) {
                consumerInfo.put("currentOffset", offset);
            }

            consumerDetails.put(consumerId, consumerInfo);
        }
        response.put("consumerDetails", consumerDetails);

        return response;
    }

    /**
     * Get current refresh state (any topic)
     * GET /admin/refresh-current
     *
     * Response when refresh is active:
     * {
     *   "refreshInProgress": true,
     *   "topic": "prices-v1",
     *   "state": "REPLAYING",
     *   ...
     * }
     *
     * Response when no refresh:
     * {
     *   "refreshInProgress": false,
     *   "message": "No refresh in progress"
     * }
     */
    @Get("/refresh-current")
    @Produces(MediaType.APPLICATION_JSON)
    public Map<String, Object> getCurrentRefresh() {
        boolean inProgress = refreshManager.isRefreshInProgress();

        if (!inProgress) {
            return Map.of(
                "refreshInProgress", false,
                "message", "No refresh in progress"
            );
        }

        String topic = refreshManager.getCurrentRefreshTopic();
        DataRefreshContext context = refreshManager.getCurrentRefreshContext();

        if (context == null) {
            return Map.of(
                "refreshInProgress", false,
                "message", "No refresh in progress"
            );
        }

        Map<String, Object> response = new HashMap<>();
        response.put("refreshInProgress", true);
        response.put("topic", topic);
        response.put("state", context.getState().toString());
        response.put("expectedConsumers", context.getExpectedConsumers().size());
        response.put("receivedResetAcks", context.getReceivedResetAcks().size());
        response.put("receivedReadyAcks", context.getReceivedReadyAcks().size());
        response.put("startTime", context.getStartTime().toString());

        return response;
    }
}
