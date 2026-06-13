package com.messaging.broker.consistency;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.messaging.common.exception.ErrorCode;
import com.messaging.common.exception.NetworkException;
import io.micronaut.http.HttpRequest;
import io.micronaut.http.HttpResponse;
import io.micronaut.http.HttpStatus;
import io.micronaut.http.client.HttpClient;
import io.micronaut.http.client.annotation.Client;
import io.micronaut.http.client.exceptions.HttpClientResponseException;
import jakarta.inject.Inject;
import jakarta.inject.Singleton;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * HTTP client for the consistency endpoints served by a parent broker (or the cloud).
 *
 * <p>Follows the {@code CloudRegistryClient} pattern: absolute URLs on a {@code @Client("/")}
 * blocking client, plain-JSON bodies via {@link ObjectMapper}. Outbound auth (if configured)
 * is applied by the global {@code AuthTokenClientFilter}. Calls are made only from the
 * consistency check task on the compaction executor — never from a delivery or ingest path.
 */
@Singleton
public class ParentConsistencyClient {

    /** Parent's reply to a digest request. */
    public static final class DigestResponse {
        public final long parentHead;
        public final long effectiveWatermark;
        public final long[] digests;
        public final int[] counts;

        public DigestResponse(long parentHead, long effectiveWatermark, long[] digests, int[] counts) {
            this.parentHead = parentHead;
            this.effectiveWatermark = effectiveWatermark;
            this.digests = digests;
            this.counts = counts;
        }
    }

    /** One (keyHash, latestOffset) pair from a parent's bucket page. */
    public static final class BucketEntry {
        public final long keyHash;
        public final long offset;

        public BucketEntry(long keyHash, long offset) {
            this.keyHash = keyHash;
            this.offset = offset;
        }
    }


    /** Parent's classification of child-extra keys. */
    public enum KeyState { PRESENT_BEYOND_WATERMARK, PRESENT_AT_OR_BELOW_WATERMARK, ABSENT }

    public static final class ClassifyResponse {
        /** offset -> record physically present on parent */
        public final Map<Long, Boolean> offsets;
        /** msgKey -> index state on parent */
        public final Map<String, KeyState> keys;
        /**
         * True when the answering node's keyspace is complete and never expires (the cloud) —
         * its ABSENT answers then prove a key was fabricated, not merely unseen. POS parents
         * never set this (they may legitimately lack keys that expired before they were
         * provisioned).
         */
        public final boolean authoritative;

        public ClassifyResponse(Map<Long, Boolean> offsets, Map<String, KeyState> keys) {
            this(offsets, keys, false);
        }

        public ClassifyResponse(Map<Long, Boolean> offsets, Map<String, KeyState> keys,
                                boolean authoritative) {
            this.offsets = offsets;
            this.keys = keys;
            this.authoritative = authoritative;
        }
    }

    /** Thrown when the parent answers 404 — older build or feature disabled there. */
    public static final class UnsupportedParentException extends RuntimeException {
        UnsupportedParentException(String url) {
            super("Consistency endpoints not available on " + url);
        }
    }

    private final HttpClient httpClient;
    private final ObjectMapper objectMapper = new ObjectMapper();

    @Inject
    public ParentConsistencyClient(@Client("/") HttpClient httpClient) {
        this.httpClient = httpClient;
    }

    /**
     * Cheap head probe for verifier-candidate filtering — no scan on the target.
     * Throws (network / 404-unsupported) like the other calls; escalation treats any
     * failure as "skip this candidate".
     */
    public long fetchHead(String baseUrl, String topic) {
        JsonNode json = getJson(baseUrl + "/pipe/consistency/head?topic=" + topic);
        return json.path("head").asLong(-1);
    }

    public DigestResponse fetchDigest(String baseUrl, String topic, long watermark, int buckets) {
        String url = baseUrl + "/pipe/consistency/digest?topic=" + topic
                + "&watermark=" + watermark + "&buckets=" + buckets;
        JsonNode json = getJson(url);
        return new DigestResponse(
                json.path("parentHead").asLong(-1),
                json.path("effectiveWatermark").asLong(watermark),
                toLongArray(json.path("digests")),
                toIntArray(json.path("counts")));
    }

    /**
     * Fetch the (keyHash, latestOffset) pairs of several mismatched buckets in ONE request —
     * the parent answers with a single index scan regardless of how many buckets are asked.
     */
    public Map<Integer, List<BucketEntry>> fetchBuckets(String baseUrl, String topic, long watermark,
                                                        int buckets, List<Integer> bucketIds) {
        StringBuilder ids = new StringBuilder();
        for (int id : bucketIds) {
            if (ids.length() > 0) ids.append(',');
            ids.append(id);
        }
        String url = baseUrl + "/pipe/consistency/bucket?topic=" + topic
                + "&watermark=" + watermark + "&buckets=" + buckets + "&bucket=" + ids;
        JsonNode json = getJson(url);
        Map<Integer, List<BucketEntry>> result = new HashMap<>();
        json.path("entries").fields().forEachRemaining(field -> {
            List<BucketEntry> entries = new ArrayList<>();
            for (JsonNode e : field.getValue()) {
                entries.add(new BucketEntry(e.path("h").asLong(), e.path("o").asLong()));
            }
            result.put(Integer.parseInt(field.getKey()), entries);
        });
        return result;
    }

    public ClassifyResponse classify(String baseUrl, String topic, long watermark,
                                     List<Long> offsets, List<String> keys) {
        ObjectNode body = objectMapper.createObjectNode();
        body.put("topic", topic);
        body.put("watermark", watermark);
        ArrayNode offsetsNode = body.putArray("offsets");
        offsets.forEach(offsetsNode::add);
        ArrayNode keysNode = body.putArray("keys");
        keys.forEach(keysNode::add);

        JsonNode json = postJson(baseUrl + "/pipe/consistency/classify", body.toString());

        Map<Long, Boolean> offsetResults = new HashMap<>();
        json.path("offsets").fields().forEachRemaining(e ->
                offsetResults.put(Long.parseLong(e.getKey()), e.getValue().asBoolean()));
        Map<String, KeyState> keyResults = new HashMap<>();
        json.path("keys").fields().forEachRemaining(e ->
                keyResults.put(e.getKey(), KeyState.valueOf(e.getValue().asText())));
        return new ClassifyResponse(offsetResults, keyResults,
                json.path("authoritative").asBoolean(false));
    }

    // ── Helpers ───────────────────────────────────────────────────────────────

    private JsonNode getJson(String url) {
        return exchange(HttpRequest.GET(url), url);
    }

    private JsonNode postJson(String url, String body) {
        return exchange(HttpRequest.POST(url, body).contentType("application/json"), url);
    }

    private JsonNode exchange(HttpRequest<?> request, String url) {
        try {
            HttpResponse<String> response = httpClient.toBlocking().exchange(request, String.class);
            return objectMapper.readTree(response.body());
        } catch (HttpClientResponseException e) {
            if (e.getStatus() == HttpStatus.NOT_FOUND) {
                throw new UnsupportedParentException(url);
            }
            throw new NetworkException(ErrorCode.NETWORK_RECEIVE_FAILED,
                    "Consistency call failed: " + url + " -> " + e.getStatus(), e);
        } catch (UnsupportedParentException e) {
            throw e;
        } catch (Exception e) {
            throw new NetworkException(ErrorCode.NETWORK_CONNECTION_FAILED,
                    "Consistency call failed: " + url, e);
        }
    }

    private static long[] toLongArray(JsonNode array) {
        long[] result = new long[array.size()];
        for (int i = 0; i < array.size(); i++) result[i] = array.get(i).asLong();
        return result;
    }

    private static int[] toIntArray(JsonNode array) {
        int[] result = new int[array.size()];
        for (int i = 0; i < array.size(); i++) result[i] = array.get(i).asInt();
        return result;
    }

}
