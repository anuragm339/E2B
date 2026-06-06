package com.messaging.broker.consistency;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.micronaut.context.annotation.Value;
import jakarta.inject.Singleton;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.URI;
import java.net.URLEncoder;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.ArrayList;
import java.util.HexFormat;
import java.util.List;

/**
 * Thin HTTP client for the upstream's {@code /pipe/consistency/*} contract. Honors
 * HTTP 429 with exponential backoff (matching {@code HttpPipeConnector}'s pattern).
 *
 * <p>Stateless — one instance per broker, called with the target {@code parentUrl}
 * each time so HOP and DEEP can hit different upstreams as needed.
 */
@Singleton
public class UpstreamConsistencyClient {
    private static final Logger LOG = LoggerFactory.getLogger(UpstreamConsistencyClient.class);

    private final HttpClient http;
    private final ObjectMapper mapper = new ObjectMapper();
    private final Duration timeout;
    private final PipeConsistencyMetrics metrics;

    public UpstreamConsistencyClient(
            @Value("${pipe.consistency.upstream.timeout:30s}") Duration timeout,
            PipeConsistencyMetrics metrics
    ) {
        this.timeout = timeout != null ? timeout : Duration.ofSeconds(30);
        this.metrics = metrics;
        this.http = HttpClient.newBuilder()
                .connectTimeout(Duration.ofSeconds(5))
                .build();
    }

    public HashResponse fetchHash(String parentUrl, String topic, long from, long to, String projection) {
        String url = parentUrl + "/pipe/consistency/hash?topic=" + encode(topic)
                + "&from=" + from + "&to=" + to + "&projection=" + encode(projection);
        JsonNode node = doGet(url);
        if (node == null) {
            return new HashResponse(false, null, 0L, projection, null, null, 429);
        }
        byte[] hash = HexFormat.of().parseHex(node.path("hash").asText(""));
        long count = node.path("recordCount").asLong();
        String proj = node.path("projection").asText(projection);
        String nodeId = node.path("nodeId").asText("");
        String upstreamParent = node.hasNonNull("parentUrl") ? node.get("parentUrl").asText() : null;
        return new HashResponse(true, hash, count, proj, nodeId, upstreamParent, 200);
    }

    public long fetchMaxOffset(String parentUrl, String topic) {
        String url = parentUrl + "/pipe/consistency/max-offset?topic=" + encode(topic);
        JsonNode node = doGet(url);
        return node == null ? -1L : node.path("maxOffset").asLong(-1L);
    }

    /**
     * Cursor-paged range fetch. Pass {@code cursor=null} for the first request,
     * then pass the last record's offset from the previous response to continue.
     * Returns empty list when no more records remain in {@code (cursor, to]}.
     */
    public List<RangeRecord> fetchRange(String parentUrl, String topic, long from, long to,
                                         Long cursor, int pageSize) {
        StringBuilder url = new StringBuilder(parentUrl)
                .append("/pipe/consistency/range?topic=").append(encode(topic))
                .append("&from=").append(from)
                .append("&to=").append(to)
                .append("&pageSize=").append(pageSize);
        if (cursor != null) url.append("&cursor=").append(cursor);
        JsonNode node = doGet(url.toString());
        List<RangeRecord> out = new ArrayList<>();
        if (node == null) return out;
        JsonNode records = node.path("records");
        if (records.isArray()) {
            for (JsonNode r : records) {
                out.add(new RangeRecord(
                        r.path("offset").asLong(),
                        r.path("msgKey").asText(""),
                        r.path("eventType").asText("M").isEmpty() ? 'M' : r.path("eventType").asText("M").charAt(0),
                        r.hasNonNull("data") ? r.get("data").asText() : null));
            }
        }
        return out;
    }

    private JsonNode doGet(String url) {
        // Exponential backoff matching HttpPipeConnector's pattern (delay *= 3, capped).
        long delayMs = 200;
        long maxDelayMs = 5000;
        int attempts = 0;
        while (attempts < 3) {
            attempts++;
            try {
                HttpRequest req = HttpRequest.newBuilder(URI.create(url))
                        .timeout(timeout)
                        .header("Accept", "application/json")
                        .GET()
                        .build();
                HttpResponse<String> resp = http.send(req, HttpResponse.BodyHandlers.ofString());
                int status = resp.statusCode();
                if (status == 200) {
                    return mapper.readTree(resp.body());
                }
                if (status == 429) {
                    LOG.debug("upstream 429 for {} (attempt {})", url, attempts);
                    metrics.recordThrottled(hostOf(url));
                    Thread.sleep(delayMs);
                    delayMs = Math.min(delayMs * 3, maxDelayMs);
                    continue;
                }
                if (status == 204) {
                    return null;
                }
                LOG.debug("upstream {} for {}", status, url);
                return null;
            } catch (InterruptedException ie) {
                Thread.currentThread().interrupt();
                return null;
            } catch (Exception e) {
                LOG.debug("upstream request failed: {} - {}", url, e.toString());
                return null;
            }
        }
        return null;
    }

    private static String encode(String s) {
        return URLEncoder.encode(s == null ? "" : s, StandardCharsets.UTF_8);
    }

    /** Extract host[:port] from a URL for use as a parent_node_id label. */
    private static String hostOf(String url) {
        try {
            URI u = URI.create(url);
            int port = u.getPort();
            return port > 0 ? u.getHost() + ":" + port : u.getHost();
        } catch (Exception e) {
            return "unknown";
        }
    }

    public static final class HashResponse {
        public final boolean ok;
        public final byte[] hash;
        public final long recordCount;
        public final String projection;
        public final String nodeId;
        public final String parentUrl;     // null when upstream is the chain root (cloud)
        public final int httpStatus;

        public HashResponse(boolean ok, byte[] hash, long recordCount, String projection,
                            String nodeId, String parentUrl, int httpStatus) {
            this.ok = ok;
            this.hash = hash;
            this.recordCount = recordCount;
            this.projection = projection;
            this.nodeId = nodeId;
            this.parentUrl = parentUrl;
            this.httpStatus = httpStatus;
        }
    }

    public static final class RangeRecord {
        public final long offset;
        public final String msgKey;
        public final char eventTypeCode;
        public final String data;

        public RangeRecord(long offset, String msgKey, char eventTypeCode, String data) {
            this.offset = offset;
            this.msgKey = msgKey;
            this.eventTypeCode = eventTypeCode;
            this.data = data;
        }
    }
}
