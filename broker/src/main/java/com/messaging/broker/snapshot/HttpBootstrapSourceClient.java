package com.messaging.broker.snapshot;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.messaging.common.api.StorageEngine;
import com.messaging.common.exception.DataRefreshException;
import com.messaging.common.exception.ErrorCode;
import com.messaging.common.model.MessageRecord;
import io.micronaut.context.annotation.Value;
import jakarta.inject.Singleton;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.time.Duration;
import java.util.List;

/**
 * HTTP implementation of {@link BootstrapSourceClient}: health probe, snapshot info/download, the
 * k-way-merge pull from a parent POS, and the global pull from the cloud. Records are ingested into
 * local storage with offset-idempotent dedup (mirrors the steady-state pipe ingest), so a retry
 * never double-stores.
 *
 * <p>Cloud note: a production cloud is finite and the pull terminates on 204; the synthetic test
 * cloud loops, so the pull is bounded by {@code maxBatches} as a safety stop.
 */
@Singleton
public class HttpBootstrapSourceClient implements BootstrapSourceClient {
    private static final Logger log = LoggerFactory.getLogger(HttpBootstrapSourceClient.class);

    static final String CURSORS_HEADER = "X-Pipe-Cursors";

    private final StorageEngine storage;
    private final String cloudUrl;
    private final int maxBatches;
    private final ObjectMapper objectMapper = new ObjectMapper().findAndRegisterModules();
    private final HttpClient http = HttpClient.newBuilder()
            .connectTimeout(Duration.ofSeconds(5))
            .build();

    public HttpBootstrapSourceClient(
            StorageEngine storage,
            @Value("${broker.registry.url:http://localhost:8080}") String cloudUrl,
            @Value("${broker.bootstrap.max-batches:100000}") int maxBatches) {
        this.storage = storage;
        this.cloudUrl = cloudUrl;
        this.maxBatches = maxBatches;
    }

    @Override
    public boolean isParentHealthy(String parentUrl) {
        try {
            HttpResponse<Void> resp = http.send(
                    get(parentUrl + "/health"), HttpResponse.BodyHandlers.discarding());
            return resp.statusCode() / 100 == 2;
        } catch (Exception e) {
            log.warn("event=bootstrap.parent_health_probe_failed parentUrl={} err={}", parentUrl, e.toString());
            return false;
        }
    }

    @Override
    public boolean snapshotAvailable(String parentUrl) {
        try {
            HttpResponse<String> resp = http.send(
                    get(parentUrl + "/pipe/snapshot/info"), HttpResponse.BodyHandlers.ofString());
            if (resp.statusCode() != 200) {
                return false;
            }
            var node = objectMapper.readTree(resp.body());
            return node.path("available").asBoolean(false);
        } catch (Exception e) {
            log.warn("event=bootstrap.snapshot_info_failed parentUrl={} err={}", parentUrl, e.toString());
            return false;
        }
    }

    @Override
    public Path downloadSnapshot(String parentUrl, Path dataDir) {
        Path dest = dataDir.resolve("snapshots").resolve("incoming.zip");
        try {
            Files.createDirectories(dest.getParent());
            HttpResponse<Path> resp = http.send(
                    get(parentUrl + "/pipe/snapshot"),
                    HttpResponse.BodyHandlers.ofFile(dest, StandardOpenOption.CREATE,
                            StandardOpenOption.WRITE, StandardOpenOption.TRUNCATE_EXISTING));
            if (resp.statusCode() != 200) {
                throw new DataRefreshException(ErrorCode.DATA_REFRESH_SNAPSHOT_NOT_FOUND,
                        "Parent returned HTTP " + resp.statusCode() + " for snapshot download")
                        .withContext("parentUrl", parentUrl);
            }
            log.info("event=bootstrap.snapshot_downloaded parentUrl={} dest={} bytes={}",
                    parentUrl, dest, sizeOf(dest));
            return dest;
        } catch (DataRefreshException e) {
            throw e;
        } catch (Exception e) {
            throw new DataRefreshException(ErrorCode.DATA_REFRESH_SNAPSHOT_RESTORE_FAILED,
                    "Failed to download snapshot from " + parentUrl, e);
        }
    }

    @Override
    public void bulkFetchFromParent(String parentUrl, String dataDir) {
        String cursors = "{}"; // empty map → every topic starts at the floor (0)
        for (int batch = 0; batch < maxBatches; batch++) {
            HttpResponse<String> resp = pollMerge(parentUrl, cursors);
            if (resp.statusCode() == 204) {
                log.info("event=bootstrap.parent_pull_caught_up parentUrl={} batches={}", parentUrl, batch);
                return;
            }
            if (resp.statusCode() != 200) {
                throw new DataRefreshException(ErrorCode.DATA_REFRESH_REPLAY_FAILED,
                        "Parent pull HTTP " + resp.statusCode()).withContext("parentUrl", parentUrl);
            }
            ingestRecords(parseRecords(resp.body()));
            cursors = resp.headers().firstValue(CURSORS_HEADER).orElse(cursors);
        }
        log.warn("event=bootstrap.parent_pull_max_batches parentUrl={} maxBatches={}", parentUrl, maxBatches);
    }

    @Override
    public void bulkFetchFromCloud(String dataDir) {
        long offset = 0;
        for (int batch = 0; batch < maxBatches; batch++) {
            HttpResponse<String> resp = pollCloud(offset);
            if (resp.statusCode() == 204) {
                log.info("event=bootstrap.cloud_pull_caught_up batches={}", batch);
                return;
            }
            if (resp.statusCode() != 200) {
                throw new DataRefreshException(ErrorCode.DATA_REFRESH_REPLAY_FAILED,
                        "Cloud pull HTTP " + resp.statusCode());
            }
            List<MessageRecord> records = parseRecords(resp.body());
            if (records.isEmpty()) {
                return;
            }
            ingestRecords(records);
            long maxOffset = offset;
            for (MessageRecord r : records) {
                maxOffset = Math.max(maxOffset, r.getOffset());
            }
            if (maxOffset <= offset) {
                return; // no progress — caught up
            }
            offset = maxOffset;
        }
        log.warn("event=bootstrap.cloud_pull_max_batches maxBatches={} (synthetic looping cloud?)", maxBatches);
    }

    /** Append a record unless it is already stored (offset-idempotent dedup). Package-private for tests. */
    void ingestRecords(List<MessageRecord> records) {
        for (MessageRecord r : records) {
            long head = storage.getCurrentOffset(r.getTopic(), 0);
            if (r.getOffset() > 0 && r.getOffset() <= head) {
                continue; // already stored
            }
            storage.append(r.getTopic(), 0, r);
        }
    }

    List<MessageRecord> parseRecords(String body) {
        try {
            if (body == null || body.isBlank()) {
                return List.of();
            }
            return List.of(objectMapper.readValue(body, MessageRecord[].class));
        } catch (Exception e) {
            throw new DataRefreshException(ErrorCode.DATA_REFRESH_REPLAY_FAILED,
                    "Failed to parse pull response", e);
        }
    }

    private HttpResponse<String> pollMerge(String parentUrl, String cursors) {
        try {
            HttpRequest req = HttpRequest.newBuilder(URI.create(parentUrl + "/pipe/poll"))
                    .timeout(Duration.ofSeconds(30))
                    .header(CURSORS_HEADER, cursors)
                    .GET().build();
            return http.send(req, HttpResponse.BodyHandlers.ofString());
        } catch (Exception e) {
            throw new DataRefreshException(ErrorCode.DATA_REFRESH_REPLAY_FAILED,
                    "Parent pull request failed", e).withContext("parentUrl", parentUrl);
        }
    }

    private HttpResponse<String> pollCloud(long offset) {
        try {
            return http.send(get(cloudUrl + "/pipe/poll?offset=" + offset),
                    HttpResponse.BodyHandlers.ofString());
        } catch (Exception e) {
            throw new DataRefreshException(ErrorCode.DATA_REFRESH_REPLAY_FAILED, "Cloud pull request failed", e);
        }
    }

    private static HttpRequest get(String url) {
        return HttpRequest.newBuilder(URI.create(url)).timeout(Duration.ofSeconds(30)).GET().build();
    }

    private static long sizeOf(Path p) {
        try {
            return Files.size(p);
        } catch (Exception e) {
            return -1;
        }
    }
}
