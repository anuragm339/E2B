package com.messaging.broker.http;

import com.messaging.broker.snapshot.BootstrapProgressTracker;
import com.messaging.broker.snapshot.DownloadRefreshResult;
import com.messaging.broker.snapshot.DownloadRefreshService;
import com.messaging.broker.snapshot.RefreshType;
import io.micronaut.core.annotation.Nullable;
import io.micronaut.http.MediaType;
import io.micronaut.http.annotation.Body;
import io.micronaut.http.annotation.Controller;
import io.micronaut.http.annotation.Consumes;
import io.micronaut.http.annotation.Get;
import io.micronaut.http.annotation.Post;
import io.micronaut.http.annotation.Produces;
import io.micronaut.http.annotation.QueryValue;
import jakarta.inject.Inject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.LinkedHashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Admin API to trigger a download-refresh: wipe local data and re-source it from the parent POS
 * (snapshot or incremental) or the cloud, then refresh connected consumers.
 *
 * <p>The refresh type is chosen by the caller via {@code ?type=} or a JSON body {@code {"type":..}}:
 * {@code LOCAL} (replay local segments), {@code DOWNLOAD} (wipe + auto-selected source — default),
 * or a forced source {@code SNAPSHOT}/{@code INCREMENTAL}/{@code CLOUD}.
 *
 * <ul>
 *   <li>{@code POST /admin/download-refresh?type=DOWNLOAD} — start it (async); returns immediately.</li>
 *   <li>{@code GET /admin/download-refresh/status} — last run's type/source/outcome.</li>
 * </ul>
 */
@Controller("/admin/download-refresh")
public class DownloadRefreshController {
    private static final Logger log = LoggerFactory.getLogger(DownloadRefreshController.class);

    private final DownloadRefreshService service;
    private final BootstrapProgressTracker progress;
    private final AtomicBoolean running = new AtomicBoolean(false);
    private volatile DownloadRefreshResult lastResult;
    private volatile RefreshType lastType;

    @Inject
    public DownloadRefreshController(DownloadRefreshService service, BootstrapProgressTracker progress) {
        this.service = service;
        this.progress = progress;
    }

    @Post
    @Consumes(MediaType.APPLICATION_JSON)
    @Produces(MediaType.APPLICATION_JSON)
    public io.micronaut.http.HttpResponse<Map<String, Object>> trigger(
            @QueryValue @Nullable String type, @Body @Nullable Map<String, Object> body) {
        // Type comes from ?type= or {"type":..}; blank defaults to PIPE_AND_PROVIDER_REFRESH.
        String raw = (type != null && !type.isBlank())
                ? type
                : (body != null && body.get("type") != null ? body.get("type").toString() : null);

        // Reject a typo with 400 — never silently run a destructive refresh on an unknown type.
        var parsed = RefreshType.tryParse(raw);
        if (parsed.isEmpty()) {
            return io.micronaut.http.HttpResponse.badRequest(Map.of("status", "INVALID_TYPE", "type", raw));
        }
        RefreshType refreshType = parsed.get();

        if (!running.compareAndSet(false, true)) {
            return io.micronaut.http.HttpResponse.ok(Map.of("status", "ALREADY_RUNNING"));
        }
        lastType = refreshType;
        log.info("event=refresh.requested type={}", refreshType);
        // Run off the request thread: bootstrap downloads/pulls can take a while.
        Thread.ofVirtual().name("refresh-" + refreshType).start(() -> {
            try {
                lastResult = service.runRefresh(refreshType);
            } catch (Exception e) {
                log.error("event=refresh.unexpected_error type={} err={}", refreshType, e.toString(), e);
            } finally {
                running.set(false);
            }
        });
        return io.micronaut.http.HttpResponse.ok(Map.of("status", "INITIATED", "type", refreshType.toString()));
    }

    @Get("/status")
    @Produces(MediaType.APPLICATION_JSON)
    public Map<String, Object> status() {
        Map<String, Object> body = new LinkedHashMap<>();
        body.put("running", running.get());
        body.put("type", lastType != null ? lastType.toString() : null);
        body.put("progress", progress.snapshot());
        DownloadRefreshResult r = lastResult;
        if (r == null) {
            body.put("lastRun", "none");
            return body;
        }
        body.put("success", r.isSuccess());
        // source is null for a LOCAL refresh (no download).
        body.put("source", r.getSource() != null ? r.getSource().toString() : null);
        if (r.getError() != null) {
            body.put("error", r.getError());
        }
        if (r.getManifest() != null) {
            body.put("topics", r.getManifest().getTopicHeads().size());
        }
        return body;
    }
}
