package com.messaging.broker.http;

import com.messaging.broker.snapshot.BootstrapProgressTracker;
import com.messaging.broker.snapshot.DownloadRefreshResult;
import com.messaging.broker.snapshot.DownloadRefreshService;
import io.micronaut.http.MediaType;
import io.micronaut.http.annotation.Controller;
import io.micronaut.http.annotation.Get;
import io.micronaut.http.annotation.Post;
import io.micronaut.http.annotation.Produces;
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
 * <ul>
 *   <li>{@code POST /admin/download-refresh} — start it (async); returns immediately.</li>
 *   <li>{@code GET /admin/download-refresh/status} — last run's source/outcome.</li>
 * </ul>
 */
@Controller("/admin/download-refresh")
public class DownloadRefreshController {
    private static final Logger log = LoggerFactory.getLogger(DownloadRefreshController.class);

    private final DownloadRefreshService service;
    private final BootstrapProgressTracker progress;
    private final AtomicBoolean running = new AtomicBoolean(false);
    private volatile DownloadRefreshResult lastResult;

    @Inject
    public DownloadRefreshController(DownloadRefreshService service, BootstrapProgressTracker progress) {
        this.service = service;
        this.progress = progress;
    }

    @Post
    @Produces(MediaType.APPLICATION_JSON)
    public Map<String, Object> trigger() {
        if (!running.compareAndSet(false, true)) {
            return Map.of("status", "ALREADY_RUNNING");
        }
        // Run off the request thread: bootstrap downloads/pulls can take a while.
        Thread.ofVirtual().name("download-refresh").start(() -> {
            try {
                lastResult = service.runBootstrapAndRefresh();
            } catch (Exception e) {
                log.error("event=download_refresh.unexpected_error err={}", e.toString(), e);
            } finally {
                running.set(false);
            }
        });
        return Map.of("status", "INITIATED");
    }

    @Get("/status")
    @Produces(MediaType.APPLICATION_JSON)
    public Map<String, Object> status() {
        Map<String, Object> body = new LinkedHashMap<>();
        body.put("running", running.get());
        body.put("progress", progress.snapshot());
        DownloadRefreshResult r = lastResult;
        if (r == null) {
            body.put("lastRun", "none");
            return body;
        }
        body.put("success", r.isSuccess());
        body.put("source", r.getSource().toString());
        if (r.getError() != null) {
            body.put("error", r.getError());
        }
        if (r.getManifest() != null) {
            body.put("topics", r.getManifest().getTopicHeads().size());
        }
        return body;
    }
}
