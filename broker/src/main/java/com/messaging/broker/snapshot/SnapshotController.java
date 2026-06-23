package com.messaging.broker.snapshot;

import io.micronaut.http.HttpResponse;
import io.micronaut.http.MediaType;
import io.micronaut.http.annotation.Controller;
import io.micronaut.http.annotation.Get;
import io.micronaut.http.annotation.Produces;
import io.micronaut.http.server.types.files.SystemFile;
import jakarta.inject.Inject;

import java.util.LinkedHashMap;
import java.util.Map;

/**
 * Serves the data snapshot to a bootstrapping child POS.
 *
 * <ul>
 *   <li>{@code GET /pipe/snapshot/info} — does a snapshot exist + its watermark/manifest, so the
 *       child can decide between the fast ZIP path and the incremental fallback.</li>
 *   <li>{@code GET /pipe/snapshot} — stream the latest snapshot ZIP.</li>
 * </ul>
 */
@Controller("/pipe/snapshot")
public class SnapshotController {

    private final SnapshotStore store;

    @Inject
    public SnapshotController(SnapshotStore store) {
        this.store = store;
    }

    @Get("/info")
    @Produces(MediaType.APPLICATION_JSON)
    public HttpResponse<Map<String, Object>> info() {
        SnapshotManifest manifest = store.currentManifest();
        if (manifest == null) {
            return HttpResponse.ok(Map.of("available", false));
        }
        Map<String, Object> body = new LinkedHashMap<>();
        body.put("available", true);
        body.put("schemaVersion", manifest.getSchemaVersion());
        body.put("createdAtMs", manifest.getCreatedAtMs());
        body.put("topicHeads", manifest.getTopicHeads());
        return HttpResponse.ok(body);
    }

    @Get
    @Produces(MediaType.APPLICATION_OCTET_STREAM)
    public HttpResponse<SystemFile> download() {
        if (!store.exists()) {
            return HttpResponse.notFound();
        }
        return HttpResponse.ok(new SystemFile(store.latestZip().toFile()).attach("snapshot.zip"));
    }
}
