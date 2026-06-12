package com.messaging.broker.http;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.messaging.broker.consistency.PipeConsistencyReport;
import com.messaging.broker.consistency.PipeConsistencyService;
import io.micronaut.http.HttpResponse;
import io.micronaut.http.HttpStatus;
import io.micronaut.http.MediaType;
import io.micronaut.http.annotation.Controller;
import io.micronaut.http.annotation.Get;
import io.micronaut.http.annotation.Post;
import io.micronaut.http.annotation.Produces;
import io.micronaut.http.annotation.QueryValue;
import jakarta.inject.Inject;
import jakarta.inject.Named;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ExecutorService;

/**
 * Local admin trigger + report for pipe-consistency checks.
 *
 * <pre>
 * POST /admin/pipe-consistency/check?topic=all&target=parent   — start a check (async)
 * GET  /admin/pipe-consistency/report                          — latest verdict per topic + history
 * </pre>
 *
 * Checks run on the shared {@code compactionExecutor} (CLAUDE.md: prefer ExecutorFactory pools);
 * the service's single-flight guard rejects overlapping runs. Detect-only — this controller
 * never mutates broker state.
 */
@Controller("/admin/pipe-consistency")
public class PipeConsistencyAdminController {

    private static final Logger log = LoggerFactory.getLogger(PipeConsistencyAdminController.class);

    private final PipeConsistencyService service;
    private final ExecutorService compactionExecutor;
    private final ObjectMapper objectMapper = new ObjectMapper();

    @Inject
    public PipeConsistencyAdminController(
            PipeConsistencyService service,
            @Named("compactionExecutor") ExecutorService compactionExecutor) {
        this.service = service;
        this.compactionExecutor = compactionExecutor;
    }

    @Post("/check")
    @Produces(MediaType.APPLICATION_JSON)
    public HttpResponse<String> check(
            @QueryValue(defaultValue = "all") String topic,
            @QueryValue(defaultValue = "parent") String target) {
        if (!service.isEnabled()) {
            return HttpResponse.status(HttpStatus.SERVICE_UNAVAILABLE)
                    .body("{\"error\":\"pipe.consistency.enabled=false\"}");
        }
        if (!PipeConsistencyService.TARGET_PARENT.equalsIgnoreCase(target)
                && !PipeConsistencyService.TARGET_CLOUD.equalsIgnoreCase(target)) {
            return HttpResponse.badRequest("{\"error\":\"target must be parent or cloud\"}");
        }
        if (service.isRunning()) {
            return HttpResponse.status(HttpStatus.CONFLICT)
                    .body("{\"error\":\"a consistency check is already running\"}");
        }

        compactionExecutor.execute(() -> {
            try {
                if (service.runCheck(topic, target) == null) {
                    log.info("event=pipe_consistency.run_skipped reason=already_running");
                }
            } catch (Throwable t) {
                // Pool has no uncaught-exception surfacing for execute() — log here.
                log.error("event=pipe_consistency.run_failed topic={} target={}", topic, target, t);
            }
        });
        log.info("event=pipe_consistency.run_started topic={} target={}", topic, target);
        return HttpResponse.accepted().body(
                "{\"status\":\"started\",\"topic\":\"" + topic + "\",\"target\":\"" + target
                + "\",\"report\":\"GET /admin/pipe-consistency/report\"}");
    }

    @Get("/report")
    @Produces(MediaType.APPLICATION_JSON)
    public HttpResponse<String> report() {
        try {
            Map<String, Object> body = new HashMap<>();
            body.put("enabled", service.isEnabled());
            body.put("running", service.isRunning());
            body.put("latestByTopic", service.latestReports());
            body.put("history", service.reportHistory());
            return HttpResponse.ok(objectMapper.writeValueAsString(body));
        } catch (Exception e) {
            log.error("event=pipe_consistency.report_failed", e);
            return HttpResponse.serverError("{\"error\":\"" + e.getMessage() + "\"}");
        }
    }
}
