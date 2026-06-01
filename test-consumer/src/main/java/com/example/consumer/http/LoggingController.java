package com.example.consumer.http;

import com.example.consumer.logging.RuntimeLogLevelService;
import io.micronaut.http.MediaType;
import io.micronaut.http.annotation.Controller;
import io.micronaut.http.annotation.Get;
import io.micronaut.http.annotation.PathVariable;
import io.micronaut.http.annotation.Post;
import io.micronaut.http.annotation.Produces;
import io.micronaut.http.annotation.QueryValue;
import jakarta.inject.Inject;

import java.util.LinkedHashMap;
import java.util.Map;

@Controller("/admin/logging")
public class LoggingController {

    private final RuntimeLogLevelService runtimeLogLevelService;

    @Inject
    public LoggingController(RuntimeLogLevelService runtimeLogLevelService) {
        this.runtimeLogLevelService = runtimeLogLevelService;
    }

    @Get("/features")
    @Produces(MediaType.APPLICATION_JSON)
    public Map<String, Object> features() {
        Map<String, Object> response = new LinkedHashMap<>();
        response.put("scope", "consumer");
        response.put("features", runtimeLogLevelService.describeAll());
        response.put("tip", "POST /admin/logging/feature/{feature}?level=DEBUG and later set level=INFO or RESET");
        return response;
    }

    @Get("/feature/{feature}")
    @Produces(MediaType.APPLICATION_JSON)
    public Map<String, Object> feature(@PathVariable String feature) {
        Map<String, Object> response = new LinkedHashMap<>();
        response.put("scope", "consumer");
        response.put("feature", runtimeLogLevelService.describeFeature(feature));
        return response;
    }

    @Post("/feature/{feature}")
    @Produces(MediaType.APPLICATION_JSON)
    public Map<String, Object> setFeatureLevel(@PathVariable String feature,
                                               @QueryValue String level) {
        Map<String, Object> response = new LinkedHashMap<>();
        response.put("scope", "consumer");
        response.put("feature", runtimeLogLevelService.setFeatureLevel(feature, level));
        response.put("level", level);
        return response;
    }
}
