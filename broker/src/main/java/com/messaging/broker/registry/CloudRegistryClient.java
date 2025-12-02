package com.messaging.broker.registry;

import com.messaging.common.model.TopologyResponse;
import com.fasterxml.jackson.databind.ObjectMapper;
import jakarta.inject.Singleton;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Duration;
import java.util.concurrent.CompletableFuture;

/**
 * Client for querying Cloud Registry to get topology information
 */
@Singleton
public class CloudRegistryClient {
    private static final Logger log = LoggerFactory.getLogger(CloudRegistryClient.class);

    private final HttpClient httpClient;
    private final ObjectMapper objectMapper;

    public CloudRegistryClient() {
        this.httpClient = HttpClient.newBuilder()
                .connectTimeout(Duration.ofSeconds(10))
                .build();
        this.objectMapper = new ObjectMapper();
        this.objectMapper.findAndRegisterModules();
        log.info("CloudRegistryClient initialized");
    }

    /**
     * Query Cloud Registry for topology information
     *
     * @param registryUrl URL of the Cloud Registry (e.g., "http://localhost:8080")
     * @param nodeId      This broker's node ID (e.g., "local-001", "broker-root")
     * @return TopologyResponse with parent URLs and role
     */
    public CompletableFuture<TopologyResponse> getTopology(String registryUrl, String nodeId) {
        return CompletableFuture.supplyAsync(() -> {
            try {
                String url = registryUrl + "/registry/topology?nodeId=" + nodeId;
                log.debug("Querying Cloud Registry: {}", url);

                HttpRequest request = HttpRequest.newBuilder()
                        .uri(URI.create(url))
                        .GET()
                        .timeout(Duration.ofSeconds(5))
                        .build();

                HttpResponse<String> response = httpClient.send(request, HttpResponse.BodyHandlers.ofString());

                if (response.statusCode() != 200) {
                    log.error("Failed to query Cloud Registry: status={}", response.statusCode());
                    throw new RuntimeException("Cloud Registry query failed: " + response.statusCode());
                }

                TopologyResponse topology = objectMapper.readValue(response.body(), TopologyResponse.class);
                log.info("Received topology from Cloud: nodeId={}, role={}, parents={}",
                        topology.getNodeId(), topology.getRole(), topology.getRequestToFollow());

                return topology;

            } catch (Exception e) {
                log.error("Error querying Cloud Registry", e);
                throw new RuntimeException("Failed to query Cloud Registry", e);
            }
        });
    }
}
