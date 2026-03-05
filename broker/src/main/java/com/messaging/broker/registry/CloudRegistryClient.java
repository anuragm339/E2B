package com.messaging.broker.registry;

import com.messaging.common.exception.ErrorCode;
import com.messaging.common.exception.MessagingException;
import com.messaging.common.model.TopologyResponse;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.micronaut.http.HttpRequest;
import io.micronaut.http.HttpResponse;
import io.micronaut.http.client.HttpClient;
import io.micronaut.http.client.annotation.Client;
import jakarta.inject.Inject;
import jakarta.inject.Singleton;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.CompletableFuture;

/**
 * Client for querying Cloud Registry to get topology information
 */
@Singleton
public class CloudRegistryClient {
    private static final Logger log = LoggerFactory.getLogger(CloudRegistryClient.class);

    private final HttpClient httpClient;
    private final ObjectMapper objectMapper;

    @Inject
    public CloudRegistryClient(@Client("/") HttpClient httpClient) {
        this.httpClient = httpClient;
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

                HttpRequest<?> request = HttpRequest.GET(url);

                HttpResponse<String> response = httpClient.toBlocking().exchange(request, String.class);

                if (response.getStatus().getCode() != 200) {
                    log.error("Failed to query Cloud Registry: status={}", response.getStatus().getCode());
                    throw new MessagingException(ErrorCode.REGISTRY_TOPOLOGY_FETCH_FAILED,
                        "Cloud Registry query failed: " + response.getStatus().getCode())
                        .withContext("registryUrl", registryUrl)
                        .withContext("nodeId", nodeId)
                        .withContext("statusCode", response.getStatus().getCode());
                }

                String body = response.body();
                if (body == null) {
                    throw new MessagingException(ErrorCode.REGISTRY_TOPOLOGY_FETCH_FAILED,
                            "Cloud Registry response body was empty")
                            .withContext("registryUrl", registryUrl)
                            .withContext("nodeId", nodeId);
                }
                TopologyResponse topology = objectMapper.readValue(body, TopologyResponse.class);
                log.info("Received topology from Cloud: nodeId={}, role={}, parents={}",
                        topology.getNodeId(), topology.getRole(), topology.getRequestToFollow());

                return topology;

            } catch (MessagingException e) {
                // CompletableFuture lambda can't throw checked exceptions - wrap in RuntimeException
                throw new RuntimeException("Failed to query Cloud Registry: " + e.getMessage(), e);
            } catch (Exception e) {
                MessagingException ex = new MessagingException(ErrorCode.REGISTRY_TOPOLOGY_FETCH_FAILED,
                    "Failed to query Cloud Registry", e)
                    .withContext("registryUrl", registryUrl)
                    .withContext("nodeId", nodeId);
                throw new RuntimeException("Failed to query Cloud Registry", ex);
            }
        });
    }
}
