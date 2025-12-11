package com.messaging.broker.registry;

import com.messaging.common.api.PipeConnector;
import com.messaging.common.model.MessageRecord;
import com.messaging.common.model.TopologyResponse;
import io.micronaut.context.annotation.Value;
import jakarta.annotation.PreDestroy;
import jakarta.inject.Singleton;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.List;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Consumer;

/**
 * Manages broker topology by periodically querying Cloud Registry
 * and maintaining parent connections
 */
@Singleton
public class TopologyManager {
    private static final Logger log = LoggerFactory.getLogger(TopologyManager.class);
    private static final long REGISTRY_POLL_INTERVAL_MS = 30000; // Query every 30 seconds
    private static final AtomicInteger threadCounter = new AtomicInteger(0);

    private final CloudRegistryClient registryClient;
    private final PipeConnector pipeConnector;
    private final String registryUrl;
    private final String nodeId;
    private final ScheduledExecutorService scheduler;
    private final TopologyPropertiesStore propertiesStore;

    private volatile String currentParentUrl;
    private volatile TopologyResponse currentTopology;
    private volatile Consumer<MessageRecord> messageHandler;
    private volatile boolean running;

    public TopologyManager(
            CloudRegistryClient registryClient,
            PipeConnector pipeConnector,
            @Value("${broker.registry.url}") String registryUrl,
            @Value("${broker.nodeId:local-001}") String nodeId,
            @Value("${broker.storage.data-dir:./data}") String dataDir) {

        this.registryClient = registryClient;
        this.pipeConnector = pipeConnector;
        this.registryUrl = registryUrl;
        this.nodeId = nodeId;
        this.scheduler = Executors.newScheduledThreadPool(Runtime.getRuntime().availableProcessors(), r -> {
            Thread t = new Thread(r);
            t.setName("TopologyManager-" + threadCounter.incrementAndGet());
            return t;
        });
        this.running = false;

        // Initialize properties store
        Path dataDirPath = Paths.get(dataDir);
        this.propertiesStore = new TopologyPropertiesStore(dataDirPath);

        log.info("TopologyManager initialized: registryUrl={}, nodeId={}", registryUrl, nodeId);
    }

    /**
     * Start topology management - query registry and maintain parent connection
     */
    public void start() {
        if (registryUrl == null || registryUrl.isEmpty()) {
            log.info("No registry URL configured, skipping topology management");
            return;
        }

        log.info("Starting topology manager...");
        running = true;

        // Schedule periodic registry queries
        scheduler.scheduleWithFixedDelay(
                this::queryAndUpdateTopology,
                0,  // Initial delay
                REGISTRY_POLL_INTERVAL_MS,
                TimeUnit.MILLISECONDS
        );
    }

    /**
     * Register handler for messages received from parent
     */
    public void onMessageReceived(Consumer<MessageRecord> handler) {
        this.messageHandler = handler;
    }

    /**
     * Query Cloud Registry and update topology if changed
     */
    private void queryAndUpdateTopology() {
        if (!running) {
            return;
        }

        try {
            log.debug("Querying Cloud Registry for topology...");

            registryClient.getTopology(registryUrl, nodeId).whenComplete((topology, ex) -> {
                if (ex != null) {
//                    log.error("Failed to query Cloud Registry", ex);
                    return;
                }

                handleTopologyUpdate(topology);
            });

        } catch (Exception e) {
            log.error("Error in queryAndUpdateTopology", e);
        }
    }

    /**
     * Handle topology update from Cloud Registry
     */
    private void handleTopologyUpdate(TopologyResponse topology) {
        try {
            log.info("Topology update: role={}, parents={}", topology.getRole(), topology.getRequestToFollow());

            // Save topology to properties file
            propertiesStore.saveTopology(topology);

            // Check if parent has changed
            List<String> parents = topology.getRequestToFollow();
            String newParentUrl = (parents != null && !parents.isEmpty()) ? parents.get(0) : null;

            if (newParentUrl == null) {
                log.info("No parent assigned - running as ROOT broker");
                if (currentParentUrl != null) {
                    disconnectFromParent();
                }
                return;
            }

            // If parent changed, reconnect
            if (!newParentUrl.equals(currentParentUrl)) {
                log.info("Parent changed from {} to {}", currentParentUrl, newParentUrl);

                // Disconnect from old parent
                if (currentParentUrl != null) {
                    disconnectFromParent();
                }

                // Connect to new parent
                connectToParent(newParentUrl);
            }

            currentTopology = topology;

        } catch (Exception e) {
            log.error("Error handling topology update", e);
        }
    }

    /**
     * Connect to parent broker
     */
    private void connectToParent(String parentUrl) {
        log.info("Connecting to parent: {}", parentUrl);

        pipeConnector.connectToParent(parentUrl).whenComplete((connection, ex) -> {
            if (ex != null) {
                log.error("Failed to connect to parent: {}", parentUrl, ex);
                return;
            }

            log.info("Connected to parent: {}", parentUrl);
            currentParentUrl = parentUrl;

            // Register message handler
            if (messageHandler != null) {
                pipeConnector.onDataReceived(messageHandler);
            }
        });
    }

    /**
     * Disconnect from current parent
     */
    private void disconnectFromParent() {
        log.info("Disconnecting from parent: {}", currentParentUrl);
        pipeConnector.disconnect();
        currentParentUrl = null;
    }

    /**
     * Get current topology
     */
    public TopologyResponse getCurrentTopology() {
        return currentTopology;
    }

    /**
     * Get current parent URL
     */
    public String getCurrentParentUrl() {
        return currentParentUrl;
    }

    @PreDestroy
    public void shutdown() {
        log.info("Shutting down topology manager...");
        running = false;
        scheduler.shutdown();

        if (currentParentUrl != null) {
            disconnectFromParent();
        }

        log.info("Topology manager shutdown complete");
    }
}
