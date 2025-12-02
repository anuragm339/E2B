package com.messaging.pipe;

import com.messaging.common.api.PipeConnector;
import com.messaging.common.model.MessageRecord;
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
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;

/**
 * HTTP-based implementation of PipeConnector
 * Polls parent broker for new messages via HTTP
 */
@Singleton
public class HttpPipeConnector implements PipeConnector {
    private static final Logger log = LoggerFactory.getLogger(HttpPipeConnector.class);
    private static final long POLL_INTERVAL_MS = 1000; // Poll every 1 second
    private static final int BATCH_SIZE = 100;

    private final HttpClient httpClient;
    private final ObjectMapper objectMapper;
    private final ScheduledExecutorService scheduler;

    private volatile PipeConnectionImpl connection;
    private volatile Consumer<MessageRecord> dataHandler;
    private volatile boolean running;
    private long currentOffset = 0;

    public HttpPipeConnector() {
        this.httpClient = HttpClient.newBuilder()
                .connectTimeout(Duration.ofSeconds(10))
                .build();
        this.objectMapper = new ObjectMapper();
        this.objectMapper.findAndRegisterModules();
        this.scheduler = Executors.newScheduledThreadPool(1);
        log.info("HttpPipeConnector initialized");
    }

    @Override
    public CompletableFuture<PipeConnection> connectToParent(String parentUrl) {
        log.info("Connecting to parent broker: {}", parentUrl);

        return CompletableFuture.supplyAsync(() -> {
            this.connection = new PipeConnectionImpl(parentUrl);
            this.running = true;

            // Start polling for messages
            scheduler.scheduleWithFixedDelay(
                this::pollParent,
                0,
                POLL_INTERVAL_MS,
                TimeUnit.MILLISECONDS
            );

            log.info("Connected to parent: {}", parentUrl);
            return connection;
        });
    }

    @Override
    public void onDataReceived(Consumer<MessageRecord> handler) {
        this.dataHandler = handler;
        log.info("Registered data handler");
    }

    @Override
    public CompletableFuture<Void> sendAck(long offset) {
        // No ACK needed - child tracks its own offset locally
        log.debug("Child offset updated locally: {}", offset);
        return CompletableFuture.completedFuture(null);
    }

    @Override
    public PipeHealth getHealth() {
        if (connection == null || !connection.isConnected()) {
            return PipeHealth.UNHEALTHY;
        }

        // Check if we're receiving data
        long timeSinceLastMessage = System.currentTimeMillis() - connection.lastMessageTime;
        if (timeSinceLastMessage > 60000) { // 1 minute
            return PipeHealth.DEGRADED;
        }

        return PipeHealth.HEALTHY;
    }

    @Override
    public void reconnect() {
        log.info("Reconnecting to parent...");
        if (connection != null) {
            disconnect();
            connectToParent(connection.getParentUrl());
        }
    }

    @Override
    public void disconnect() {
        log.info("Disconnecting from parent...");
        running = false;
        scheduler.shutdown();
        if (connection != null) {
            connection.connected = false;
        }
        log.info("Disconnected");
    }

    /**
     * Poll parent broker for new messages
     */
    private void pollParent() {
        if (!running || connection == null) {
            return;
        }

        try {
            String parentUrl = connection.getParentUrl();
            // Add http:// if not present
            if (!parentUrl.startsWith("http://") && !parentUrl.startsWith("https://")) {
                parentUrl = "http://" + parentUrl;
            }
            String pollUrl = parentUrl + "/pipe/poll?offset=" + currentOffset + "&limit=" + BATCH_SIZE;

            HttpRequest request = HttpRequest.newBuilder()
                    .uri(URI.create(pollUrl))
                    .GET()
                    .timeout(Duration.ofSeconds(5))
                    .build();

            httpClient.sendAsync(request, HttpResponse.BodyHandlers.ofString())
                    .thenAccept(response -> {
                        if (response.statusCode() == 200) {
                            handlePollResponse(response.body());
                        } else if (response.statusCode() != 204) { // 204 = No content
                            log.warn("Poll failed with status: {}", response.statusCode());
                        }
                    })
                    .exceptionally(ex -> {
                        log.error("Error polling parent", ex);
                        return null;
                    });

        } catch (Exception e) {
            log.error("Error in pollParent", e);
        }
    }

    /**
     * Handle response from parent poll
     */
    private void handlePollResponse(String responseBody) {
        try {
            if (responseBody == null || responseBody.isEmpty()) {
                return;
            }

            // Parse response as array of MessageRecords
            MessageRecord[] records = objectMapper.readValue(responseBody, MessageRecord[].class);

            for (MessageRecord record : records) {
                if (dataHandler != null) {
                    dataHandler.accept(record);
                }
                currentOffset++;
                connection.lastReceivedOffset = currentOffset;
                connection.lastMessageTime = System.currentTimeMillis();
            }

            if (records.length > 0) {
                log.info("Received {} messages from parent, current offset: {}",
                        records.length, currentOffset);
            }

        } catch (Exception e) {
            log.error("Error handling poll response", e);
        }
    }

    /**
     * Implementation of PipeConnection
     */
    private static class PipeConnectionImpl implements PipeConnection {
        private final String parentUrl;
        private volatile boolean connected;
        private volatile long lastReceivedOffset;
        private volatile long lastMessageTime;

        PipeConnectionImpl(String parentUrl) {
            this.parentUrl = parentUrl;
            this.connected = true;
            this.lastReceivedOffset = 0;
            this.lastMessageTime = System.currentTimeMillis();
        }

        @Override
        public boolean isConnected() {
            return connected;
        }

        @Override
        public String getParentUrl() {
            return parentUrl;
        }

        @Override
        public long getLastReceivedOffset() {
            return lastReceivedOffset;
        }
    }
}
