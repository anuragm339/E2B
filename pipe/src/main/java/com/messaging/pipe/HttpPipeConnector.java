package com.messaging.pipe;

import com.messaging.common.api.PipeConnector;
import com.messaging.common.model.MessageRecord;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.micronaut.context.annotation.Value;
import jakarta.inject.Singleton;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.Duration;
import java.util.Properties;
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
    private static final String OFFSET_FILE = "pipe-offset.properties";
    private static final long PERSIST_INTERVAL_MS = 5000; // Persist every 5 seconds

    private final HttpClient httpClient;
    private final ObjectMapper objectMapper;
    private final ScheduledExecutorService scheduler;
    private final String dataDir;
    private final Path offsetFilePath;

    private volatile PipeConnectionImpl connection;
    private volatile Consumer<MessageRecord> dataHandler;
    private volatile boolean running;
    private volatile long currentOffset = 0;
    private volatile long lastPersistedOffset = -1;

    public HttpPipeConnector(@Value("${broker.storage.data-dir:./data}") String dataDir) {
        this.httpClient = HttpClient.newBuilder()
                .connectTimeout(Duration.ofSeconds(10))
                .build();
        this.objectMapper = new ObjectMapper();
        this.objectMapper.findAndRegisterModules();
        this.scheduler = Executors.newScheduledThreadPool(2); // 1 for polling, 1 for persistence
        this.dataDir = dataDir;
        this.offsetFilePath = Paths.get(dataDir, OFFSET_FILE);

        // Create data directory if needed
        try {
            Files.createDirectories(Paths.get(dataDir));
        } catch (IOException e) {
            log.error("Failed to create data directory", e);
        }

        // Load persisted offset
        loadOffset();

        log.info("HttpPipeConnector initialized with dataDir={}, offset={}", dataDir, currentOffset);
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

            // Start periodic offset persistence
            scheduler.scheduleWithFixedDelay(
                this::persistOffset,
                PERSIST_INTERVAL_MS,
                PERSIST_INTERVAL_MS,
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

        // Final persist before shutdown
        persistOffset();

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
                // Use actual offset from the record, not just increment
                currentOffset = record.getOffset() + 1;  // Next offset to fetch
                connection.lastReceivedOffset = record.getOffset();
                connection.lastMessageTime = System.currentTimeMillis();
            }

            if (records.length > 0) {
                log.info("Received {} messages from parent, current offset: {} (last record offset: {})",
                        records.length, currentOffset, connection.lastReceivedOffset);
            }

        } catch (Exception e) {
            log.error("Error handling poll response", e);
        }
    }

    /**
     * Load offset from properties file
     */
    private void loadOffset() {
        if (!Files.exists(offsetFilePath)) {
            log.info("No pipe offset file found, starting from offset 0");
            currentOffset = 0;
            return;
        }

        try (FileInputStream fis = new FileInputStream(offsetFilePath.toFile())) {
            Properties props = new Properties();
            props.load(fis);

            String offsetStr = props.getProperty("pipe.current.offset");
            if (offsetStr != null) {
                currentOffset = Long.parseLong(offsetStr);
                lastPersistedOffset = currentOffset;
                log.info("Loaded pipe offset from disk: {}", currentOffset);
            } else {
                currentOffset = 0;
            }
        } catch (Exception e) {
            log.error("Failed to load pipe offset from disk, starting from 0", e);
            currentOffset = 0;
        }
    }

    /**
     * Persist offset to properties file (only if changed)
     */
    private synchronized void persistOffset() {
        // Only persist if offset has changed
        if (currentOffset == lastPersistedOffset) {
            return;
        }

        try {
            // Write to temp file first
            Path tempFile = Paths.get(dataDir, OFFSET_FILE + ".tmp");

            Properties props = new Properties();
            props.setProperty("pipe.current.offset", String.valueOf(currentOffset));

            try (FileOutputStream fos = new FileOutputStream(tempFile.toFile())) {
                props.store(fos, "Pipe Offset - Updated: " + new java.util.Date());
            }

            // Atomic rename
            Files.move(tempFile, offsetFilePath, java.nio.file.StandardCopyOption.REPLACE_EXISTING);

            lastPersistedOffset = currentOffset;
            log.debug("Persisted pipe offset to disk: {}", currentOffset);
        } catch (Exception e) {
            log.error("Failed to persist pipe offset to disk", e);
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
