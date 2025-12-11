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
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;
import java.util.stream.Collectors;

/**
 * HTTP-based implementation of PipeConnector
 * Polls parent broker for new messages via HTTP
 */
@Singleton
public class HttpPipeConnector implements PipeConnector {
    private static final Logger log = LoggerFactory.getLogger(HttpPipeConnector.class);
    private static final long MIN_POLL_INTERVAL_MS = 100; // Min delay between polls
    private static final long MAX_POLL_INTERVAL_MS = 5000; // Max backoff when no data
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
    private volatile long currentOffset = 1012001;
    private volatile long lastPersistedOffset = -1;
    private volatile long lastPollDuration = 0;
    private volatile long adaptiveDelay = MIN_POLL_INTERVAL_MS;

    public HttpPipeConnector(@Value("${broker.storage.data-dir:./data}") String dataDir) {
        this.httpClient = HttpClient.newBuilder()
                .connectTimeout(Duration.ofSeconds(10))
                .build();
        this.objectMapper = new ObjectMapper();
        this.objectMapper.findAndRegisterModules();
        this.scheduler = Executors.newScheduledThreadPool(Runtime.getRuntime().availableProcessors(),runnable -> {
            Thread t = new Thread(runnable);
            t.setName("HttpPipeConnector" + t.getId());
            return t;
        });
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

            // Start adaptive polling for messages (schedules next poll after completion)
            scheduleNextPoll(0);

            // Start periodic offset persistence
            scheduler.scheduleWithFixedDelay(
                this::persistOffset,
                PERSIST_INTERVAL_MS,
                PERSIST_INTERVAL_MS,
                TimeUnit.MILLISECONDS
            );

            log.info("Connected to parent: {} with adaptive polling", parentUrl);
            return connection;
        }, scheduler);
    }

    /**
     * Schedule next poll with adaptive delay based on previous response time and data availability
     */
    private void scheduleNextPoll(long delayMs) {
        if (running) {
            scheduler.schedule(this::pollParentAdaptive, delayMs, TimeUnit.MILLISECONDS);
        }
    }

    /**
     * Poll parent and schedule next poll based on response
     */
    private void pollParentAdaptive() {
        long startTime = System.currentTimeMillis();
        try {
            int messagesReceived = pollParent();
            lastPollDuration = System.currentTimeMillis() - startTime;

            // Adaptive delay calculation
            if (messagesReceived > 0) {
                // Got data - poll again quickly (but wait at least MIN_POLL_INTERVAL_MS)
                adaptiveDelay = Math.max(MIN_POLL_INTERVAL_MS, lastPollDuration / 2);
                log.debug("Received {} messages, next poll in {}ms", messagesReceived, adaptiveDelay);
            } else {
                // No data - exponential backoff up to MAX_POLL_INTERVAL_MS
                adaptiveDelay = Math.min(adaptiveDelay * 2, MAX_POLL_INTERVAL_MS);
                log.debug("No messages, backing off to {}ms", adaptiveDelay);
            }

        } catch (Exception e) {
            log.error("Error in adaptive polling", e);
            // On error, back off more
            adaptiveDelay = Math.min(adaptiveDelay * 3, MAX_POLL_INTERVAL_MS);
        } finally {
            // Schedule next poll
            scheduleNextPoll(adaptiveDelay);
        }
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
     * Poll parent broker for new messages (synchronous with timeout)
     * @return number of messages received
     */
    private int pollParent() {
        if (!running || connection == null) {
            return 0;
        }

        try {
            String parentUrl = connection.getParentUrl();
            // Add http:// if not present
            if (!parentUrl.startsWith("http://") && !parentUrl.startsWith("https://")) {
                parentUrl = "http://" + parentUrl;
            }

            String pollUrl = parentUrl + "/pipe/poll?offset=" + currentOffset + "&limit=" + 10;

            HttpRequest request = HttpRequest.newBuilder()
                    .uri(URI.create(pollUrl))
                    .timeout(Duration.ofSeconds(10))  // Request timeout
                    .GET()
                    .build();

            // Synchronous call with timeout to prevent indefinite waiting
            HttpResponse<String> response = httpClient.send(request, HttpResponse.BodyHandlers.ofString());

            if (response.statusCode() == 200) {
                return handlePollResponse(response.body());
            } else if (response.statusCode() != 204) { // 204 = No content
                log.warn("Poll failed with status: {}", response.statusCode());
            }

            return 0;

        } catch (Exception e) {
            log.error("Error in pollParent", e);
            return 0;
        }
    }

    /**
     * Handle response from parent poll
     * @return number of messages received
     */
    private int handlePollResponse(String responseBody) {
        try {
            if (responseBody == null || responseBody.isEmpty()) {
                return 0;
            }

            // Parse response as array of MessageRecords
            MessageRecord[] records = objectMapper.readValue(responseBody, MessageRecord[].class);
            List<MessageRecord> list = Arrays.asList(records);
            Map<String, List<MessageRecord>> collect = list.stream().collect(Collectors.groupingBy(MessageRecord::getTopic));
            collect.forEach((s, messageRecords) -> {
                log.info("Topic: {}, Records: {}", s, messageRecords.size());
            });
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
                log.debug("Received {} messages from parent, current offset: {} (last record offset: {})",
                        records.length, currentOffset, connection.lastReceivedOffset);
            }

            return records.length;

        } catch (Exception e) {
            log.error("Error handling poll response", e);
            return 0;
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
