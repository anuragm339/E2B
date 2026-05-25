package com.messaging.pipe;

import com.messaging.common.api.PipeConnector;
import com.messaging.common.exception.ErrorCode;
import com.messaging.common.exception.MessagingException;
import com.messaging.common.exception.StorageException;
import com.messaging.common.model.MessageRecord;
import com.messaging.pipe.metrics.PipeMetrics;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonToken;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.micronaut.http.client.HttpClient;
import io.micronaut.http.client.annotation.Client;
import io.micrometer.core.instrument.Timer;
import io.micronaut.context.annotation.Value;
import jakarta.inject.Singleton;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.net.URI;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.file.*;
import java.util.Properties;
import java.util.concurrent.*;
import java.util.function.Function;

/**
 * HTTP-based PipeConnector with streaming JSON parsing
 * Memory-safe and Docker-friendly
 */
@Singleton
public class HttpPipeConnector implements PipeConnector {

    private static final Logger log = LoggerFactory.getLogger(HttpPipeConnector.class);

    private static final String OFFSET_FILE = "pipe-offset.properties";

    private final HttpClient httpClient;  // kept for Micronaut injection only; not used in pollParent
    private final java.net.http.HttpClient streamingHttpClient;
    private final ObjectMapper objectMapper;
    private final ScheduledExecutorService scheduler;
    private final Path offsetFilePath;
    private final PipeMetrics metrics;
    private final long minPollIntervalMs;
    private final long maxPollIntervalMs;
    private final int pollLimit;

    private volatile PipeConnectionImpl connection;
    private volatile Function<MessageRecord, Boolean> dataHandler;
    private volatile boolean running;
    private volatile boolean pausePipeCalls = false;  // For DataRefresh support

    private volatile long currentOffset = 0;
    private volatile long lastPersistedOffset = -1;
    private volatile long adaptiveDelay;

    public HttpPipeConnector(
            @Client("/") HttpClient httpClient,
            @Value("${broker.storage.data-dir:./data}") String dataDir,
            @Value("${broker.pipe.min-poll-interval-ms:500}") long minPollIntervalMs,
            @Value("${broker.pipe.max-poll-interval-ms:20000}") long maxPollIntervalMs,
            @Value("${broker.pipe.poll-limit:5}") int pollLimit,
            PipeMetrics metrics) throws StorageException {
        this.httpClient = httpClient;
        this.streamingHttpClient = java.net.http.HttpClient.newBuilder()
                .connectTimeout(java.time.Duration.ofSeconds(10))
                .build();

        this.objectMapper = new ObjectMapper();
        this.objectMapper.findAndRegisterModules();
        this.metrics = metrics;
        this.minPollIntervalMs = Math.max(100, minPollIntervalMs);
        this.maxPollIntervalMs = Math.max(this.minPollIntervalMs, maxPollIntervalMs);
        this.pollLimit = Math.max(1, pollLimit);
        this.adaptiveDelay = this.minPollIntervalMs;

        this.scheduler = Executors.newSingleThreadScheduledExecutor(r -> {
            Thread t = new Thread(r, "HttpPipeConnector");
            return t;
        });

        try {
            Files.createDirectories(Paths.get(dataDir));
        } catch (IOException e) {
            // Fatal error during initialization
            log.error("Failed to create data directory: {}", dataDir, e);
            throw new StorageException(ErrorCode.STORAGE_IO_ERROR,
                "Failed to create data directory: " + dataDir, e);
        }

        this.offsetFilePath = Paths.get(dataDir, OFFSET_FILE);
        loadOffset();

        log.info("event=pipe_connector.initialized offset={} minPollIntervalMs={} maxPollIntervalMs={} pollLimit={}",
                currentOffset, this.minPollIntervalMs, this.maxPollIntervalMs, this.pollLimit);
    }

    @Override
    public CompletableFuture<PipeConnection> connectToParent(String parentUrl) {
        return CompletableFuture.supplyAsync(() -> {
            this.connection = new PipeConnectionImpl(parentUrl);
            this.running = true;
            this.adaptiveDelay = minPollIntervalMs;

            scheduler.execute(this::pollLoop);

            // NOTE: Offset persistence now happens immediately after each successful batch
            // in streamAndHandle() - no separate periodic task needed

            log.info("event=pipe_connector.connected parentUrl={}", parentUrl);
            return connection;
        });
    }

    /**
     * Single-thread polling loop (no task buildup)
     */
    private void pollLoop() {
        while (running) {
            // Check if paused (for DataRefresh)
            if (pausePipeCalls) {
                try {
                    Thread.sleep(1000);  // Sleep while paused
                    continue;
                } catch (InterruptedException ignored) {
                    Thread.currentThread().interrupt();
                    return;
                }
            }

            long start = System.currentTimeMillis();
            try {
                int received = pollParent();
                long duration = System.currentTimeMillis() - start;

                if (received > 0) {
                    adaptiveDelay = Math.max(minPollIntervalMs, duration / 2);
                } else {
                    adaptiveDelay = Math.min(adaptiveDelay * 2, maxPollIntervalMs);
                }
            } catch (Exception e) {
                log.error("Polling error", e);
                adaptiveDelay = Math.min(adaptiveDelay * 3, maxPollIntervalMs);
            }

            try {
                Thread.sleep(adaptiveDelay);
            } catch (InterruptedException ignored) {
                Thread.currentThread().interrupt();
                return;
            }
        }
    }

    @Override
    public void onDataReceived(Function<MessageRecord, Boolean> handler) {
        this.dataHandler = handler;
    }

    @Override
    public CompletableFuture<Void> sendAck(long offset) {
        return CompletableFuture.completedFuture(null);
    }

    @Override
    public PipeHealth getHealth() {
        if (connection == null || !connection.connected) {
            return PipeHealth.UNHEALTHY;
        }

        long sinceLastMsg = System.currentTimeMillis() - connection.lastMessageTime;
        return sinceLastMsg > 60_000 ? PipeHealth.DEGRADED : PipeHealth.HEALTHY;
    }

    @Override
    public void reconnect() {
        if (connection != null) {
            disconnect();
            connectToParent(connection.parentUrl);
        }
    }

    /**
     * Pause pipe calls (for DataRefresh workflow)
     */
    public void pausePipeCalls() {
        this.pausePipeCalls = true;
        log.info("event=pipe_connector.paused reason=data_refresh");
    }

    /**
     * Resume pipe calls (after DataRefresh completes)
     */
    public void resumePipeCalls() {
        this.pausePipeCalls = false;
        log.info("event=pipe_connector.resumed reason=data_refresh_complete");
    }

    @Override
    public void disconnect() {
        running = false;
        persistOffset();
        scheduler.shutdownNow();
        if (connection != null) {
            connection.connected = false;
        }
    }

    /**
     * Poll parent broker using a streaming InputStream — no byte[] buffer.
     * The previous implementation used httpClient.toBlocking().exchange(request, byte[].class)
     * which materialized the full response body as a byte[] before parsing. With message
     * payloads from the 4.3 GB SQLite database, each poll response was 1-2 MB, creating a
     * humongous G1 object every 500 ms and filling the heap in under 7 minutes (OOM exit 3).
     * java.net.http.HttpClient.BodyHandlers.ofInputStream() streams directly into the JSON
     * parser with no intermediate heap copy.
     */
    private int pollParent() throws IOException {
        if (!running || connection == null) return 0;

        Timer.Sample sample = metrics.startFetchTimer();

        try {
            String parentUrl = normalizeUrl(connection.parentUrl);
            String pollUrl = parentUrl + "/pipe/poll?offset=" + currentOffset + "&limit=" + pollLimit;

            HttpRequest request = HttpRequest.newBuilder()
                    .uri(URI.create(pollUrl))
                    .timeout(java.time.Duration.ofSeconds(30))
                    .GET()
                    .build();

            HttpResponse<InputStream> response;
            try {
                response = streamingHttpClient.send(request, HttpResponse.BodyHandlers.ofInputStream());
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                return 0;
            }

            metrics.recordFetchLatency(sample);

            int statusCode = response.statusCode();
            if (statusCode == 200) {
                try (InputStream is = response.body()) {
                    int count = streamAndHandle(is);
                    if (count == 0) metrics.recordEmptyFetch();
                    return count;
                }
            }

            // Drain non-200 body to allow TCP connection reuse
            try (InputStream is = response.body()) {
                is.transferTo(OutputStream.nullOutputStream());
            }

            if (statusCode == 204) {
                metrics.recordEmptyFetch();
            } else {
                log.warn("event=pipe_connector.poll_failed status={}", statusCode);
                metrics.recordFetchError();
            }
            return 0;

        } catch (IOException e) {
            metrics.recordFetchError();
            throw e;
        }
    }

    /**
     * Stream JSON array → MessageRecord (constant memory)
     *
     * TRANSACTIONAL PROCESSING:
     * - Only advances offset for successfully stored messages
     * - Stops batch processing on first storage failure
     * - Persists offset file after successful batch
     * - On failure, next poll retries from last successful offset (no data loss)
     */
    private int streamAndHandle(InputStream is) throws IOException {
        int count = 0;
        long lastSuccessfulOffset = currentOffset;  // Track last successful offset

        JsonParser parser = objectMapper.createParser(is);

        if (parser.nextToken() != JsonToken.START_ARRAY) {
            return 0;
        }

        while (parser.nextToken() == JsonToken.START_OBJECT) {
            MessageRecord record =
                    objectMapper.readValue(parser, MessageRecord.class);

            if (dataHandler != null) {
                // Attempt to store message - handler returns true on success, false on failure
                boolean success = dataHandler.apply(record);

                if (success) {
                    // SUCCESS - update last successful offset
                    lastSuccessfulOffset = record.getOffset();
                    count++;

                    log.trace("Successfully stored pipe message at offset {}", record.getOffset());
                } else {
                    // STORAGE FAILURE - stop processing batch, keep old offset
                    log.error("CRITICAL: Failed to store pipe message at offset {}, " +
                             "stopping batch. Next poll will retry from offset {}",
                             record.getOffset(), lastSuccessfulOffset);

                    metrics.recordFetchError();

                    // Stop processing this batch - remaining messages will be retried on next poll
                    break;
                }
            }
        }

        // Update current offset only to last successfully stored message
        currentOffset = lastSuccessfulOffset;
        connection.lastReceivedOffset = lastSuccessfulOffset;
        connection.lastMessageTime = System.currentTimeMillis();

        // Persist offset file ONLY if we successfully stored messages
        if (count > 0) {
            persistOffset();
            log.debug("Persisted pipe offset after successful batch: offset={}, messagesStored={}",
                     currentOffset, count);
        }

        // Record metrics: messages received from pipe
        if (count > 0) {
            metrics.recordMessagesReceived(count);
        }

        return count;
    }

    private static String normalizeUrl(String url) {
        if (url.startsWith("http://") || url.startsWith("https://")) {
            return url;
        }
        return "http://" + url;
    }

    private void loadOffset() {
        if (!Files.exists(offsetFilePath)) return;

        try (InputStream in = Files.newInputStream(offsetFilePath)) {
            Properties props = new Properties();
            props.load(in);
            currentOffset = Long.parseLong(
                    props.getProperty("pipe.current.offset", "0"));
            lastPersistedOffset = currentOffset;
        } catch (Exception e) {
            log.warn("event=pipe_connector.offset_load_failed action=start_at_zero", e);
            currentOffset = 0;
        }
    }

    private synchronized void persistOffset() {
        if (currentOffset == lastPersistedOffset) return;

        try {
            Properties props = new Properties();
            props.setProperty("pipe.current.offset", String.valueOf(currentOffset));

            Path tmp = offsetFilePath.resolveSibling(OFFSET_FILE + ".tmp");
            try (OutputStream os = Files.newOutputStream(tmp)) {
                props.store(os, "Pipe offset");
            }

            Files.move(tmp, offsetFilePath, StandardCopyOption.REPLACE_EXISTING);
            lastPersistedOffset = currentOffset;
        } catch (Exception e) {
            log.error("Failed to persist offset", e);
        }
    }

    /**
     * PipeConnection implementation
     */
    private static class PipeConnectionImpl implements PipeConnection {
        private final String parentUrl;
        private volatile boolean connected = true;
        private volatile long lastReceivedOffset;
        private volatile long lastMessageTime = System.currentTimeMillis();

        PipeConnectionImpl(String parentUrl) {
            this.parentUrl = parentUrl;
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
