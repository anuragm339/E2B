package com.messaging.pipe;

import com.messaging.common.api.PipeConnector;
import com.messaging.common.model.MessageRecord;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonToken;
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
import java.nio.file.*;
import java.time.Duration;
import java.util.Properties;
import java.util.concurrent.*;
import java.util.function.Consumer;

/**
 * HTTP-based PipeConnector with streaming JSON parsing
 * Memory-safe and Docker-friendly
 */
@Singleton
public class HttpPipeConnector implements PipeConnector {

    private static final Logger log = LoggerFactory.getLogger(HttpPipeConnector.class);

    private static final long MIN_POLL_INTERVAL_MS = 200;  // Reduced polling frequency (was 100ms)
    private static final long MAX_POLL_INTERVAL_MS = 10000;  // Longer backoff when idle (was 5000ms)
    private static final long PERSIST_INTERVAL_MS = 5000;
    private static final String OFFSET_FILE = "pipe-offset.properties";

    private final HttpClient httpClient;
    private final ObjectMapper objectMapper;
    private final ScheduledExecutorService scheduler;
    private final Path offsetFilePath;

    private volatile PipeConnectionImpl connection;
    private volatile Consumer<MessageRecord> dataHandler;
    private volatile boolean running;

    private volatile long currentOffset = 0;
    private volatile long lastPersistedOffset = -1;
    private volatile long adaptiveDelay = MIN_POLL_INTERVAL_MS;

    public HttpPipeConnector(
            @Value("${broker.storage.data-dir:./data}") String dataDir) {

        this.httpClient = HttpClient.newBuilder()
                .connectTimeout(Duration.ofSeconds(10))
                .build();

        this.objectMapper = new ObjectMapper();
        this.objectMapper.findAndRegisterModules();

        this.scheduler = Executors.newSingleThreadScheduledExecutor(r -> {
            Thread t = new Thread(r, "HttpPipeConnector");
            return t;
        });

        try {
            Files.createDirectories(Paths.get(dataDir));
        } catch (IOException e) {
            throw new IllegalStateException("Failed to create data dir", e);
        }

        this.offsetFilePath = Paths.get(dataDir, OFFSET_FILE);
        loadOffset();

        log.info("HttpPipeConnector initialized, offset={}", currentOffset);
    }

    @Override
    public CompletableFuture<PipeConnection> connectToParent(String parentUrl) {
        return CompletableFuture.supplyAsync(() -> {
            this.connection = new PipeConnectionImpl(parentUrl);
            this.running = true;

            scheduler.execute(this::pollLoop);

            scheduler.scheduleWithFixedDelay(
                    this::persistOffset,
                    PERSIST_INTERVAL_MS,
                    PERSIST_INTERVAL_MS,
                    TimeUnit.MILLISECONDS
            );

            log.info("Connected to parent {}", parentUrl);
            return connection;
        });
    }

    /**
     * Single-thread polling loop (no task buildup)
     */
    private void pollLoop() {
        while (running) {
            long start = System.currentTimeMillis();
            try {
                int received = pollParent();
                long duration = System.currentTimeMillis() - start;

                if (received > 0) {
                    adaptiveDelay = Math.max(MIN_POLL_INTERVAL_MS, duration / 2);
                } else {
                    adaptiveDelay = Math.min(adaptiveDelay * 2, MAX_POLL_INTERVAL_MS);
                }
            } catch (Exception e) {
                log.error("Polling error", e);
                adaptiveDelay = Math.min(adaptiveDelay * 3, MAX_POLL_INTERVAL_MS);
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
    public void onDataReceived(Consumer<MessageRecord> handler) {
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
     * Poll parent broker using streaming InputStream
     */
    private int pollParent() throws IOException, InterruptedException {
        if (!running || connection == null) return 0;

        String parentUrl = normalizeUrl(connection.parentUrl);
        String pollUrl = parentUrl + "/pipe/poll?offset=" + currentOffset + "&limit=5";  // Reduced from 10 to 5

        HttpRequest request = HttpRequest.newBuilder()
                .uri(URI.create(pollUrl))
                .timeout(Duration.ofSeconds(10))
                .GET()
                .build();

        HttpResponse<InputStream> response =
                httpClient.send(request, HttpResponse.BodyHandlers.ofInputStream());

        if (response.statusCode() == 200) {
            try (InputStream is = response.body()) {
                return streamAndHandle(is);
            }
        }

        if (response.statusCode() != 204) {
            log.warn("Poll failed: {}", response.statusCode());
        }

        return 0;
    }

    /**
     * Stream JSON array → MessageRecord (constant memory)
     */
    private int streamAndHandle(InputStream is) throws IOException {
        int count = 0;

        JsonParser parser = objectMapper.createParser(is);

        if (parser.nextToken() != JsonToken.START_ARRAY) {
            return 0;
        }

        while (parser.nextToken() == JsonToken.START_OBJECT) {
            MessageRecord record =
                    objectMapper.readValue(parser, MessageRecord.class);

            if (dataHandler != null) {
                dataHandler.accept(record);
            }

            currentOffset = record.getOffset();
            connection.lastReceivedOffset = record.getOffset();
            connection.lastMessageTime = System.currentTimeMillis();
            count++;
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
            log.warn("Failed to load offset, starting at 0", e);
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
