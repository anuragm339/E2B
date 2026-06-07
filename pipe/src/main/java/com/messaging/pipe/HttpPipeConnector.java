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
import io.micronaut.core.io.buffer.ByteBuffer;
import io.micronaut.http.HttpRequest;
import io.micronaut.http.client.StreamingHttpClient;
import io.micronaut.http.client.annotation.Client;
import io.micronaut.http.client.exceptions.HttpClientResponseException;
import io.micrometer.core.instrument.Timer;
import io.micronaut.context.annotation.Value;
import io.netty.util.ReferenceCounted;
import jakarta.annotation.PreDestroy;
import jakarta.inject.Singleton;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.core.publisher.Flux;

import java.io.*;
import java.nio.file.*;
import java.time.Duration;
import java.util.Enumeration;
import java.util.Iterator;
import java.util.Properties;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Function;

/**
 * HTTP-based PipeConnector with streaming JSON parsing
 * Memory-safe and Docker-friendly
 */
@Singleton
public class HttpPipeConnector implements PipeConnector {

    private static final Logger log = LoggerFactory.getLogger(HttpPipeConnector.class);

    private static final String OFFSET_FILE = "pipe-offset.properties";

    private final StreamingHttpClient streamingHttpClient;
    private final ObjectMapper objectMapper;
    private final ScheduledExecutorService scheduler;
    private final Path offsetFilePath;
    private final PipeMetrics metrics;
    private final long minPollIntervalMs;
    private final long maxPollIntervalMs;
    private final int pollLimit;
    private final Object lifecycleLock = new Object();
    private final AtomicLong connectionGeneration = new AtomicLong();

    private volatile PipeConnectionImpl connection;
    private volatile Function<MessageRecord, Boolean> dataHandler;
    private volatile boolean running;
    private volatile boolean destroyed;
    private volatile Future<?> pollTask;
    private volatile boolean pausePipeCalls = false;  // For DataRefresh support

    private volatile long currentOffset = 0;
    private volatile long lastPersistedOffset = -1;
    private volatile long adaptiveDelay;

    public HttpPipeConnector(
            @Client("/") StreamingHttpClient streamingHttpClient,
            @Value("${broker.storage.data-dir:./data}") String dataDir,
            @Value("${broker.pipe.min-poll-interval-ms:500}") long minPollIntervalMs,
            @Value("${broker.pipe.max-poll-interval-ms:20000}") long maxPollIntervalMs,
            @Value("${broker.pipe.poll-limit:5}") int pollLimit,
            PipeMetrics metrics) throws StorageException {
        this.streamingHttpClient = streamingHttpClient;

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
        synchronized (lifecycleLock) {
            if (destroyed) {
                return CompletableFuture.failedFuture(
                        new IllegalStateException("Pipe connector has been destroyed"));
            }

            stopPollingLocked();

            long generation = connectionGeneration.incrementAndGet();
            PipeConnectionImpl newConnection = new PipeConnectionImpl(parentUrl);
            connection = newConnection;
            running = true;
            adaptiveDelay = minPollIntervalMs;

            try {
                pollTask = scheduler.submit(() -> pollLoop(generation, newConnection));
            } catch (RejectedExecutionException e) {
                running = false;
                newConnection.connected = false;
                return CompletableFuture.failedFuture(e);
            }

            log.info("event=pipe_connector.connected parentUrl={} generation={}", parentUrl, generation);
            return CompletableFuture.completedFuture(newConnection);
        }
    }

    /**
     * The next offset this connector will request via /pipe/poll. Used by PipeConsistency
     * lineage tracking to record the boundary where a new parent started producing.
     */
    @Override
    public long getCurrentOffset() {
        return currentOffset;
    }

    /**
     * Single-thread polling loop (no task buildup)
     */
    private void pollLoop(long generation, PipeConnectionImpl expectedConnection) {
        while (isActive(generation, expectedConnection)) {
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
                int received = pollParent(generation, expectedConnection);
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
        String parentUrl;
        synchronized (lifecycleLock) {
            parentUrl = connection != null ? connection.parentUrl : null;
        }
        if (parentUrl != null) {
            disconnect();
            connectToParent(parentUrl).whenComplete((ignored, failure) -> {
                if (failure != null) {
                    log.error("event=pipe_connector.reconnect_failed parentUrl={}", parentUrl, failure);
                }
            });
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
        synchronized (lifecycleLock) {
            connectionGeneration.incrementAndGet();
            stopPollingLocked();
        }
        persistOffset();
    }

    @PreDestroy
    public void destroy() {
        synchronized (lifecycleLock) {
            if (destroyed) {
                return;
            }
            destroyed = true;
            connectionGeneration.incrementAndGet();
            stopPollingLocked();
            scheduler.shutdownNow();
        }
        persistOffset();
        try {
            if (!scheduler.awaitTermination(5, TimeUnit.SECONDS)) {
                log.warn("event=pipe_connector.scheduler_shutdown_timeout");
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }

    private void stopPollingLocked() {
        running = false;
        Future<?> task = pollTask;
        pollTask = null;
        if (task != null) {
            task.cancel(true);
        }
        PipeConnectionImpl currentConnection = connection;
        if (currentConnection != null) {
            currentConnection.connected = false;
        }
    }

    private boolean isActive(long generation, PipeConnectionImpl expectedConnection) {
        return running
                && connectionGeneration.get() == generation
                && connection == expectedConnection
                && expectedConnection.connected;
    }

    /**
     * Poll parent broker using a streaming body — no byte[] buffer.
     *
     * <p>Uses Micronaut {@link StreamingHttpClient#dataStream(HttpRequest)} which emits
     * {@link ByteBuffer} chunks as they arrive on the wire. The chunks are bridged into a
     * {@link SequenceInputStream} so the existing Jackson streaming parser consumes the body
     * incrementally. Materialising the full response body (e.g. via {@code toBlocking().exchange(_, byte[].class)})
     * regressed previously: 1-2 MB poll responses created humongous G1 objects every 500 ms
     * and filled the heap in under 7 minutes (OOM exit 3). The streaming path here preserves
     * the original constant-memory property while removing the second HTTP stack.
     *
     * <p>{@code .timeout(30s)} on the Reactor pipeline replaces the per-request timeout that
     * used to live on the {@code java.net.http} request builder: it fires if no chunk arrives
     * for 30 s. Initial connect/response-headers timeout is governed by the Micronaut HTTP
     * client config ({@code micronaut.http.client.read-timeout} / {@code connect-timeout}).
     */
    private int pollParent(long generation, PipeConnectionImpl expectedConnection) throws IOException {
        if (!isActive(generation, expectedConnection)) return 0;

        Timer.Sample sample = metrics.startFetchTimer();
        String parentUrl = normalizeUrl(expectedConnection.parentUrl);
        String pollUrl = parentUrl + "/pipe/poll?offset=" + currentOffset + "&limit=" + pollLimit;
        HttpRequest<?> request = HttpRequest.GET(pollUrl);

        try {
            // CRITICAL: copy each chunk to a heap byte[] **on the Netty event-loop thread**
            // (synchronously inside .map()), BEFORE Micronaut's dataStream releases the underlying
            // pooled ByteBuf when onNext returns. Doing the copy later, from the polling thread
            // inside the iterator, reads pool memory that has already been reclaimed and zeroed by
            // a subsequent request — visible to Jackson as
            //   "Illegal unquoted character (CTRL-CHAR, code 0)"
            // somewhere deep inside a JSON string value. .map() runs synchronously in onNext, so
            // the bytes are safely materialised on the heap before the slot is reused.
            Iterator<byte[]> chunks = Flux.from(streamingHttpClient.dataStream(request))
                    .timeout(Duration.ofSeconds(30))
                    .map(HttpPipeConnector::copyAndRelease)
                    .toIterable()
                    .iterator();

            try (InputStream is = new SequenceInputStream(toInputStreamEnumeration(chunks))) {
                if (!isActive(generation, expectedConnection)) {
                    return 0;
                }
                int count = streamAndHandle(is, generation, expectedConnection);
                if (count == 0) metrics.recordEmptyFetch();
                return count;
            }
        } catch (RuntimeException re) {
            HttpClientResponseException responseEx = unwrapResponseException(re);
            if (responseEx != null) {
                int statusCode = responseEx.getStatus().getCode();
                log.warn("event=pipe_connector.poll_failed status={}", statusCode);
                metrics.recordFetchError();
                return 0;
            }
            metrics.recordFetchError();
            throw new IOException("Pipe poll failed", re);
        } catch (IOException e) {
            metrics.recordFetchError();
            throw e;
        } finally {
            metrics.recordFetchLatency(sample);
        }
    }

    /**
     * Copy a chunk from Micronaut's pooled Netty buffer into a heap {@code byte[]} and release
     * the underlying buffer back to the pool. Called synchronously inside Reactor's {@code .map()}
     * so it runs on the Netty event-loop thread — before the framework's auto-release would
     * otherwise reuse the slot for the next request.
     *
     * <p>The explicit release is also necessary to avoid a slow Netty-pool leak: {@code dataStream}
     * hands buffer ownership to the subscriber, so without this call the {@code refCnt} stays at 1
     * for the lifetime of the JVM.
     */
    private static byte[] copyAndRelease(ByteBuffer<?> chunk) {
        try {
            return chunk.toByteArray();
        } finally {
            Object nativeBuffer = chunk.asNativeBuffer();
            if (nativeBuffer instanceof ReferenceCounted refCounted) {
                refCounted.release();
            }
        }
    }

    /**
     * Bridge an {@code Iterator<byte[]>} of pre-copied chunks (see {@link #copyAndRelease}) into
     * the {@link Enumeration} {@link SequenceInputStream} expects. Wrapping each byte array in a
     * {@link ByteArrayInputStream} is constant-memory: only one chunk-sized array is live per
     * iteration step.
     */
    private static Enumeration<InputStream> toInputStreamEnumeration(Iterator<byte[]> chunks) {
        return new Enumeration<InputStream>() {
            @Override
            public boolean hasMoreElements() {
                return chunks.hasNext();
            }

            @Override
            public InputStream nextElement() {
                return new ByteArrayInputStream(chunks.next());
            }
        };
    }

    /**
     * Walk the cause chain to find an {@link HttpClientResponseException} that may have been
     * wrapped by Reactor's blocking iterator. Returns {@code null} if the failure is something
     * else (network I/O, Reactor timeout, etc.).
     */
    private static HttpClientResponseException unwrapResponseException(Throwable t) {
        while (t != null) {
            if (t instanceof HttpClientResponseException hcre) {
                return hcre;
            }
            t = t.getCause();
        }
        return null;
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
    private int streamAndHandle(
            InputStream is,
            long generation,
            PipeConnectionImpl expectedConnection) throws IOException {
        int count = 0;
        long lastSuccessfulOffset = currentOffset;  // Track last successful offset

        JsonParser parser = objectMapper.createParser(is);

        if (parser.nextToken() != JsonToken.START_ARRAY) {
            return 0;
        }

        while (parser.nextToken() == JsonToken.START_OBJECT) {
            if (!isActive(generation, expectedConnection)) {
                break;
            }
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

        if (!isActive(generation, expectedConnection)) {
            return count;
        }

        // Update current offset only to last successfully stored message
        currentOffset = lastSuccessfulOffset;
        expectedConnection.lastReceivedOffset = lastSuccessfulOffset;
        expectedConnection.lastMessageTime = System.currentTimeMillis();

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
