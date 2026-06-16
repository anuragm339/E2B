package com.messaging.broker.core;

import com.messaging.broker.ack.AckStoreSeeder;
import com.messaging.broker.compaction.CompactionIndex;
import com.messaging.broker.handler.DisconnectHandler;
import com.messaging.broker.handler.MessageHandler;
import com.messaging.broker.handler.MessageHandlerRegistry;
import com.messaging.broker.consumer.AdaptiveBatchDeliveryManager;
import com.messaging.broker.consumer.ConsumerDeliveryManager;
import com.messaging.broker.monitoring.BrokerMetrics;
import com.messaging.broker.monitoring.TraceIds;
import com.messaging.broker.core.TopologyManager;
import com.messaging.common.api.NetworkServer;
import com.messaging.common.api.StorageEngine;
import com.messaging.common.exception.ErrorCode;
import com.messaging.common.exception.ExceptionLogger;
import com.messaging.common.exception.NetworkException;
import com.messaging.common.exception.StorageException;
import com.messaging.common.model.BrokerMessage;
import com.messaging.common.model.MessageRecord;
import io.micrometer.core.instrument.Timer;
import io.micronaut.context.annotation.Value;
import io.micronaut.context.event.ApplicationEventListener;
import io.micronaut.runtime.server.event.ServerStartupEvent;
import jakarta.annotation.PreDestroy;
import jakarta.inject.Inject;
import jakarta.inject.Singleton;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Main broker service that wires storage and network together
 */
@Singleton
public class BrokerService implements ApplicationEventListener<ServerStartupEvent> {
    private static final Logger log = LoggerFactory.getLogger(BrokerService.class);

    private final StorageEngine storage;
    private final NetworkServer server;
    private final ConsumerDeliveryManager consumerDelivery;
    private final AdaptiveBatchDeliveryManager adaptiveDeliveryManager;
    private final TopologyManager topologyManager;
    private final BrokerMetrics metrics;
    private final MessageHandlerRegistry handlerRegistry;
    private final DisconnectHandler disconnectHandler;
    private final ShutdownCoordinator shutdownCoordinator;
    private final AckStoreSeeder ackStoreSeeder;
    private final CompactionIndex compactionIndex;
    private final int serverPort;
    private final boolean ackStoreSeedOnStartupEnabled;

    @Inject
    public BrokerService(
            StorageEngine storage,
            NetworkServer server,
            ConsumerDeliveryManager consumerDelivery,
            AdaptiveBatchDeliveryManager adaptiveDeliveryManager,
            TopologyManager topologyManager,
            BrokerMetrics metrics,
            MessageHandlerRegistry handlerRegistry,
            DisconnectHandler disconnectHandler,
            ShutdownCoordinator shutdownCoordinator,
            AckStoreSeeder ackStoreSeeder,
            CompactionIndex compactionIndex,
            @Value("${broker.network.port:9092}") int serverPort,
            @Value("${ack-store.seed-on-startup.enabled:true}") boolean ackStoreSeedOnStartupEnabled) {

        this.storage = storage;
        this.server = server;
        this.consumerDelivery = consumerDelivery;
        this.adaptiveDeliveryManager = adaptiveDeliveryManager;
        this.topologyManager = topologyManager;
        this.metrics = metrics;
        this.handlerRegistry = handlerRegistry;
        this.disconnectHandler = disconnectHandler;
        this.shutdownCoordinator = shutdownCoordinator;
        this.ackStoreSeeder = ackStoreSeeder;
        this.compactionIndex = compactionIndex;
        this.serverPort = serverPort;
        this.ackStoreSeedOnStartupEnabled = ackStoreSeedOnStartupEnabled;

        log.info("BrokerService initialized with handler registry");
    }

    @Override
    public void onApplicationEvent(ServerStartupEvent event) {
        log.info("Application started, initializing broker...");

        // Recover storage
        try {
            storage.recover();
            log.info("Storage recovered");
        } catch (StorageException e) {
            // MessagingException is unchecked, so it propagates straight out of this
            // ApplicationEventListener callback and fails application startup as intended.
            throw ExceptionLogger.logAndThrow(log, e);
        } catch (Exception e) {
            throw ExceptionLogger.logAndThrow(log, new StorageException(ErrorCode.STORAGE_RECOVERY_FAILED,
                "Storage recovery failed during broker initialization", e));
        }

        // Backfill RocksDB ACK entries for records consumed before ACK tracking existed.
        // Must run after storage.recover() so segment data is available.
        // Runs before reconciler's first scheduled execution (2-min initial delay).
        if (ackStoreSeedOnStartupEnabled) {
            try {
                ackStoreSeeder.seed();
            } catch (Exception e) {
                // Degrade, don't brick: a transient RocksDB problem (disk full, leftover lock)
                // must not make a store device unbootable. The reconciliation scheduler
                // detects and reports any ACK gaps the failed seeding would have closed.
                log.error("ACK store seeding failed — continuing startup with possibly " +
                        "incomplete ACK state; reconciliation will surface any gaps", e);
            }
        } else {
            log.info("AckStoreSeeder skipped at startup because ack-store.seed-on-startup.enabled=false");
        }

        // Register message handler
        server.registerHandler(this::handleMessage);

        // Register disconnect handler
        server.registerDisconnectHandler(disconnectHandler::handleDisconnect);

        // Start network server
        try {
            server.start(serverPort);
            log.info("Broker ready on port {}", serverPort);
        } catch (NetworkException e) {
            // Unchecked MessagingException propagates out of startup and fails the boot.
            throw ExceptionLogger.logAndThrow(log, e);
        }

        // Start consumer delivery
        consumerDelivery.startDelivery();
        log.info("Consumer delivery started");

        // Start adaptive batch delivery manager (watermark-based polling)
        adaptiveDeliveryManager.start();
        log.info("Adaptive batch delivery manager started");

        // Start topology manager - it will query Cloud Registry and connect to parent
        topologyManager.onMessageReceived(this::handlePipeMessage);
        topologyManager.start();
        log.info("Topology manager started");
    }

    /**
     * Handle messages received from parent via Pipe
     *
     * TRANSACTIONAL SEMANTICS:
     * Returns true if message was successfully stored, false otherwise.
     * HttpPipeConnector uses the return value to decide whether to advance the offset.
     * This prevents data loss on restart after storage failures.
     *
     * @param record Message record from parent broker
     * @return true if message was successfully stored, false on any failure
     */
    private boolean handlePipeMessage(MessageRecord record) {
        Timer.Sample e2eSample = metrics.startE2ETimer();

        try {
            log.debug("Received message from parent: key={}, type={}",
                    record.getMsgKey(), record.getEventType());

            // Calculate message size (estimate: key + data + metadata)
            long messageBytes = (record.getMsgKey() != null ? record.getMsgKey().length() : 0) +
                                (record.getData() != null ? record.getData().length() : 0) +
                                50; // metadata overhead estimate

            metrics.recordMessageReceived(messageBytes);

            // Store message in local storage
            // Use topic from record, fallback to default if not set
            String topic = record.getTopic();

            // Duplicate guard: parent offsets are monotonically increasing per topic, so a
            // record at or below the topic's storage head was already stored — the parent
            // re-sent it (poll retry after a mid-stream failure). Re-appending it would create
            // duplicate physical records with the same offset and break the sorted-index
            // invariant. Treat as success so the pipe offset advances past it.
            long topicHead = storage.getCurrentOffset(topic, 0);
            boolean alreadyStored = record.getOffset() > 0 && record.getOffset() <= topicHead;
            // P0 #2: offset 0 is BOTH the producer "unset" sentinel and a legitimate first parent
            // offset, so the > 0 guard never skips it. A broker parent (PipeServer) reads inclusively
            // and re-serves its offset-0 record on every poll; SegmentManager then renumbers it to a
            // fresh, ever-growing offset -> unbounded storage growth. Detect the re-delivery by key:
            // an already-indexed key means the offset-0 record was already stored, so skip it. The
            // genuine first delivery has no index entry and proceeds normally below.
            if (!alreadyStored && record.getOffset() == 0
                    && compactionIndex.getLatestOffsetAndTimestamp(topic, record.getMsgKey()) != null) {
                alreadyStored = true;
            }
            if (alreadyStored) {
                // Heal the index even when skipping the storage write: a crash (or lost
                // RocksDB WAL tail on power cut) between append and updateKey leaves the
                // record in segments but not in the index — the parent's re-send is the
                // ONE chance to repair that gap. updateKey is idempotent and monotone, so
                // for genuine duplicates this is a no-op.
                compactionIndex.updateKey(topic, record.getMsgKey(), record.getOffset(),
                        record.getCreatedAt().toEpochMilli());
                log.info("event=pipe_message.duplicate_skipped topic={} offset={} storageHead={}",
                        topic, record.getOffset(), topicHead);
                metrics.stopE2ETimer(e2eSample);
                return true;
            }

            Timer.Sample storageSample = metrics.startStorageWriteTimer();
            long offset = storage.append(topic, 0, record);
            metrics.stopStorageWriteTimer(storageSample);

            metrics.recordMessageStored(topic);
            metrics.recordTopicLastMessageTime(topic);
            metrics.updateTopicHeadOffset(topic, offset);

            compactionIndex.updateKey(topic, record.getMsgKey(), offset,
                    record.getCreatedAt().toEpochMilli());

            log.debug("Stored message from parent: topic={}, offset={}, key={}",
                    topic, offset, record.getMsgKey());

            // Broker will discover new messages via adaptive polling (watermark-based)
            // No notification needed - Pipe is now fully decoupled from Broker

            metrics.stopE2ETimer(e2eSample);

            return true;  // Success

        } catch (Exception e) {
            // Log the error but return false instead of throwing
            log.error("CRITICAL: Failed to store pipe message at offset {}: {}",
                     record.getOffset(), e.getMessage(), e);
            return false;  // Failure - caller will not advance offset
        }
    }

    /**
     * Handle incoming messages from clients using handler registry.
     */
    private void handleMessage(String clientId, BrokerMessage message) {
        String traceId = TraceIds.newTraceId();
        log.debug("Handling message from {}: type={}, id={}, traceId={}",
                  clientId, message.getType(), message.getMessageId(), traceId);

        BrokerMessage.MessageType messageType = message.getType();

        // Legacy compatibility: Legacy clients send READY instead of READY_ACK
        if (messageType == BrokerMessage.MessageType.READY) {
            log.debug("Legacy client {} sent READY (expected READY_ACK) - treating as READY_ACK", clientId);
            messageType = BrokerMessage.MessageType.READY_ACK;
        }

        // Look up handler from registry
        MessageHandler handler = handlerRegistry.getHandler(messageType);

        if (handler != null) {
            handler.handle(clientId, message, traceId);
        } else {
            log.warn("No handler registered for message type: {}, traceId={}", messageType, traceId);
        }
    }


    @PreDestroy
    public void shutdown() {
        log.info("Shutting down broker...");

        // Stop network ingress before draining executors so no new ACK tasks are accepted.
        server.shutdown();

        // Stop adaptive delivery manager
        adaptiveDeliveryManager.stop();

        // Stop topology manager (will disconnect from parent)
        topologyManager.shutdown();

        // Stop consumer delivery
        consumerDelivery.shutdown();

        // Drain the centrally owned executor beans after all producers have stopped.
        shutdownCoordinator.shutdown();

        try {
            storage.close();
        } catch (Exception e) {
            log.error("Error closing storage during shutdown", e);
        }

        log.info("Broker shutdown complete");
    }
}
