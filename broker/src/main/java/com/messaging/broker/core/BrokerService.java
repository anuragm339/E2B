package com.messaging.broker.core;

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
import jakarta.inject.Named;
import jakarta.inject.Singleton;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;

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
    private final ExecutorService ackExecutor;
    private final int serverPort;

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
            @Named("ackExecutor") ExecutorService ackExecutor,
            @Value("${broker.network.port:9092}") int serverPort) {

        this.storage = storage;
        this.server = server;
        this.consumerDelivery = consumerDelivery;
        this.adaptiveDeliveryManager = adaptiveDeliveryManager;
        this.topologyManager = topologyManager;
        this.metrics = metrics;
        this.handlerRegistry = handlerRegistry;
        this.disconnectHandler = disconnectHandler;
        this.ackExecutor = ackExecutor;
        this.serverPort = serverPort;

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
            // Framework limitation: ApplicationEventListener cannot throw checked exceptions
            // Wrap in RuntimeException to propagate and fail application startup
            throw new RuntimeException("Storage recovery failed - see cause for details", e);
        } catch (Exception e) {
            log.error("Failed to recover storage", e);
            StorageException storageEx = new StorageException(ErrorCode.STORAGE_RECOVERY_FAILED,
                "Storage recovery failed during broker initialization", e);
            // Framework limitation: Wrap in RuntimeException to fail application startup
            throw new RuntimeException("Storage recovery failed - see cause for details", storageEx);
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
            // Framework limitation: Wrap in RuntimeException to fail application startup
            throw new RuntimeException("Network server startup failed - see cause for details", e);
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

            Timer.Sample storageSample = metrics.startStorageWriteTimer();
            long offset = storage.append(topic, 0, record);
            metrics.stopStorageWriteTimer(storageSample);

            metrics.recordMessageStored();
            metrics.recordTopicLastMessageTime(topic);

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

        // Shutdown ACK executor first to stop accepting new ACKs
        log.info("Shutting down ACK executor...");
        ackExecutor.shutdown();
        try {
            if (!ackExecutor.awaitTermination(30, TimeUnit.SECONDS)) {
                log.warn("ACK executor did not terminate in time, forcing shutdown");
                ackExecutor.shutdownNow();
            }
        } catch (InterruptedException e) {
            log.warn("Interrupted while waiting for ACK executor shutdown", e);
            ackExecutor.shutdownNow();
            Thread.currentThread().interrupt();
        }

        // Stop adaptive delivery manager
        adaptiveDeliveryManager.stop();

        // Stop topology manager (will disconnect from parent)
        topologyManager.shutdown();

        // Stop consumer delivery
        consumerDelivery.shutdown();

        // Shutdown server and storage
        server.shutdown();
        try {
            storage.close();
        } catch (Exception e) {
            log.error("Error closing storage during shutdown", e);
        }

        log.info("Broker shutdown complete");
    }
}
