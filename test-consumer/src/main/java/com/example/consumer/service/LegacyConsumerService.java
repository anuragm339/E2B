package com.example.consumer.service;

import com.example.consumer.config.LegacyConfig;
import com.example.consumer.legacy.events.*;
import io.micronaut.context.annotation.Requires;
import io.micronaut.context.annotation.Value;
import io.micronaut.context.event.ApplicationEventListener;
import io.micronaut.runtime.server.event.ServerStartupEvent;
import jakarta.inject.Inject;
import jakarta.inject.Singleton;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Legacy consumer service - only activated when consumer.legacy.enabled=true
 * Bypasses the @Consumer framework and uses direct LegacyBrokerConnection
 */
@Singleton
@Requires(property = "consumer.legacy.enabled", value = "true")
public class LegacyConsumerService implements ApplicationEventListener<ServerStartupEvent> {
    private static final Logger log = LoggerFactory.getLogger(LegacyConsumerService.class);

    private final LegacyConfig legacyConfig;
    private final String brokerHost;
    private final int brokerPort;

    private LegacyBrokerConnection connection;
    private volatile boolean running = false;
    private Thread eventLoopThread;

    @Inject
    public LegacyConsumerService(
            LegacyConfig legacyConfig,
            @Value("${messaging.broker.host}") String brokerHost,
            @Value("${messaging.broker.port}") int brokerPort) {

        this.legacyConfig = legacyConfig;
        this.brokerHost = brokerHost;
        this.brokerPort = brokerPort;

        log.info("🔧 LegacyConsumerService created - will start on ServerStartupEvent");
    }

    @Override
    public void onApplicationEvent(ServerStartupEvent event) {
        log.info("═══════════════════════════════════════════════════");
        log.info("🚀 LEGACY MODE ENABLED - Using Event Protocol");
        log.info("   Service: {}", legacyConfig.getServiceName());
        log.info("   Broker: {}:{}", brokerHost, brokerPort);
        log.info("═══════════════════════════════════════════════════");

        try {
            connect();
        } catch (Exception e) {
            log.error("❌ Failed to start legacy consumer", e);
            throw new RuntimeException("Failed to start legacy consumer", e);
        }
    }

    private void connect() throws Exception {
        connection = new LegacyBrokerConnection(legacyConfig.getServiceName());
        connection.connect(brokerHost, brokerPort);

        running = true;
        eventLoopThread = new Thread(this::runEventLoop, "LegacyConsumer-EventLoop");
        eventLoopThread.setDaemon(false);  // Keep application alive
        eventLoopThread.start();

        log.info("✅ Legacy consumer started successfully");
    }

    private void runEventLoop() {
        log.info("🔄 Event loop started");

        while (running) {
            try {
                Event event = connection.nextEvent();
                log.debug("📨 Received event: {}", event.getType());

                handleEvent(event);

            } catch (Exception e) {
                if (running) {
                    log.error("❌ Error in event loop", e);
                    // Optionally: implement reconnection logic here
                }
                running = false;
            }
        }

        log.info("🛑 Event loop stopped");
    }

    private void handleEvent(Event event) throws Exception {
        boolean shouldAck = false;

        switch (event.getType()) {
            case BATCH:
                shouldAck = handleBatch((BatchEvent) event);
                break;

            case MESSAGE:
                shouldAck = handleMessage((DataMessageEvent) event);
                break;

            case RESET:
                shouldAck = handleReset((ResetEvent) event);
                break;

            case READY:
                shouldAck = handleReady((ReadyEvent) event);
                break;

            case EOF:
                log.info("📪 EOF received - shutting down");
                running = false;
                break;

            default:
                log.warn("⚠️ Unexpected event type: {}", event.getType());
        }

        if (shouldAck) {
            connection.sendAck();
        }
    }

    private boolean handleBatch(BatchEvent batchEvent) throws Exception {
        int size = batchEvent.count();
        log.info("📦 BATCH received: size={}", size);

        for (Event e : batchEvent.getEvents()) {
            if (e.getType() == EventType.MESSAGE) {
                DataMessageEvent msgEvent = (DataMessageEvent) e;
                // Convert to ConsumerRecord for GenericConsumerHandler
                // For now, just process the data
                log.debug("  Message: type={}, key={}",
                        msgEvent.getMessage().getType(),
                        msgEvent.getMessage().getKey());
            } else if (e.getType() == EventType.DELETE) {
                DeleteMessageEvent delEvent = (DeleteMessageEvent) e;
                log.debug("  DELETE: key={}", delEvent.getMessage().getKey());
            }
        }

        return true;  // Send ACK
    }

    private boolean handleMessage(DataMessageEvent event) throws Exception {
        DataMessage msg = event.getMessage();
        log.debug("📨 MESSAGE received: type={}, key={}", msg.getType(), msg.getKey());
        return true;  // Send ACK
    }

    private boolean handleReset(ResetEvent event) throws Exception {
        log.info("🔄 RESET received - clearing local data");
        // Call GenericConsumerHandler.onReset() if needed
        // messageHandler.onReset("all-topics");
        return true;  // Send ACK
    }

    private boolean handleReady(ReadyEvent event) throws Exception {
        log.info("✅ READY received - refresh complete");
        return true;  // Send ACK
    }

    public void shutdown() {
        log.info("🛑 Shutting down legacy consumer...");
        running = false;

        try {
            if (connection != null) {
                connection.close();
            }

            if (eventLoopThread != null) {
                eventLoopThread.join(5000);
            }
        } catch (Exception e) {
            log.error("❌ Error during shutdown", e);
        }

        log.info("✅ Legacy consumer shutdown complete");
    }
}
