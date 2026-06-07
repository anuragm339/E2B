package com.example.consumer.service;

import com.example.consumer.config.LegacyConfig;
import com.example.consumer.legacy.events.*;
import io.micronaut.context.annotation.Requires;
import io.micronaut.context.annotation.Value;
import io.micronaut.context.event.ApplicationEventListener;
import io.micronaut.runtime.server.event.ServerStartupEvent;
import jakarta.annotation.PreDestroy;
import jakarta.inject.Inject;
import jakarta.inject.Singleton;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Legacy consumer service - only activated when consumer.legacy.enabled=true
 * Bypasses the @Consumer framework and uses direct LegacyBrokerConnection
 */
@Singleton
@Requires(property = "consumer.legacy.enabled", value = "true")
public class LegacyConsumerService implements ApplicationEventListener<ServerStartupEvent> {
    private static final Logger log = LoggerFactory.getLogger(LegacyConsumerService.class);
    private static final int INFO_BATCH_SUMMARY_INTERVAL = 100;

    private final LegacyConfig legacyConfig;
    private final String brokerHost;
    private final int brokerPort;

    private LegacyBrokerConnection connection;
    private volatile boolean running = false;
    private Thread eventLoopThread;
    private final AtomicBoolean shutdownStarted = new AtomicBoolean();
    private int batchCount = 0;
    private int messageCount = 0;

    @Inject
    public LegacyConsumerService(
            LegacyConfig legacyConfig,
            @Value("${messaging.broker.host}") String brokerHost,
            @Value("${messaging.broker.port}") int brokerPort) {

        this.legacyConfig = legacyConfig;
        this.brokerHost = brokerHost;
        this.brokerPort = brokerPort;

        log.info("event=legacy_consumer.initialized service={} broker={}:{}", legacyConfig.getServiceName(), brokerHost, brokerPort);
    }

    @Override
    public void onApplicationEvent(ServerStartupEvent event) {
        log.info("event=legacy_consumer.starting service={} broker={}:{}", legacyConfig.getServiceName(), brokerHost, brokerPort);

        try {
            connect();
        } catch (Exception e) {
            log.error("event=legacy_consumer.start_failed service={}", legacyConfig.getServiceName(), e);
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

        log.info("event=legacy_consumer.started service={}", legacyConfig.getServiceName());
    }

    private void runEventLoop() {
        log.info("event=legacy_consumer.event_loop_started service={}", legacyConfig.getServiceName());

        while (running) {
            try {
                Event event = connection.nextEvent();
                log.debug("event=legacy_consumer.event_received type={}", event.getType());

                handleEvent(event);

            } catch (Exception e) {
                if (running) {
                    log.error("event=legacy_consumer.event_loop_failed service={}", legacyConfig.getServiceName(), e);
                    // Optionally: implement reconnection logic here
                }
                running = false;
            }
        }

        log.info("event=legacy_consumer.event_loop_stopped service={}", legacyConfig.getServiceName());
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
                log.info("event=legacy_consumer.eof_received service={}", legacyConfig.getServiceName());
                running = false;
                break;

            default:
                log.warn("event=legacy_consumer.unexpected_event service={} type={}", legacyConfig.getServiceName(), event.getType());
        }

        if (shouldAck) {
            connection.sendAck();
        }
    }

    private boolean handleBatch(BatchEvent batchEvent) throws Exception {
        int size = batchEvent.count();
        batchCount++;
        messageCount += size;
        if (batchCount == 1 || batchCount % INFO_BATCH_SUMMARY_INTERVAL == 0) {
            log.info("event=legacy_consumer.progress service={} batches={} messages={} lastBatchSize={}",
                    legacyConfig.getServiceName(), batchCount, messageCount, size);
        } else {
            log.debug("event=legacy_consumer.batch_received service={} batch={} size={} cumulativeMessages={}",
                    legacyConfig.getServiceName(), batchCount, size, messageCount);
        }

        for (Event e : batchEvent.getEvents()) {
            if (e.getType() == EventType.MESSAGE) {
                DataMessageEvent msgEvent = (DataMessageEvent) e;
                // Convert to ConsumerRecord for GenericConsumerHandler
                // For now, just process the data
                log.debug("event=legacy_consumer.batch_message service={} type={} key={}",
                        legacyConfig.getServiceName(),
                        msgEvent.getMessage().getType(),
                        msgEvent.getMessage().getKey());
            } else if (e.getType() == EventType.DELETE) {
                DeleteMessageEvent delEvent = (DeleteMessageEvent) e;
                log.debug("event=legacy_consumer.batch_delete service={} key={}",
                        legacyConfig.getServiceName(), delEvent.getMessage().getKey());
            }
        }

        return true;  // Send ACK
    }

    private boolean handleMessage(DataMessageEvent event) throws Exception {
        DataMessage msg = event.getMessage();
        log.debug("event=legacy_consumer.message_received service={} type={} key={}",
                legacyConfig.getServiceName(), msg.getType(), msg.getKey());
        return true;  // Send ACK
    }

    private boolean handleReset(ResetEvent event) throws Exception {
        batchCount = 0;
        messageCount = 0;
        log.info("event=legacy_consumer.reset_received service={}", legacyConfig.getServiceName());
        // Call GenericConsumerHandler.onReset() if needed
        // messageHandler.onReset("all-topics");
        return true;  // Send ACK
    }

    private boolean handleReady(ReadyEvent event) throws Exception {
        log.info("event=legacy_consumer.ready_received service={}", legacyConfig.getServiceName());
        return true;  // Send ACK
    }

    @PreDestroy
    public void shutdown() {
        if (!shutdownStarted.compareAndSet(false, true)) {
            return;
        }
        log.info("event=legacy_consumer.shutdown_started service={}", legacyConfig.getServiceName());
        running = false;

        try {
            if (connection != null) {
                connection.close();
            }

            if (eventLoopThread != null) {
                eventLoopThread.join(5000);
                if (eventLoopThread.isAlive()) {
                    eventLoopThread.interrupt();
                }
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            if (eventLoopThread != null) {
                eventLoopThread.interrupt();
            }
            log.warn("event=legacy_consumer.shutdown_interrupted service={}", legacyConfig.getServiceName());
        } catch (Exception e) {
            log.error("event=legacy_consumer.shutdown_failed service={}", legacyConfig.getServiceName(), e);
        }

        log.info("event=legacy_consumer.shutdown_complete service={}", legacyConfig.getServiceName());
    }
}
