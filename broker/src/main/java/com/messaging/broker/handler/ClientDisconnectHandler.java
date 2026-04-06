package com.messaging.broker.handler;

import com.messaging.broker.handler.DisconnectHandler;
import com.messaging.broker.consumer.ConsumerRegistry;
import com.messaging.broker.monitoring.BrokerMetrics;
import jakarta.inject.Inject;
import jakarta.inject.Singleton;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Handles client disconnections - unregisters consumers and updates metrics.
 */
@Singleton
public class ClientDisconnectHandler implements DisconnectHandler {
    private static final Logger log = LoggerFactory.getLogger(ClientDisconnectHandler.class);

    private final ConsumerRegistry remoteConsumers;
    private final BrokerMetrics metrics;

    @Inject
    public ClientDisconnectHandler(
            ConsumerRegistry remoteConsumers,
            BrokerMetrics metrics) {
        this.remoteConsumers = remoteConsumers;
        this.metrics = metrics;
    }

    @Override
    public void handleDisconnect(String clientId) {
        log.info("Handling disconnect for client: {}", clientId);
        int unregisteredCount = remoteConsumers.unregisterConsumer(clientId);

        // Decrement metrics for each topic subscription that was removed
        for (int i = 0; i < unregisteredCount; i++) {
            metrics.recordConsumerDisconnection();
        }

        log.info("Disconnected client {} with {} topic subscriptions", clientId, unregisteredCount);
    }
}
