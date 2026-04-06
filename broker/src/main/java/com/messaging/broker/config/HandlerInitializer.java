package com.messaging.broker.config;

import com.messaging.broker.handler.MessageHandlerRegistry;
import com.messaging.broker.handler.*;
import io.micronaut.context.event.ApplicationEventListener;
import io.micronaut.context.event.StartupEvent;
import jakarta.inject.Inject;
import jakarta.inject.Singleton;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Initializes and registers all message handlers with the handler registry.
 */
@Singleton
public class HandlerInitializer implements ApplicationEventListener<StartupEvent> {
    private static final Logger log = LoggerFactory.getLogger(HandlerInitializer.class);

    private final MessageHandlerRegistry registry;
    private final DataHandler dataHandler;
    private final SubscribeHandler subscribeHandler;
    private final CommitOffsetHandler commitOffsetHandler;
    private final ResetAckHandler resetAckHandler;
    private final ReadyAckHandler readyAckHandler;
    private final BatchAckHandler batchAckHandler;

    @Inject
    public HandlerInitializer(
            MessageHandlerRegistry registry,
            DataHandler dataHandler,
            SubscribeHandler subscribeHandler,
            CommitOffsetHandler commitOffsetHandler,
            ResetAckHandler resetAckHandler,
            ReadyAckHandler readyAckHandler,
            BatchAckHandler batchAckHandler) {
        this.registry = registry;
        this.dataHandler = dataHandler;
        this.subscribeHandler = subscribeHandler;
        this.commitOffsetHandler = commitOffsetHandler;
        this.resetAckHandler = resetAckHandler;
        this.readyAckHandler = readyAckHandler;
        this.batchAckHandler = batchAckHandler;
    }

    @Override
    public void onApplicationEvent(StartupEvent event) {
        log.info("Registering message handlers...");

        registry.registerHandler(dataHandler);
        registry.registerHandler(subscribeHandler);
        registry.registerHandler(commitOffsetHandler);
        registry.registerHandler(resetAckHandler);
        registry.registerHandler(readyAckHandler);
        registry.registerHandler(batchAckHandler);

        log.info("All message handlers registered successfully");
    }
}
