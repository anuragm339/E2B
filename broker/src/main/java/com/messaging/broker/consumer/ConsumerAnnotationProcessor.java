package com.messaging.broker.consumer;

import com.messaging.common.annotation.Consumer;
import com.messaging.common.api.ErrorHandler;
import com.messaging.common.api.MessageHandler;
import io.micronaut.context.ApplicationContext;
import io.micronaut.context.event.ApplicationEventListener;
import io.micronaut.runtime.server.event.ServerStartupEvent;
import jakarta.inject.Inject;
import jakarta.inject.Singleton;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collection;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Scans for @Consumer annotated classes and registers them
 */
@Singleton
public class ConsumerAnnotationProcessor implements ApplicationEventListener<ServerStartupEvent> {
    private static final Logger log = LoggerFactory.getLogger(ConsumerAnnotationProcessor.class);

    private final ApplicationContext context;
    private final Map<String, ConsumerContext> consumers = new ConcurrentHashMap<>();

    @Inject
    public ConsumerAnnotationProcessor(ApplicationContext context) {
        this.context = context;
        log.info("ConsumerAnnotationProcessor initialized");
    }

    @Override
    public void onApplicationEvent(ServerStartupEvent event) {
        log.info("Scanning for @Consumer annotated beans...");
        scanAndRegisterConsumers();
    }

    /**
     * Scan classpath for @Consumer annotated classes
     */
    private void scanAndRegisterConsumers() {
        // Get all MessageHandler beans
        Collection<MessageHandler> handlerBeans = context.getBeansOfType(MessageHandler.class);

        log.info("Found {} MessageHandler beans", handlerBeans.size());

        for (MessageHandler handler : handlerBeans) {
            // Check if it has @Consumer annotation
            Consumer annotation = handler.getClass().getAnnotation(Consumer.class);
            if (annotation != null) {
                registerConsumer(handler, annotation);
            }
        }

        log.info("Consumer registration complete. Total consumers: {}", consumers.size());
    }

    /**
     * Register a single consumer bean
     */
    private void registerConsumer(MessageHandler handler, Consumer annotation) {
        Class<?> clazz = handler.getClass();

        // Create error handler
        ErrorHandler errorHandler = createErrorHandler(annotation);

        // Generate unique consumer ID
        String consumerId = generateConsumerId(annotation);

        // Create consumer context
        ConsumerContext consumerContext = new ConsumerContext(
                consumerId,
                annotation,
                handler,
                errorHandler
        );

        // Register
        consumers.put(consumerId, consumerContext);

        log.info("Registered consumer: {} -> topic={}, group={}, retryPolicy={}",
                consumerId, annotation.topic(), annotation.group(), annotation.retryPolicy());
    }

    /**
     * Create error handler instance from annotation
     */
    private ErrorHandler createErrorHandler(Consumer annotation) {
        try {
            Class<? extends ErrorHandler> errorHandlerClass = annotation.errorHandler();
            return context.createBean(errorHandlerClass);
        } catch (Exception e) {
            log.warn("Failed to create error handler, using no-op handler", e);
            return context.getBean(com.messaging.common.api.NoOpErrorHandler.class);
        }
    }

    /**
     * Generate unique consumer ID: {topic}:{group}:{className}
     */
    private String generateConsumerId(Consumer annotation) {
        return String.format("%s:%s:%s",
                annotation.topic(),
                annotation.group(),
                System.currentTimeMillis());
    }

    /**
     * Get all registered consumers
     */
    public Collection<ConsumerContext> getAllConsumers() {
        return consumers.values();
    }

    /**
     * Get consumers for a specific topic
     */
    public Collection<ConsumerContext> getConsumersByTopic(String topic) {
        return consumers.values().stream()
                .filter(ctx -> ctx.getTopic().equals(topic))
                .toList();
    }

    /**
     * Get consumer by ID
     */
    public ConsumerContext getConsumer(String consumerId) {
        return consumers.get(consumerId);
    }

    /**
     * Remove consumer
     */
    public void unregisterConsumer(String consumerId) {
        ConsumerContext removed = consumers.remove(consumerId);
        if (removed != null) {
            log.info("Unregistered consumer: {}", consumerId);
        }
    }
}
