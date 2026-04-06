package com.messaging.broker.handler;

import com.messaging.common.model.BrokerMessage;
import jakarta.inject.Singleton;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Registry for message handlers.
 * Maps message types to their corresponding handlers.
 */
@Singleton
public class MessageHandlerRegistry {
    private static final Logger log = LoggerFactory.getLogger(MessageHandlerRegistry.class);

    private final Map<BrokerMessage.MessageType, MessageHandler> handlers = new ConcurrentHashMap<>();

    /**
     * Register a message handler.
     *
     * @param handler Handler to register
     */
    public void registerHandler(MessageHandler handler) {
        BrokerMessage.MessageType type = handler.getMessageType();
        handlers.put(type, handler);
        log.info("Registered handler for message type: {}", type);
    }

    /**
     * Get handler for message type.
     *
     * @param type Message type
     * @return Handler, or null if no handler registered
     */
    public MessageHandler getHandler(BrokerMessage.MessageType type) {
        return handlers.get(type);
    }

    /**
     * Check if handler exists for message type.
     *
     * @param type Message type
     * @return true if handler registered
     */
    public boolean hasHandler(BrokerMessage.MessageType type) {
        return handlers.containsKey(type);
    }
}
