package com.messaging.broker.core;

import com.messaging.common.api.StorageEngine;
import com.messaging.common.exception.ErrorCode;
import com.messaging.common.exception.ExceptionLogger;
import com.messaging.common.exception.MessagingException;
import com.messaging.common.model.MessageRecord;
import jakarta.inject.Singleton;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

/**
 * Forwards messages from parent storage to child brokers via Pipe
 * This component makes messages available via HTTP endpoints (served by PipeServer)
 */
@Singleton
public class PipeMessageForwarder {
    private static final Logger log = LoggerFactory.getLogger(PipeMessageForwarder.class);

    private final StorageEngine storage;

    public PipeMessageForwarder(StorageEngine storage) {
        this.storage = storage;
        log.info("PipeMessageForwarder initialized - messages will be served via /pipe/poll endpoint");
    }

    /**
     * Get messages for child broker
     * This is called by PipeServer when a child broker polls
     * @throws MessagingException if reading messages fails
     */
    public List<MessageRecord> getMessagesForChild(String topic, long offset, int limit) throws MessagingException {
        try {
            List<MessageRecord> records = storage.read(topic, 0, offset, limit);
            log.debug("Serving {} messages to child: topic={}, offset={}",
                     records.size(), topic, offset);
            return records;
        } catch (MessagingException e) {
            // Re-throw as-is
            throw e;
        } catch (Exception e) {
            // Wrap unexpected exceptions and throw
            throw ExceptionLogger.logAndThrow(log,
                new MessagingException(ErrorCode.PIPE_MESSAGE_FORWARD_FAILED,
                    "Failed to read messages for child broker", e)
                    .withContext("topic", topic)
                    .withContext("offset", offset)
                    .withContext("limit", limit));
        }
    }

    /**
     * Get current max offset for a topic
     */
    public long getCurrentOffset(String topic) {
        // This would need to be implemented in StorageEngine
        // For now, return 0
        return 0;
    }
}
