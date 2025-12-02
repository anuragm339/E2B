package com.messaging.broker.pipe;

import com.messaging.common.api.StorageEngine;
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
     */
    public List<MessageRecord> getMessagesForChild(String topic, long offset, int limit) {
        try {
            List<MessageRecord> records = storage.read(topic, 0, offset, limit);
            log.debug("Serving {} messages to child: topic={}, offset={}",
                     records.size(), topic, offset);
            return records;
        } catch (Exception e) {
            log.error("Error reading messages for child", e);
            throw new RuntimeException("Failed to read messages", e);
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
