package com.example.consumer;


import com.messaging.common.annotation.Consumer;
import com.messaging.common.annotation.RetryPolicy;
import com.messaging.common.api.MessageHandler;
import com.messaging.common.model.ConsumerRecord;
import com.messaging.common.model.EventType;
import io.micronaut.context.annotation.Requires;
import io.micronaut.context.annotation.Value;
import jakarta.inject.Inject;
import jakarta.inject.Singleton;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

/**
 * Generic consumer handler that adapts based on environment variables.
 * Single class handles all consumer types (price, product, inventory, etc.)
 *
 * NOTE: This handler is DISABLED when consumer.legacy.enabled=true
 * In legacy mode, LegacyConsumerService handles message consumption instead.
 */
@Singleton
@Requires(property = "consumer.legacy.enabled", notEquals = "true")
@Consumer(
    topic = "${CONSUMER_TOPICS:price-topic}",
    group = "${CONSUMER_GROUP:price-group}"
    // RetryPolicy will be set via RetryPolicyProvider
)
public class GenericConsumerHandler implements MessageHandler {
    private static final Logger log = LoggerFactory.getLogger(GenericConsumerHandler.class);

    @Value("${consumer.type}")
    private String consumerType;

    private int recordCount=0;

    @Override
    public void handleBatch(List<ConsumerRecord> records) throws Exception {

        recordCount+=records.size();

        // CRITICAL DEBUG: Log EVERY batch received with detailed information
        // This helps diagnose data refresh metric discrepancies by tracking actual deliveries
        if (!records.isEmpty()) {
            // Calculate approximate batch size (rough estimate based on average message size)
            long estimatedBytes = records.stream()
                .mapToLong(r -> {
                    long size = r.getMsgKey().length();
                    if (r.getData() != null) {
                        size += r.getData().length();
                    }
                    return size + 32; // Add overhead for metadata
                })
                .sum();

            // Log batch reception (offset and topic not exposed to consumer)
            log.info("📦 [{}] BATCH_RECEIVED messages={}, estimatedBytes={}, cumulativeCount={}, firstKey={}, lastKey={}",
                     consumerType, records.size(), estimatedBytes, recordCount,
                     records.get(0).getMsgKey(), records.get(records.size() - 1).getMsgKey());
        }

        log.info("[{}] Successfully processed batch of {} records", consumerType, recordCount);
    }

    @Override
    public void onReset(String topic) throws Exception {
        // Clear caches, reset state, prepare for refreshed data
        // Example: cache.clear();
        recordCount=0;
        log.info("[{}] Cleanup complete for topic: {}, ready to receive fresh data reseting count to {}", consumerType, topic);
    }

    @Override
    public void onReady(String topic) throws Exception {
        log.info("[{}] ===== GOT READY for topic: {} ===== Performing finalization operations... with total records {}", consumerType, topic,recordCount);

    }

    private void handleMessage(ConsumerRecord record) throws Exception {
        // Store MESSAGE in consumer's segments


        // Process business logic specific to consumer type
        processBusinessLogic(record);

        //log.info("[{}] Processed MESSAGE: key={}", consumerType, record.getMsgKey());
    }

    private void handleDelete(ConsumerRecord record) throws Exception {
        // Store DELETE (tombstone) in consumer's segments


        // Handle deletion logic
        handleDeletion(record.getMsgKey());

        //log.info("[{}] Processed DELETE: key={}", consumerType, record.getMsgKey());
    }

    private void processBusinessLogic(ConsumerRecord record) {
        // Business logic specific to consumer type
        // E.g., price consumer might update price index
        log.debug("[{}] Business logic for: {}", consumerType,record.getEventType(), record.getMsgKey());

    }

    private void processPriceUpdate(ConsumerRecord record) {
        // Price-specific logic (e.g., update price cache, trigger alerts)
        log.debug("[PRICE] Processing price update for: {}", record.getMsgKey());
    }

    private void processProductUpdate(ConsumerRecord record) {
        // Product-specific logic (e.g., update search index)
        log.debug("[PRODUCT] Processing product update for: {}", record.getMsgKey());
    }

    private void processInventoryUpdate(ConsumerRecord record) {
        // Inventory-specific logic (e.g., check stock levels, trigger reorder)
        log.debug("[INVENTORY] Processing inventory update for: {}", record.getMsgKey());
    }

    private void processAuditLog(ConsumerRecord record) {
        // Audit-specific logic (e.g., append to audit trail, compliance checks)
        log.debug("[AUDIT] Processing audit log for: {}", record.getMsgKey());
    }

    private void handleDeletion(String msgKey) {
        log.debug("[{}] Handling deletion for: {}", consumerType, msgKey);
    }
}
