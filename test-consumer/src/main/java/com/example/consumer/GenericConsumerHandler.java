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
    private static final int INFO_BATCH_SUMMARY_INTERVAL = 100;

    @Value("${consumer.type}")
    private String consumerType;

    private int recordCount = 0;
    private int batchCount = 0;

    @Override
    public void handleBatch(List<ConsumerRecord> records) throws Exception {
        batchCount++;
        recordCount += records.size();

        if (!records.isEmpty()) {
            long estimatedBytes = records.stream()
                .mapToLong(r -> {
                    long size = r.getMsgKey().length();
                    if (r.getData() != null) {
                        size += r.getData().length();
                    }
                    return size + 32; // Add overhead for metadata
                })
                .sum();

            if (batchCount == 1 || batchCount % INFO_BATCH_SUMMARY_INTERVAL == 0) {
                log.info("event=consumer.progress consumerType={} batches={} messages={} lastBatchMessages={} estimatedBytes={} firstKey={} lastKey={}",
                        consumerType, batchCount, recordCount, records.size(), estimatedBytes,
                        records.get(0).getMsgKey(), records.get(records.size() - 1).getMsgKey());
            } else {
                log.debug("event=consumer.batch_received consumerType={} batch={} messages={} estimatedBytes={} cumulativeCount={} firstKey={} lastKey={}",
                        consumerType, batchCount, records.size(), estimatedBytes, recordCount,
                        records.get(0).getMsgKey(), records.get(records.size() - 1).getMsgKey());
            }
        }

        log.debug("event=consumer.batch_processed consumerType={} cumulativeCount={}", consumerType, recordCount);
    }

    @Override
    public void onReset(String topic) throws Exception {
        // Clear caches, reset state, prepare for refreshed data
        // Example: cache.clear();
        recordCount = 0;
        batchCount = 0;
        log.info("event=consumer.reset consumerType={} topic={} cumulativeCountResetTo={}", consumerType, topic, recordCount);
    }

    @Override
    public void onReady(String topic) throws Exception {
        log.info("event=consumer.ready consumerType={} topic={} cumulativeCount={}", consumerType, topic, recordCount);

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
        log.debug("event=consumer.business_logic consumerType={} eventType={} msgKey={}",
                consumerType, record.getEventType(), record.getMsgKey());

    }

    private void processPriceUpdate(ConsumerRecord record) {
        // Price-specific logic (e.g., update price cache, trigger alerts)
        log.debug("event=consumer.price_update msgKey={}", record.getMsgKey());
    }

    private void processProductUpdate(ConsumerRecord record) {
        // Product-specific logic (e.g., update search index)
        log.debug("event=consumer.product_update msgKey={}", record.getMsgKey());
    }

    private void processInventoryUpdate(ConsumerRecord record) {
        // Inventory-specific logic (e.g., check stock levels, trigger reorder)
        log.debug("event=consumer.inventory_update msgKey={}", record.getMsgKey());
    }

    private void processAuditLog(ConsumerRecord record) {
        // Audit-specific logic (e.g., append to audit trail, compliance checks)
        log.debug("event=consumer.audit_update msgKey={}", record.getMsgKey());
    }

    private void handleDeletion(String msgKey) {
        log.debug("event=consumer.delete consumerType={} msgKey={}", consumerType, msgKey);
    }
}
