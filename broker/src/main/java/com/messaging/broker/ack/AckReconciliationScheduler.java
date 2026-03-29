package com.messaging.broker.ack;

import com.messaging.broker.consumer.ConsumerRegistrationService;
import com.messaging.broker.consumer.RemoteConsumer;
import com.messaging.broker.monitoring.BrokerMetrics;
import com.messaging.common.api.StorageEngine;
import com.messaging.common.model.MessageRecord;
import io.micronaut.context.annotation.Value;
import io.micronaut.scheduling.annotation.Scheduled;
import jakarta.inject.Singleton;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;

/**
 * Periodically scans sealed-segment data and compares against the RocksDB ACK store
 * to surface any msgKeys that have never been acknowledged by a consumer.
 *
 * Results are exposed as a Prometheus gauge:
 *   ack_reconciliation_missing_keys{topic="...", group="..."}
 *
 * Auto-sync (re-delivery of missing keys) is stubbed for a future iteration.
 */
@Singleton
public class AckReconciliationScheduler {

    private static final Logger log = LoggerFactory.getLogger(AckReconciliationScheduler.class);
    private static final int BATCH_SIZE = 500;

    private final ConsumerRegistrationService registrationService;
    private final StorageEngine storage;
    private final RocksDbAckStore ackStore;
    private final BrokerMetrics metrics;
    private final boolean enabled;
    private final boolean autoSyncEnabled;

    public AckReconciliationScheduler(
            ConsumerRegistrationService registrationService,
            StorageEngine storage,
            RocksDbAckStore ackStore,
            BrokerMetrics metrics,
            @Value("${ack-store.reconciliation.enabled:true}") boolean enabled,
            @Value("${ack-store.reconciliation.auto-sync-enabled:false}") boolean autoSyncEnabled) {
        this.registrationService = registrationService;
        this.storage = storage;
        this.ackStore = ackStore;
        this.metrics = metrics;
        this.enabled = enabled;
        this.autoSyncEnabled = autoSyncEnabled;
    }

    @Scheduled(
            fixedDelay  = "${ack-store.reconciliation.interval:5m}",
            initialDelay = "${ack-store.reconciliation.initial-delay:2m}")
    public void reconcile() {
        if (!enabled) {
            return;
        }

        // Collect unique (topic, group) pairs from all registered consumers
        Collection<RemoteConsumer> consumers = registrationService.getAllConsumers();
        if (consumers.isEmpty()) {
            return;
        }

        // Deduplicate by topic+group
        Map<String, Set<String>> topicToGroups = new LinkedHashMap<>();
        for (RemoteConsumer c : consumers) {
            topicToGroups.computeIfAbsent(c.getTopic(), k -> new LinkedHashSet<>()).add(c.getGroup());
        }

        for (Map.Entry<String, Set<String>> entry : topicToGroups.entrySet()) {
            String topic = entry.getKey();

            // Determine range: earliest offset → current head (exclusive, i.e., active segment boundary)
            long earliestOffset = storage.getEarliestOffset(topic, 0);
            long headOffset = storage.getCurrentOffset(topic, 0);
            if (headOffset <= earliestOffset) {
                continue;  // no sealed data
            }

            for (String group : entry.getValue()) {
                reconcileTopicGroup(topic, group, earliestOffset, headOffset);
            }
        }
    }

    private void reconcileTopicGroup(String topic, String group, long earliestOffset, long headOffset) {
        long missingCount = 0;
        long minMissingOffset = Long.MAX_VALUE;
        long maxMissingOffset = Long.MIN_VALUE;
        long offset = earliestOffset;

        outer:
        while (offset < headOffset) {
            List<MessageRecord> records;
            try {
                records = storage.read(topic, 0, offset, BATCH_SIZE);
            } catch (Exception e) {
                log.warn("Reconciliation: storage read failed for topic={} group={} offset={}", topic, group, offset, e);
                break;
            }

            if (records.isEmpty()) {
                break;
            }

            for (MessageRecord r : records) {
                if (r.getMsgKey() == null) {
                    continue;
                }
                if (r.getOffset() >= headOffset) {
                    // Reached active segment boundary — stop
                    break outer;
                }
                AckRecord ackRecord = ackStore.get(topic, group, r.getMsgKey());
                if (ackRecord == null) {
                    missingCount++;
                    if (r.getOffset() < minMissingOffset) minMissingOffset = r.getOffset();
                    if (r.getOffset() > maxMissingOffset) maxMissingOffset = r.getOffset();
                }
            }

            // Advance to the offset after the last record read
            offset = records.get(records.size() - 1).getOffset() + 1;
        }

        metrics.updateReconciliationMissingKeys(topic, group, missingCount);

        if (missingCount > 0) {
            // Report gap range: earliest and latest offset that has no ACK record
            metrics.updateReconciliationGapOffsets(topic, group, minMissingOffset, maxMissingOffset);
            log.warn("Reconciliation WARNING: topic={} group={} missing={} offsetRange=[{}, {}]",
                    topic, group, missingCount, minMissingOffset, maxMissingOffset);
            if (autoSyncEnabled) {
                // TODO: trigger re-delivery of missing keys (future iteration)
                log.debug("Auto-sync is enabled but not yet implemented for topic={} group={}", topic, group);
            }
        } else {
            // Consistent — clear the gap markers back to sentinel -1
            metrics.updateReconciliationGapOffsets(topic, group, -1, -1);
            log.debug("Reconciliation: topic={} group={} CONSISTENT", topic, group);
        }
    }
}
