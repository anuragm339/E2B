package com.messaging.broker.ack;

import com.messaging.broker.consumer.ConsumerOffsetTracker;
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
 * to surface any offsets that have never been acknowledged by a consumer.
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
    private final ConsumerOffsetTracker offsetTracker;
    private final boolean enabled;
    private final boolean autoSyncEnabled;

    public AckReconciliationScheduler(
            ConsumerRegistrationService registrationService,
            StorageEngine storage,
            RocksDbAckStore ackStore,
            BrokerMetrics metrics,
            ConsumerOffsetTracker offsetTracker,
            @Value("${ack-store.reconciliation.enabled:true}") boolean enabled,
            @Value("${ack-store.reconciliation.auto-sync-enabled:false}") boolean autoSyncEnabled) {
        this.registrationService = registrationService;
        this.storage = storage;
        this.ackStore = ackStore;
        this.metrics = metrics;
        this.offsetTracker = offsetTracker;
        this.enabled = enabled;
        this.autoSyncEnabled = autoSyncEnabled;
    }

    @Scheduled(
            fixedDelay  = "${ack-store.reconciliation.interval:14m}",
            initialDelay = "${ack-store.reconciliation.initial-delay:10m}")
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
            long earliestOffset = storage.getEarliestOffset(topic, 0);

            for (String group : entry.getValue()) {
                // Use the consumer's committed offset as the upper scan bound.
                // Records at or after committedOffset are either in-flight or undelivered —
                // they cannot be expected to have ACK entries yet.
                long committedOffset = offsetTracker.getOffset(group + ":" + topic);
                if (committedOffset <= earliestOffset) {
                    continue;  // consumer has not consumed anything yet
                }
                reconcileTopicGroup(topic, group, earliestOffset, committedOffset);
            }
        }
    }

    private void reconcileTopicGroup(String topic, String group, long earliestOffset, long committedOffset) {
        long missingCount = 0;
        long minMissingOffset = Long.MAX_VALUE;
        long maxMissingOffset = Long.MIN_VALUE;
        long offset = earliestOffset;

        // Collect missing records for backfill (only populated when autoSyncEnabled)
        List<String> backfillTopics  = autoSyncEnabled ? new ArrayList<>() : null;
        List<String> backfillGroups  = autoSyncEnabled ? new ArrayList<>() : null;
        List<AckRecord> backfillAcks = autoSyncEnabled ? new ArrayList<>() : null;
        long now = System.currentTimeMillis();

        outer:
        while (offset < committedOffset) {
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
                if (r.getOffset() >= committedOffset) {
                    // Past the committed boundary — consumer hasn't ACKed these yet
                    break outer;
                }
                AckRecord ackRecord = ackStore.get(topic, group, r.getOffset());
                if (ackRecord == null) {
                    missingCount++;
                    if (r.getOffset() < minMissingOffset) minMissingOffset = r.getOffset();
                    if (r.getOffset() > maxMissingOffset) maxMissingOffset = r.getOffset();
                    if (autoSyncEnabled) {
                        backfillTopics.add(topic);
                        backfillGroups.add(group);
                        backfillAcks.add(new AckRecord(r.getOffset(), now));
                    }
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
            if (autoSyncEnabled && !backfillAcks.isEmpty()) {
                try {
                    ackStore.putBatch(
                            backfillTopics.toArray(new String[0]),
                            backfillGroups.toArray(new String[0]),
                            backfillAcks.toArray(new AckRecord[0]));
                    log.info("Reconciliation auto-sync: backfilled {} missing offsets for topic={} group={}",
                            backfillAcks.size(), topic, group);
                } catch (Exception e) {
                    log.warn("Reconciliation auto-sync: backfill write failed for topic={} group={}", topic, group, e);
                }
            }
        } else {
            // Consistent — clear the gap markers back to sentinel -1
            metrics.updateReconciliationGapOffsets(topic, group, -1, -1);
            log.debug("Reconciliation: topic={} group={} CONSISTENT", topic, group);
        }
    }
}
