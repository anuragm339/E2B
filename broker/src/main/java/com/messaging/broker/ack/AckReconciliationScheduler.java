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
import java.util.concurrent.ConcurrentHashMap;

/**
 * Periodically scans sealed-segment data and compares against the RocksDB ACK store
 * to surface any offsets that have never been acknowledged by a consumer.
 *
 * Results are exposed as a Prometheus gauge:
 *   ack_reconciliation_missing_keys{topic="...", group="..."}
 *
 * Refresh-awareness:
 *   During a data refresh, RocksDB is intentionally wiped and re-populated via replay.
 *   Running reconciliation mid-refresh would produce massive false-positive "missing" counts
 *   and, if auto-sync is enabled, would fight against the ongoing replay writes.
 *   Call pauseForTopic(topic) before the RESET broadcast and resumeForTopic(topic) once the
 *   refresh completes. Any in-memory scan checkpoints for that topic are also cleared on
 *   pause so the first post-refresh run performs a full verification from scratch.
 *
 * Scan checkpointing:
 *   Each completed reconcileTopicGroup() call records the scanned committedOffset as a
 *   checkpoint (keyed by "topic:group"). The next run starts from max(earliestOffset, checkpoint)
 *   instead of always scanning from earliestOffset, avoiding redundant re-scans of records
 *   already verified in previous runs. Checkpoints are in-memory only — a broker restart
 *   resets them to trigger one full scan on the first scheduled execution.
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

    // Topics whose RocksDB ACK data is currently being wiped/rebuilt by a data refresh.
    // Reconciliation is skipped entirely for these topics until resumeForTopic() is called.
    private final Set<String> pausedTopics = ConcurrentHashMap.newKeySet();

    // Last committedOffset that was fully scanned for each "topic:group".
    // Next reconcile() call starts from max(earliestOffset, checkpoint) instead of earliestOffset,
    // skipping ranges that were already verified in the previous run.
    // Entries are removed by pauseForTopic() so a post-refresh run always does a full scan.
    private final ConcurrentHashMap<String, Long> scanCheckpoints = new ConcurrentHashMap<>();

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

    // ── Refresh coordination ──────────────────────────────────────────────────

    /**
     * Pause reconciliation for {@code topic} and clear its scan checkpoints.
     * Called by RefreshResetService immediately before broadcasting RESET so that the
     * reconciler never runs against a partially-wiped RocksDB store.
     */
    public void pauseForTopic(String topic) {
        pausedTopics.add(topic);
        scanCheckpoints.keySet().removeIf(key -> key.startsWith(topic + ":"));
        log.info("Reconciliation paused for topic={} (data refresh started)", topic);
    }

    /**
     * Resume reconciliation for {@code topic} after a data refresh completes.
     * Because pauseForTopic() already cleared the checkpoints, the first run after
     * resume performs a full scan from earliestOffset to verify the replayed data.
     */
    public void resumeForTopic(String topic) {
        pausedTopics.remove(topic);
        log.info("Reconciliation resumed for topic={} (data refresh completed)", topic);
    }

    // ── Scheduled reconciliation ──────────────────────────────────────────────

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

            // Skip topics whose ACK store is being wiped/rebuilt by an active data refresh.
            if (pausedTopics.contains(topic)) {
                log.debug("Reconciliation skipped for topic={} — data refresh in progress", topic);
                continue;
            }

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
        // Start from the checkpoint saved by the previous run (or earliestOffset on first run /
        // after a refresh). This avoids re-scanning records that were already verified clean.
        String checkpointKey = topic + ":" + group;
        long checkpointOffset = scanCheckpoints.getOrDefault(checkpointKey, earliestOffset);
        long scanStart = Math.max(earliestOffset, checkpointOffset);

        long missingCount = 0;
        long minMissingOffset = Long.MAX_VALUE;
        long maxMissingOffset = Long.MIN_VALUE;
        long offset = scanStart;

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

        // Advance checkpoint to committedOffset so the next run only scans new records.
        // This is correct regardless of missingCount: any gaps found this run have already
        // been reported (and backfilled if auto-sync is enabled). Re-scanning won't change
        // their status — only new delivery failures after this point are actionable.
        scanCheckpoints.put(checkpointKey, committedOffset);

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
