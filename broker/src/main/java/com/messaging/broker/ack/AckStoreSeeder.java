package com.messaging.broker.ack;

import com.messaging.broker.consumer.ConsumerOffsetTracker;
import com.messaging.common.api.StorageEngine;
import com.messaging.common.model.MessageRecord;
import jakarta.inject.Singleton;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * Backfills RocksDB ACK entries at broker startup for records that were consumed
 * (committed offset advanced) before RocksDB tracking existed, or lost in a
 * broker-restart race between the offset-file write and the async RocksDB write.
 *
 * Called once from BrokerService after storage.recover() so that segment data
 * is available. Runs synchronously so the reconciler sees a consistent ACK store
 * before its first scheduled execution (2-minute initial delay).
 */
@Singleton
public class AckStoreSeeder {

    private static final Logger log = LoggerFactory.getLogger(AckStoreSeeder.class);
    private static final int READ_BATCH_SIZE = 500;

    private final ConsumerOffsetTracker offsetTracker;
    private final RocksDbAckStore ackStore;
    private final StorageEngine storage;

    public AckStoreSeeder(
            ConsumerOffsetTracker offsetTracker,
            RocksDbAckStore ackStore,
            StorageEngine storage) {
        this.offsetTracker = offsetTracker;
        this.ackStore = ackStore;
        this.storage = storage;
    }

    /**
     * Scan sealed segments up to each consumer's committed offset and write
     * synthetic ACK entries for any offset that has no existing RocksDB entry.
     *
     * Safe to call multiple times — existing entries are never overwritten.
     */
    public void seed() {
        Map<String, Long> allOffsets = offsetTracker.getAllOffsets();
        if (allOffsets.isEmpty()) {
            log.info("AckStoreSeeder: no committed offsets found — nothing to seed");
            return;
        }

        log.info("AckStoreSeeder: seeding RocksDB for {} topic/group pairs", allOffsets.size());
        int totalSeeded = 0;

        for (Map.Entry<String, Long> entry : allOffsets.entrySet()) {
            String consumerKey = entry.getKey(); // "group:topic"
            long committedOffset = entry.getValue();
            if (committedOffset <= 0) {
                continue;
            }

            int colonIdx = consumerKey.indexOf(':');
            if (colonIdx < 0) {
                log.warn("AckStoreSeeder: skipping malformed key '{}'", consumerKey);
                continue;
            }
            String group = consumerKey.substring(0, colonIdx);
            String topic = consumerKey.substring(colonIdx + 1);

            totalSeeded += seedTopicGroup(topic, group, committedOffset);
        }

        if (totalSeeded > 0) {
            log.info("AckStoreSeeder: backfilled {} missing ACK entries across all topic/group pairs", totalSeeded);
        } else {
            log.info("AckStoreSeeder: all ACK entries already present — nothing to backfill");
        }
    }

    private int seedTopicGroup(String topic, String group, long committedOffset) {
        long earliestOffset = storage.getEarliestOffset(topic, 0);
        long offset = earliestOffset;
        int seeded = 0;

        while (offset < committedOffset) {
            List<MessageRecord> records;
            try {
                records = storage.read(topic, 0, offset, READ_BATCH_SIZE);
            } catch (Exception e) {
                log.warn("AckStoreSeeder: storage read failed for topic={} group={} offset={}", topic, group, offset, e);
                break;
            }

            if (records.isEmpty()) {
                break;
            }

            List<String> topics = new ArrayList<>();
            List<String> groups = new ArrayList<>();
            List<AckRecord> acks = new ArrayList<>();
            long syntheticAckTime = System.currentTimeMillis();

            for (MessageRecord r : records) {
                if (r.getOffset() >= committedOffset) {
                    continue;
                }
                // Only backfill if no entry exists — never overwrite a real ACK
                if (ackStore.get(topic, group, r.getOffset()) == null) {
                    topics.add(topic);
                    groups.add(group);
                    acks.add(new AckRecord(r.getOffset(), syntheticAckTime));
                    seeded++;
                }
            }

            if (!topics.isEmpty()) {
                ackStore.putBatch(
                        topics.toArray(new String[0]),
                        groups.toArray(new String[0]),
                        acks.toArray(new AckRecord[0]));
            }

            offset = records.get(records.size() - 1).getOffset() + 1;
        }

        if (seeded > 0) {
            log.info("AckStoreSeeder: backfilled {} entries for topic={} group={} up to committedOffset={}",
                    seeded, topic, group, committedOffset);
        }
        return seeded;
    }
}
