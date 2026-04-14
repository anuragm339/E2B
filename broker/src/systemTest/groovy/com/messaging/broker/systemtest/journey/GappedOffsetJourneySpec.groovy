package com.messaging.broker.systemtest.journey

import com.messaging.broker.ack.RocksDbAckStore
import com.messaging.broker.consumer.ConsumerOffsetTracker
import com.messaging.broker.systemtest.support.BrokerSystemTestSupport
import com.messaging.broker.systemtest.support.TestRecordCollector
import com.messaging.common.api.StorageEngine
import io.micronaut.context.ApplicationContext
import spock.util.concurrent.PollingConditions

/**
 * Journey: consumer reconnects after offline period where records arrived at
 * widely-spaced (non-sequential) offsets.
 *
 * In production the broker receives records whose offsets can jump by thousands
 * between consecutive messages (e.g. 1 → 100 → 1000 → 50000).  The delivery
 * path uses getBatchFileRegion() which performs a binary-search for the first
 * index entry ≥ startOffset, so offset gaps are transparent to delivery.
 *
 * This spec verifies:
 * 1. A consumer correctly receives a batch whose records have large offset gaps.
 * 2. After disconnect + reconnect the consumer resumes from its committed offset,
 *    jumping the gap to the next available record (offset 50000).
 * 3. No re-delivery of records already ACKed before the disconnect.
 * 4. The committed offset in ConsumerOffsetTracker reflects the actual last
 *    record offset + 1, not a sequential counter.
 *
 * Note on storage.read() and gaps (fixed in BUG-1):
 *   storage.read() previously stopped at the first offset gap, returning only the
 *   record at offset 1 for a batch of [1, 100, 1000].  The fix in Segment.java
 *   makes readRecordAtOffset() return the record at the next available offset
 *   rather than null, so readWithSizeLimit() now traverses through gaps correctly.
 *   Tests therefore assert all three gapped records are in the RocksDB ack-store.
 */
class GappedOffsetJourneySpec extends BrokerSystemTestSupport {

    def "consumer reconnects with gapped offsets — resumes from committed offset, no duplicates"() {
        given: "collector is clean before the test"
        collector().reset()

        and: "consumer receives 3 records whose offsets have large gaps (1, 100, 1000)"
        cloudServer.enqueueMessages([
            [offset: 1L,    topic: 'prices-v1', partition: 0,
             msgKey: 'sparse-1',    eventType: 'MESSAGE', data: '{"i":1}'],
            [offset: 100L,  topic: 'prices-v1', partition: 0,
             msgKey: 'sparse-100',  eventType: 'MESSAGE', data: '{"i":100}'],
            [offset: 1000L, topic: 'prices-v1', partition: 0,
             msgKey: 'sparse-1000', eventType: 'MESSAGE', data: '{"i":1000}'],
        ])
        def firstBatch = collector().waitForRecords(3, 20)
        assert firstBatch.size() == 3
        assert firstBatch*.msgKey.toSet() == ['sparse-1', 'sparse-100', 'sparse-1000'].toSet()

        and: "data payloads are preserved through the pipe → storage → consumer path"
        def byKey = firstBatch.collectEntries { [it.msgKey, it] }
        byKey['sparse-1'].data    == '{"i":1}'
        byKey['sparse-100'].data  == '{"i":100}'
        byKey['sparse-1000'].data == '{"i":1000}'

        and: "committed offset in ConsumerOffsetTracker equals lastOffset + 1 (= 1001)"
        def offsetTracker = brokerCtx.getBean(ConsumerOffsetTracker)
        new PollingConditions(timeout: 10, delay: 0.3).eventually {
            // lastOffset=1000 → nextOffset stored = 1001
            assert offsetTracker.getOffset('system-test-group:prices-v1') == 1001L
        }

        and: "each gapped record is individually readable from StorageEngine at its exact offset"
        // storage.read() stops at gaps, so each read must start at the record's exact offset
        def storage = brokerCtx.getBean(StorageEngine)
        new PollingConditions(timeout: 5, delay: 0.2).eventually {
            assert storage.read('prices-v1', 0, 1L,    1).any { it.msgKey == 'sparse-1'    }
            assert storage.read('prices-v1', 0, 100L,  1).any { it.msgKey == 'sparse-100'  }
            assert storage.read('prices-v1', 0, 1000L, 1).any { it.msgKey == 'sparse-1000' }
        }

        when: "consumer disconnects"
        consumerCtx.close()

        and: "a record arrives at a much higher offset (50000) while the consumer is offline"
        cloudServer.enqueueMessages([
            [offset: 50000L, topic: 'prices-v1', partition: 0,
             msgKey: 'sparse-50000', eventType: 'MESSAGE', data: '{"i":50000}'],
        ])
        // Give broker time to poll and persist the offline record
        sleep(2000)

        and: "the offline record is persisted in StorageEngine at offset 50000"
        new PollingConditions(timeout: 5, delay: 0.2).eventually {
            assert storage.read('prices-v1', 0, 50000L, 1).any { it.msgKey == 'sparse-50000' }
        }

        and: "a fresh consumer context connects with the same group"
        def freshConsumerCtx = ApplicationContext.run(consumerProperties() as Map<String, Object>)
        triggerConsumerManagerStartup(freshConsumerCtx)
        sleep(2000)

        then: "fresh consumer receives exactly the record at offset 50000 — resumed from committed offset 1001"
        // getBatchFileRegion() finds the first record ≥ 1001 via binary search → offset 50000
        def freshCollector = freshConsumerCtx.getBean(TestRecordCollector)
        def newRecords = freshCollector.waitForRecords(1, 20)
        newRecords.size() == 1
        newRecords[0].msgKey == 'sparse-50000'
        newRecords[0].data   == '{"i":50000}'

        and: "no records from the first gapped batch are re-delivered (no cross-restart duplicates)"
        !newRecords.any { it.msgKey in ['sparse-1', 'sparse-100', 'sparse-1000'] }

        and: "committed offset advances to 50001 (= 50000 + 1) after the reconnect ACK"
        new PollingConditions(timeout: 10, delay: 0.3).eventually {
            assert offsetTracker.getOffset('system-test-group:prices-v1') == 50001L
        }

        and: "StorageEngine confirms the post-reconnect record is persisted at offset 50000"
        new PollingConditions(timeout: 5, delay: 0.2).eventually {
            def r = storage.read('prices-v1', 0, 50000L, 1).find { it.msgKey == 'sparse-50000' }
            assert r != null
            assert r.data == '{"i":50000}'
        }

        and: "RocksDB ack-store contains all four records — gapped batch writes now traverse gaps correctly (BUG-1 fixed)"
        def ackStore = brokerCtx.getBean(RocksDbAckStore)
        new PollingConditions(timeout: 10, delay: 0.3).eventually {
            assert ackStore.get('prices-v1', 'system-test-group', 'sparse-1')     != null
            assert ackStore.get('prices-v1', 'system-test-group', 'sparse-100')   != null
            assert ackStore.get('prices-v1', 'system-test-group', 'sparse-1000')  != null
            assert ackStore.get('prices-v1', 'system-test-group', 'sparse-50000') != null
        }

        cleanup:
        freshConsumerCtx?.close()
        // Restore the shared consumer context so the broker is not left connectionless
        consumerCtx = ApplicationContext.run(consumerProperties() as Map<String, Object>)
        triggerConsumerManagerStartup(consumerCtx)
        sleep(1000)
    }
}
