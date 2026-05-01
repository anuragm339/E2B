package com.messaging.broker.systemtest.journey

import com.messaging.broker.ack.RocksDbAckStore
import com.messaging.broker.consumer.ConsumerOffsetTracker
import com.messaging.broker.systemtest.support.BrokerSystemTestSupport
import com.messaging.common.api.StorageEngine
import spock.util.concurrent.PollingConditions

/**
 * Regression test: storage.read() 1MB internal size cap truncates RocksDB ack writes.
 *
 * Root cause (fixed in BatchAckService):
 *   When a consumer ACKs a batch, the broker re-reads that batch from storage to extract
 *   msgKeys for RocksDB.  storage.read() delegates to SegmentManager.readWithSizeLimit()
 *   which has a hardcoded 1MB cap.  A batch whose total record size exceeds 1MB causes
 *   storage.read() to return only a partial record list — the remaining records are never
 *   written to RocksDB, leaving them as phantom "unacknowledged" entries in reconciliation.
 *
 * Fix:
 *   BatchAckService now loops in chunks of ≤ 500 records, advancing the offset after each
 *   storage.read() call until the full batch offset range is covered.
 *
 * How this test forces the boundary:
 *   Each record carries ~512 KB of data.  The storage record size (data + key + framing)
 *   is therefore ~512 KB.  A pair of such records reaches ~1024 KB > 1MB, so the second
 *   record is always cut off by the size limiter in a single-call approach.  With three
 *   records the broker-to-consumer delivery batch is ~1.54 MB (< the 2 MB delivery cap),
 *   ensuring all three are delivered as ONE batch — making the RocksDB write the only
 *   stage that needs multiple reads.
 *
 * Timing guarantee:
 *   The delivery adaptive-polling min-delay is set to 500 ms (overridden below), while the
 *   pipe-poll interval is 200 ms.  All three records arrive from a single pipe poll within
 *   ~200 ms of the test enqueueing them.  The delivery fires ≥ 500 ms later, by which time
 *   all three records are already in storage, so the broker produces exactly ONE delivery
 *   batch containing all three records.
 */
class LargeRecordRocksDbAckSpec extends BrokerSystemTestSupport {

    // Slow down the adaptive delivery poller so all pipe-polled records are guaranteed to be
    // in storage before the first delivery batch is assembled.
    @Override
    protected Map<String, String> brokerProperties() {
        def props = new LinkedHashMap<>(super.brokerProperties())
        props['broker.consumer.adaptive-polling.min-delay-ms'] = '500'
        props['broker.consumer.adaptive-polling.max-delay-ms'] = '2000'
        // Raise the HTTP client content-length cap so the pipe can receive the ~1.54 MB response
        props['micronaut.http.client.max-content-length'] = '10485760'  // 10 MB
        return props
    }

    def "all records in a batch whose total size exceeds 1MB are written to RocksDB ack-store"() {
        given: """
            3 records each carrying ~512 KB of data.
            Per-record storage size  ≈ 29 B (framing) + 7 B (key) + 524_300 B (data) = 524_336 B.
            After record 1: cumSize = 524_336 < 1_048_576 (1 MB) — fits.
            After record 2: cumSize = 1_048_672 > 1_048_576 — exceeds 1 MB → storage.read() stops.
            Single-call path (old code) → only record 1 in RocksDB; records 2 & 3 missing.
            Loop path (new code)         → all 3 records written to RocksDB.
        """
        collector().reset()

        // Each 'data' value is ~512 KB of ASCII 'x' characters (valid JSON string).
        def largeData = 'x' * 524_300

        cloudServer.enqueueMessages([
            [offset: 7000L, topic: 'prices-v1', partition: 0,
             msgKey: 'lrg-a', eventType: 'MESSAGE', data: largeData],
            [offset: 7001L, topic: 'prices-v1', partition: 0,
             msgKey: 'lrg-b', eventType: 'MESSAGE', data: largeData],
            [offset: 7002L, topic: 'prices-v1', partition: 0,
             msgKey: 'lrg-c', eventType: 'MESSAGE', data: largeData],
        ])

        when: "broker polls pipe, stores all 3 records, then delivers them as one batch to the consumer"
        // 60 s timeout: the records are ~1.54 MB total; delivery and ACK round-trip may take time
        def received = collector().waitForRecords(3, 60)

        then: "consumer receives all 3 large records"
        received.size() == 3
        received*.msgKey.toSet() == ['lrg-a', 'lrg-b', 'lrg-c'].toSet()

        and: "all 3 records are persisted in StorageEngine (sanity check — pipe + storage path correct)"
        def storage = brokerCtx.getBean(StorageEngine)
        new PollingConditions(timeout: 10, delay: 0.3).eventually {
            assert storage.read('prices-v1', 0, 7000L, 1).any { it.msgKey == 'lrg-a' }
            assert storage.read('prices-v1', 0, 7001L, 1).any { it.msgKey == 'lrg-b' }
            assert storage.read('prices-v1', 0, 7002L, 1).any { it.msgKey == 'lrg-c' }
        }

        and: "committed offset advances beyond 7002 (ACK was processed)"
        def offsetTracker = brokerCtx.getBean(ConsumerOffsetTracker)
        new PollingConditions(timeout: 15, delay: 0.3).eventually {
            assert offsetTracker.getOffset('system-test-group:prices-v1') >= 7003L
        }

        and: """
            All 3 msgKeys are in RocksDB ack-store.
            This assertion fails WITHOUT the loop fix: the async RocksDB write calls
            storage.read() once, gets only 'lrg-a' (second record would exceed 1 MB), and
            writes only 1 record.  With the loop fix all 3 are written.
        """
        def ackStore = brokerCtx.getBean(RocksDbAckStore)
        new PollingConditions(timeout: 20, delay: 0.5).eventually {
            assert ackStore.get('prices-v1', 'system-test-group', 'lrg-a') != null, 'lrg-a missing from RocksDB'
            assert ackStore.get('prices-v1', 'system-test-group', 'lrg-b') != null, 'lrg-b missing from RocksDB'
            assert ackStore.get('prices-v1', 'system-test-group', 'lrg-c') != null, 'lrg-c missing from RocksDB'
        }
    }
}
