package com.messaging.broker.systemtest.journey

import com.messaging.broker.ack.RocksDbAckStore
import com.messaging.broker.consumer.ConsumerOffsetTracker
import com.messaging.broker.systemtest.support.BrokerSystemTestSupport
import com.messaging.broker.systemtest.support.TestRecordCollector
import com.messaging.common.api.StorageEngine
import io.micronaut.context.ApplicationContext
import spock.util.concurrent.PollingConditions

/**
 * Journey: consumer reconnects and resumes from the committed offset.
 *
 * 1. First consumer context receives N records and commits offset.
 * 2. Context is closed (disconnect).
 * 3. More records are injected into the pipe.
 * 4. New consumer context starts with the same group/topic.
 * 5. New consumer receives only the records after the committed offset.
 *
 * Additionally verifies:
 * - The broker's ConsumerOffsetTracker holds the committed offset after the first batch.
 * - StorageEngine contains all 6 records (3 initial + 3 offline) with correct data.
 */
class ConsumerReconnectJourneySpec extends BrokerSystemTestSupport {

    def "consumer reconnect resumes from committed offset — no duplicate, no gap"() {
        given: "first consumer receives 3 records"
        cloudServer.enqueueMessages((1..3).collect { i ->
            [offset: (long) i, topic: 'prices-v1', partition: 0,
             msgKey: "rec-${i}", eventType: 'MESSAGE', data: """{"i":${i}}"""]
        })
        def firstBatch = collector().waitForRecords(3, 20)
        assert firstBatch.size() == 3

        and: "data payload is preserved for the first batch"
        firstBatch.sort { it.msgKey }
        firstBatch[0].data == '{"i":1}'
        firstBatch[1].data == '{"i":2}'
        firstBatch[2].data == '{"i":3}'

        and: "offset is committed in ConsumerOffsetTracker after ACK is processed"
        def offsetTracker = brokerCtx.getBean(ConsumerOffsetTracker)
        new PollingConditions(timeout: 10, delay: 0.3).eventually {
            // ACK is processed asynchronously; key format is group:topic
            assert offsetTracker.getOffset('system-test-group:prices-v1') > 0
        }

        and: "committed offset is flushed to consumer-offsets.properties on disk"
        new PollingConditions(timeout: 12, delay: 0.5).eventually {
            def file = new File("${dataDir}/consumer-offsets.properties")
            assert file.exists()
            def props = new Properties()
            props.load(file.newInputStream())
            assert Long.parseLong(props.getProperty('system-test-group:prices-v1', '0')) > 0
        }

        and: "first batch offsets (1-3) are in RocksDB ack-store"
        def ackStore = brokerCtx.getBean(RocksDbAckStore)
        new PollingConditions(timeout: 10, delay: 0.3).eventually {
            assert (1..3).every { i -> ackStore.get('prices-v1', 'system-test-group', (long) i) != null }
        }

        when: "consumer disconnects"
        consumerCtx.close()

        and: "3 more records arrive while consumer is offline"
        cloudServer.enqueueMessages((4..6).collect { i ->
            [offset: (long) i, topic: 'prices-v1', partition: 0,
             msgKey: "rec-${i}", eventType: 'MESSAGE', data: """{"i":${i}}"""]
        })
        // Give broker time to store the new records
        sleep(2000)

        and: "all 6 records are in StorageEngine"
        def storage = brokerCtx.getBean(StorageEngine)
        def allStored = storage.read('prices-v1', 0, 0, 100)
        allStored*.msgKey.toSet().containsAll((1..6).collect { "rec-${it}" })

        and: "stored data payloads are intact"
        def byKey = allStored.collectEntries { [it.msgKey, it] }
        (1..6).every { i -> byKey["rec-${i}"]?.data == """{"i":${i}}""" }

        and: "a fresh consumer context connects with the same group"
        def freshConsumerCtx = ApplicationContext.run(consumerProperties() as Map<String, Object>)
        triggerConsumerManagerStartup(freshConsumerCtx)
        sleep(2000)

        then: "fresh consumer receives only records 4-6 (after committed offset)"
        def freshCollector = freshConsumerCtx.getBean(TestRecordCollector)
        def newRecords = freshCollector.waitForRecords(3, 20)
        newRecords.size() == 3
        newRecords*.msgKey.toSet() == ['rec-4', 'rec-5', 'rec-6'].toSet()

        and: "no records from the first batch are re-delivered (no duplicates)"
        newRecords*.msgKey.every { !['rec-1', 'rec-2', 'rec-3'].contains(it) }

        and: "data payloads of fresh batch are correct"
        def freshByKey = newRecords.collectEntries { [it.msgKey, it] }
        freshByKey['rec-4'].data == '{"i":4}'
        freshByKey['rec-5'].data == '{"i":5}'
        freshByKey['rec-6'].data == '{"i":6}'

        and: "second batch offsets (4-6) are written to RocksDB after fresh consumer ACK"
        new PollingConditions(timeout: 10, delay: 0.3).eventually {
            assert (4..6).every { i -> ackStore.get('prices-v1', 'system-test-group', (long) i) != null }
        }

        cleanup:
        freshConsumerCtx?.close()
        // Re-open the original consumer context for subsequent tests
        consumerCtx = ApplicationContext.run(consumerProperties() as Map<String, Object>)
        triggerConsumerManagerStartup(consumerCtx)
        sleep(1000)
    }
}
