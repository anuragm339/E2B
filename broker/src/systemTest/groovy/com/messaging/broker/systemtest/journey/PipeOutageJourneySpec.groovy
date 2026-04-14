package com.messaging.broker.systemtest.journey

import com.messaging.broker.consumer.ConsumerOffsetTracker
import com.messaging.broker.systemtest.support.BrokerSystemTestSupport
import spock.util.concurrent.PollingConditions

/**
 * Journey: pipe outage — broker buffers messages and delivers them after resume.
 *
 * MockCloudServer.pausePipe() makes /pipe/poll return 204 (No Content) regardless
 * of what is in the queue.  Messages enqueued while the pipe is paused sit in
 * the server-side queue until resumePipe() is called, at which point the next
 * broker poll drains them normally.
 *
 * This spec verifies:
 * 1. Records enqueued during a pipe pause are NOT delivered to the consumer while
 *    the pipe is paused — the broker receives nothing during the outage window.
 * 2. The broker continues polling (returning 204) throughout the pause.
 * 3. After resumePipe(), all buffered records are delivered to the consumer.
 * 4. Pre-outage records are not re-delivered (no cross-boundary duplicates).
 * 5. The committed consumer offset advances correctly after the catch-up delivery.
 */
class PipeOutageJourneySpec extends BrokerSystemTestSupport {

    def "broker delivers buffered pipe messages after outage — no duplicates, no gaps"() {
        given: "collector is clean and consumer receives 3 pre-outage records"
        collector().reset()
        cloudServer.enqueueMessages((1..3).collect { i ->
            [offset: (long) i, topic: 'prices-v1', partition: 0,
             msgKey: "pre-outage-${i}", eventType: 'MESSAGE', data: """{"i":${i}}"""]
        })
        def preBatch = collector().waitForRecords(3, 20)
        assert preBatch.size() == 3
        assert preBatch*.msgKey.toSet() == ['pre-outage-1', 'pre-outage-2', 'pre-outage-3'].toSet()

        and: "pre-outage committed offset is persisted"
        def offsetTracker = brokerCtx.getBean(ConsumerOffsetTracker)
        new PollingConditions(timeout: 10, delay: 0.3).eventually {
            assert offsetTracker.getOffset('system-test-group:prices-v1') >= 3
        }
        collector().reset()

        when: "pipe is paused — all subsequent /pipe/poll calls return 204"
        cloudServer.pausePipe()
        int pollsAtPauseStart = cloudServer.pollCount

        and: "3 records are enqueued in MockCloudServer while the pipe is paused"
        cloudServer.enqueueMessages((4..6).collect { i ->
            [offset: (long) i, topic: 'prices-v1', partition: 0,
             msgKey: "during-outage-${i}", eventType: 'MESSAGE', data: """{"i":${i}}"""]
        })

        and: "broker polls for 2 seconds — all return 204 (paused)"
        sleep(2000)

        then: "no records are delivered during the pause window"
        collector().getAll().isEmpty()

        and: "broker was actively polling throughout the pause"
        cloudServer.pollCount > pollsAtPauseStart

        when: "pipe is resumed"
        cloudServer.resumePipe()

        then: "all 3 buffered records are delivered after resume — no gaps"
        new PollingConditions(timeout: 20, delay: 0.3).eventually {
            def received = collector().getAll()
            assert received.size() == 3
            assert received*.msgKey.toSet() == ['during-outage-4', 'during-outage-5', 'during-outage-6'].toSet()
        }

        and: "pre-outage records are not re-delivered (no cross-boundary duplicates)"
        !collector().getAll().any { it.msgKey.startsWith('pre-outage-') }

        and: "data payloads are intact for the catch-up batch"
        def byKey = collector().getAll().collectEntries { [it.msgKey, it] }
        byKey['during-outage-4'].data == '{"i":4}'
        byKey['during-outage-5'].data == '{"i":5}'
        byKey['during-outage-6'].data == '{"i":6}'

        and: "committed offset advances to at least 6 after the catch-up ACK"
        new PollingConditions(timeout: 10, delay: 0.3).eventually {
            assert offsetTracker.getOffset('system-test-group:prices-v1') >= 6
        }

        cleanup:
        // Ensure pipe is always resumed so it doesn't affect subsequent specs
        cloudServer.resumePipe()
    }
}
