package com.messaging.broker.systemtest.journey

import com.messaging.broker.consumer.ConsumerOffsetTracker
import com.messaging.broker.systemtest.support.BrokerSystemTestSupport
import com.messaging.broker.systemtest.support.TestRecordCollector
import io.micronaut.context.ApplicationContext
import spock.util.concurrent.PollingConditions

/**
 * Journey: full broker restart — pipe offset and consumer offsets survive.
 *
 * This spec verifies that after a hard broker shutdown and restart (same dataDir),
 * the broker resumes the pipe from its persisted offset and the consumer resumes
 * from its committed offset without receiving any duplicates.
 *
 * Scenario:
 * 1. Consumer receives 5 pre-restart records (offsets 1-5).
 * 2. Committed consumer offset and pipe poll offset are flushed to disk.
 * 3. Consumer context is closed, then broker context is closed.
 * 4. New broker context starts with the SAME dataDir — loads both persisted offsets.
 * 5. 3 post-restart records are enqueued in MockCloudServer (offsets 6-8).
 * 6. New consumer context connects to the restarted broker.
 * 7. Consumer receives exactly the 3 new records — no re-delivery of the first 5.
 * 8. Committed offset advances to ≥ 8 after ACK.
 */
class BrokerRestartJourneySpec extends BrokerSystemTestSupport {

    def "broker restart — pipe offset and consumer offsets survive, no duplicate delivery"() {
        given: "collector is clean and consumer receives 5 pre-restart records"
        collector().reset()
        cloudServer.enqueueMessages((1..5).collect { i ->
            [offset: (long) i, topic: 'prices-v1', partition: 0,
             msgKey: "pre-restart-${i}", eventType: 'MESSAGE', data: """{"i":${i}}"""]
        })
        def preBatch = collector().waitForRecords(5, 20)
        assert preBatch.size() == 5
        assert preBatch*.msgKey.toSet() == (1..5).collect { "pre-restart-${it}" }.toSet()

        and: "all 5 records are ACKed — committed offset in memory is 6 (= lastOffset 5 + 1)"
        def offsetTracker = brokerCtx.getBean(ConsumerOffsetTracker)
        new PollingConditions(timeout: 10, delay: 0.3).eventually {
            assert offsetTracker.getOffset('system-test-group:prices-v1') >= 6
        }

        and: "pipe poll offset has been written to disk (pipe persists after each poll)"
        new PollingConditions(timeout: 10, delay: 0.3).eventually {
            def file = new File("${dataDir}/pipe-offset.properties")
            assert file.exists()
            def props = new Properties()
            props.load(file.newInputStream())
            // Pipe offset stores the actual offset of the last polled message (= 5),
            // not nextOffset — so the threshold is >= 5, not >= 6.
            assert Long.parseLong(props.getProperty('pipe.current.offset', '0')) >= 5
        }

        when: "consumer context is closed, then broker context is closed (simulating restart)"
        consumerCtx.close()
        brokerCtx.close()

        and: "broker is restarted with the same dataDir on a new port"
        int newBrokerPort = findFreePort()
        def restartedBrokerCtx = ApplicationContext.run(
                restartedBrokerProperties(newBrokerPort) as Map<String, Object>)
        triggerBrokerServiceStartup(restartedBrokerCtx)
        triggerPipeConnection(restartedBrokerCtx, cloudServer.baseUrl)

        and: "3 post-restart records are enqueued in MockCloudServer"
        cloudServer.enqueueMessages((6..8).collect { i ->
            [offset: (long) i, topic: 'prices-v1', partition: 0,
             msgKey: "post-restart-${i}", eventType: 'MESSAGE', data: """{"i":${i}}"""]
        })

        and: "a fresh consumer reconnects to the restarted broker"
        def restartedConsumerCtx = ApplicationContext.run(
                restartedConsumerProperties(newBrokerPort) as Map<String, Object>)
        triggerConsumerManagerStartup(restartedConsumerCtx)
        sleep(2000)

        then: "consumer receives exactly the 3 post-restart records — no re-delivery"
        def freshCollector = restartedConsumerCtx.getBean(TestRecordCollector)
        def postBatch = freshCollector.waitForRecords(3, 20)
        postBatch.size() == 3
        postBatch*.msgKey.toSet() == ['post-restart-6', 'post-restart-7', 'post-restart-8'].toSet()

        and: "pre-restart records are NOT re-delivered (offset restored correctly)"
        !postBatch.any { it.msgKey.startsWith('pre-restart-') }

        and: "data payloads are intact for the post-restart batch"
        def byKey = postBatch.collectEntries { [it.msgKey, it] }
        byKey['post-restart-6'].data == '{"i":6}'
        byKey['post-restart-7'].data == '{"i":7}'
        byKey['post-restart-8'].data == '{"i":8}'

        and: "committed offset advances to ≥ 8 after the post-restart ACK"
        def newOffsetTracker = restartedBrokerCtx.getBean(ConsumerOffsetTracker)
        new PollingConditions(timeout: 10, delay: 0.3).eventually {
            assert newOffsetTracker.getOffset('system-test-group:prices-v1') >= 8
        }

        and: "pipe poll offset was restored — new broker did NOT re-poll from offset 0"
        // The broker loaded pipe-offset.properties (≥ 5) on startup, so it asked
        // MockCloudServer for records starting at offset ≥ 5, not from 0.
        // If it had polled from 0, the pre-restart records would be re-stored and
        // re-delivered, which the assertion above already rules out.
        def pipeOffsetFile = new File("${dataDir}/pipe-offset.properties")
        pipeOffsetFile.exists()
        def pipeProps = new Properties()
        pipeProps.load(pipeOffsetFile.newInputStream())
        Long.parseLong(pipeProps.getProperty('pipe.current.offset', '0')) >= 8

        cleanup:
        restartedConsumerCtx?.close()
        // Reassign shared fields so cleanupSpec() has valid (null-safe) refs to close
        brokerCtx = restartedBrokerCtx
        consumerCtx = restartedConsumerCtx
    }

    // ── Helpers ───────────────────────────────────────────────────────────────

    private Map<String, String> restartedBrokerProperties(int port) {
        def props = new LinkedHashMap<>(brokerProperties())
        props['broker.network.port'] = "${port}"
        props['micronaut.server.port'] = "${findFreePort()}"
        return props
    }

    private Map<String, String> restartedConsumerProperties(int brokerPort) {
        def props = new LinkedHashMap<>(consumerProperties())
        props['messaging.broker.port'] = "${brokerPort}"
        props['micronaut.server.port'] = "${findFreePort()}"
        return props
    }

    private static int findFreePort() {
        def s = new ServerSocket(0)
        try { s.localPort } finally { s.close() }
    }
}
