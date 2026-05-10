package com.messaging.broker.systemtest.journey

import com.messaging.broker.ack.RocksDbAckStore
import com.messaging.broker.consumer.ConsumerOffsetTracker
import com.messaging.broker.systemtest.support.BrokerSystemTestSupport
import com.messaging.broker.systemtest.support.TestRecordCollector
import io.micronaut.context.ApplicationContext
import spock.util.concurrent.PollingConditions

/**
 * Journey: startup ACK repair backfills missing RocksDB entries from committed offsets.
 *
 * This simulates the historical failure mode where consumer offsets were already durable but
 * RocksDB ACK records were missing. After restart, AckStoreSeeder should rebuild those ACK
 * entries before any fresh delivery occurs.
 */
class AckStoreSeederRestartJourneySpec extends BrokerSystemTestSupport {

    def "broker restart backfills missing RocksDB ACK entries from committed offsets"() {
        given: "5 records were delivered and committed before restart"
        collector().reset()
        cloudServer.enqueueMessages((1..5).collect { i ->
            [offset: (long) i, topic: 'prices-v1', partition: 0,
             msgKey: "seed-${i}", eventType: 'MESSAGE', data: """{"i":${i}}"""]
        })
        def firstBatch = collector().waitForRecords(5, 20)
        assert firstBatch.size() == 5

        and: "committed consumer offset is durable"
        def offsetTracker = brokerCtx.getBean(ConsumerOffsetTracker)
        new PollingConditions(timeout: 10, delay: 0.3).eventually {
            assert offsetTracker.getOffset('system-test-group:prices-v1') >= 6L
        }

        and: "RocksDB originally contains ACK entries for all 5 offsets"
        def ackStore = brokerCtx.getBean(RocksDbAckStore)
        new PollingConditions(timeout: 10, delay: 0.3).eventually {
            assert (1..5).every { i -> ackStore.get('prices-v1', 'system-test-group', i as long) != null }
        }

        when: "the ACK store is wiped for the topic/group but committed offsets remain"
        ackStore.clearByTopicAndGroup('prices-v1', 'system-test-group')

        then: "ACK entries are genuinely gone before restart"
        (1..5).every { i -> ackStore.get('prices-v1', 'system-test-group', i as long) == null }

        when: "consumer and broker are restarted with the same dataDir"
        consumerCtx.close()
        brokerCtx.close()

        int newBrokerPort = findFreePort()
        def restartedBrokerCtx = ApplicationContext.run(
                restartedBrokerProperties(newBrokerPort) as Map<String, Object>)
        triggerBrokerServiceStartup(restartedBrokerCtx)
        triggerPipeConnection(restartedBrokerCtx, cloudServer.baseUrl)

        then: "startup seeder backfills the missing ACK entries before any new consumer connects"
        def restartedAckStore = restartedBrokerCtx.getBean(RocksDbAckStore)
        new PollingConditions(timeout: 10, delay: 0.3).eventually {
            assert (1..5).every { i -> restartedAckStore.get('prices-v1', 'system-test-group', i as long) != null }
        }

        when: "a fresh consumer reconnects and new records are published"
        def restartedConsumerCtx = ApplicationContext.run(
                restartedConsumerProperties(newBrokerPort) as Map<String, Object>)
        triggerConsumerManagerStartup(restartedConsumerCtx)
        sleep(2000)
        cloudServer.enqueueMessages((6..7).collect { i ->
            [offset: (long) i, topic: 'prices-v1', partition: 0,
             msgKey: "post-seed-${i}", eventType: 'MESSAGE', data: """{"i":${i}}"""]
        })

        then: "only the new records are delivered after restart"
        def freshCollector = restartedConsumerCtx.getBean(TestRecordCollector)
        def postBatch = freshCollector.waitForRecords(2, 20)
        postBatch*.msgKey.toSet() == ['post-seed-6', 'post-seed-7'] as Set
        !postBatch.any { it.msgKey.startsWith('seed-') }

        cleanup:
        restartedConsumerCtx?.close()
        brokerCtx = restartedBrokerCtx
        consumerCtx = restartedConsumerCtx
    }

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

    protected static int findFreePort() {
        def s = new ServerSocket(0)
        try { s.localPort } finally { s.close() }
    }
}
