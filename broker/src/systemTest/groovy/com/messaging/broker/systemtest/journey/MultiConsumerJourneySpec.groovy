package com.messaging.broker.systemtest.journey

import com.messaging.broker.ack.RocksDbAckStore
import com.messaging.broker.consumer.ConsumerOffsetTracker
import com.messaging.broker.systemtest.support.BrokerSystemTestSupport
import com.messaging.broker.systemtest.support.TestRecordCollector
import io.micronaut.context.ApplicationContext
import spock.lang.Shared
import spock.util.concurrent.PollingConditions

/**
 * Journey: two independent consumers on different topics receive only their own records.
 *
 * consumerA subscribes to "prices-v1"   / "group-a"
 * consumerB subscribes to "ref-data-v5" / "group-b"
 *
 * Records for each topic must be delivered to the correct consumer only.
 */
class MultiConsumerJourneySpec extends BrokerSystemTestSupport {

    @Shared ApplicationContext consumerBCtx

    def setupSpec() {
        // Consumer B — different topic and group (brokerTcpPort and dataDir from super)
        consumerBCtx = ApplicationContext.run([
            'messaging.broker.host'  : '127.0.0.1',
            'messaging.broker.port'  : "${brokerTcpPort}",
            'consumer.topics'        : 'ref-data-v5',
            'consumer.group'         : 'group-b',
            'consumer.legacy.enabled': 'false',
            'consumer.type'          : 'system-test-b',
            'micronaut.server.port'  : "${findFreePort()}",
            'broker.storage.dataDir' : "${dataDir}/consumer-b",
        ] as Map<String, Object>)
        triggerConsumerManagerStartup(consumerBCtx)
        sleep(2000)
    }

    def cleanupSpec() {
        consumerBCtx?.close()
    }

    @Override
    protected String defaultTopic() { 'prices-v1' }

    @Override
    protected String defaultGroup() { 'group-a' }

    def "prices-v1 records go to group-a and ref-data-v5 records go to group-b"() {
        given:
        collector().reset()
        def collectorB = consumerBCtx.getBean(TestRecordCollector)
        collectorB.reset()

        cloudServer.enqueueMessages([
            [offset: 1L, topic: 'prices-v1',  partition: 0, msgKey: 'p-1', eventType: 'MESSAGE', data: '{"t":"p"}'],
            [offset: 2L, topic: 'prices-v1',  partition: 0, msgKey: 'p-2', eventType: 'MESSAGE', data: '{"t":"p"}'],
            [offset: 3L, topic: 'ref-data-v5',partition: 0, msgKey: 'r-1', eventType: 'MESSAGE', data: '{"t":"r"}'],
            [offset: 4L, topic: 'ref-data-v5',partition: 0, msgKey: 'r-2', eventType: 'MESSAGE', data: '{"t":"r"}'],
            [offset: 5L, topic: 'ref-data-v5',partition: 0, msgKey: 'r-3', eventType: 'MESSAGE', data: '{"t":"r"}'],
        ])

        when:
        def receivedA = collector().waitForRecords(2, 20)
        def receivedB = collectorB.waitForRecords(3, 20)

        then: "consumer A received exactly the prices-v1 records"
        receivedA*.msgKey.toSet() == ['p-1', 'p-2'].toSet()

        and: "consumer B received exactly the ref-data-v5 records"
        receivedB*.msgKey.toSet() == ['r-1', 'r-2', 'r-3'].toSet()

        and: "group-a committed offset for prices-v1 is tracked by ConsumerOffsetTracker"
        def offsetTracker = brokerCtx.getBean(ConsumerOffsetTracker)
        new PollingConditions(timeout: 10, delay: 0.3).eventually {
            assert offsetTracker.getOffset('group-a:prices-v1') > 0
        }

        and: "group-b committed offset for ref-data-v5 is tracked by ConsumerOffsetTracker"
        new PollingConditions(timeout: 10, delay: 0.3).eventually {
            assert offsetTracker.getOffset('group-b:ref-data-v5') > 0
        }

        and: "there is no cross-group contamination — group-a offset for ref-data-v5 is zero (never delivered)"
        offsetTracker.getOffset('group-a:ref-data-v5') == 0

        and: "there is no cross-group contamination — group-b offset for prices-v1 is zero (never delivered)"
        offsetTracker.getOffset('group-b:prices-v1') == 0

        and: "prices-v1 msgKeys are written to RocksDB under group-a (not group-b)"
        def ackStore = brokerCtx.getBean(RocksDbAckStore)
        new PollingConditions(timeout: 10, delay: 0.3).eventually {
            assert ackStore.get('prices-v1',  'group-a', 'p-1') != null
            assert ackStore.get('prices-v1',  'group-a', 'p-2') != null
        }

        and: "ref-data-v5 msgKeys are written to RocksDB under group-b (not group-a)"
        new PollingConditions(timeout: 10, delay: 0.3).eventually {
            assert ackStore.get('ref-data-v5', 'group-b', 'r-1') != null
            assert ackStore.get('ref-data-v5', 'group-b', 'r-2') != null
            assert ackStore.get('ref-data-v5', 'group-b', 'r-3') != null
        }

        and: "cross-group RocksDB keys are absent — group-b did not ACK prices-v1"
        ackStore.get('prices-v1', 'group-b', 'p-1') == null
        ackStore.get('prices-v1', 'group-b', 'p-2') == null

        and: "cross-group RocksDB keys are absent — group-a did not ACK ref-data-v5"
        ackStore.get('ref-data-v5', 'group-a', 'r-1') == null
    }

    private static int findFreePort() {
        def s = new ServerSocket(0)
        try { s.localPort } finally { s.close() }
    }
}
