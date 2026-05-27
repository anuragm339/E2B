package com.messaging.broker.systemtest.journey

import com.messaging.broker.ack.RocksDbAckStore
import com.messaging.broker.consumer.ConsumerOffsetTracker
import com.messaging.broker.consumer.RefreshCoordinator
import com.messaging.broker.consumer.RefreshState
import com.messaging.broker.systemtest.support.BrokerSystemTestSupport
import com.messaging.broker.systemtest.support.TestRecordCollector
import io.micronaut.context.ApplicationContext
import spock.lang.Shared
import spock.util.concurrent.PollingConditions

/**
 * Journey: broker restarts while a refresh is actively REPLAYING, then recovery resumes and
 * the workflow completes after consumers reconnect.
 */
class RefreshRestartRecoveryJourneySpec extends BrokerSystemTestSupport {

    @Shared ApplicationContext consumerBCtx

    @Override protected String defaultGroup() { 'group-a' }

    def setupSpec() {
        consumerBCtx = newConsumerB(brokerTcpPort)
        triggerConsumerManagerStartup(consumerBCtx)
        awaitConsumerConnected(consumerBCtx)
    }

    def cleanupSpec() {
        consumerBCtx?.close()
    }

    def "broker restart during REPLAYING resumes refresh and post-refresh delivery"() {
        given: "both consumers received pre-refresh history"
        collector().reset()
        consumerBCtx.getBean(TestRecordCollector).reset()
        cloudServer.enqueueMessages((1..30).collect { i ->
            [offset: (long) i, topic: 'prices-v1', partition: 0,
             msgKey: "pre-${i}", eventType: 'MESSAGE', data: """{"v":${i}}"""]
        })
        new PollingConditions(timeout: 30, delay: 0.3).eventually {
            assert collector().getAll().size() >= 30
            assert consumerBCtx.getBean(TestRecordCollector).getAll().size() >= 30
        }

        and: "both groups have durable committed offsets"
        def offsetTracker = brokerCtx.getBean(ConsumerOffsetTracker)
        new PollingConditions(timeout: 10, delay: 0.3).eventually {
            assert offsetTracker.getOffset('group-a:prices-v1') >= 30
            assert offsetTracker.getOffset('group-b:prices-v1') >= 30
        }

        when: "a refresh starts and reaches REPLAYING"
        def coordinator = brokerCtx.getBean(RefreshCoordinator)
        coordinator.startRefresh('prices-v1')

        then: "both consumers receive RESET and the broker persists active refresh state"
        new PollingConditions(timeout: 20, delay: 0.3).eventually {
            assert collector().resetCount >= 1
            assert consumerBCtx.getBean(TestRecordCollector).resetCount >= 1
        }
        new PollingConditions(timeout: 10, delay: 0.3).eventually {
            assert coordinator.getRefreshStatus('prices-v1')?.state == RefreshState.REPLAYING
            def file = new File("${dataDir}/data-refresh-state.properties")
            assert file.exists()
            assert file.text.contains('prices-v1')
        }

        when: "broker is shut down mid-refresh and later restarted with the same dataDir"
        consumerCtx.close()
        consumerBCtx.close()
        closeBrokerContext(brokerCtx)

        int newBrokerPort = findFreePort()
        def restartedBrokerCtx = ApplicationContext.run(
                restartedBrokerProperties(newBrokerPort) as Map<String, Object>)
        triggerBrokerServiceStartup(restartedBrokerCtx)
        triggerPipeConnection(restartedBrokerCtx, cloudServer.baseUrl)

        and: "both consumers reconnect to the restarted broker"
        def restartedConsumerA = ApplicationContext.run(
                restartedConsumerAProperties(newBrokerPort) as Map<String, Object>)
        triggerConsumerManagerStartup(restartedConsumerA)
        def restartedConsumerB = newConsumerB(newBrokerPort)
        triggerConsumerManagerStartup(restartedConsumerB)
        awaitConsumerConnected(restartedConsumerA)
        awaitConsumerConnected(restartedConsumerB)

        then: "refresh recovery completes and both consumers receive READY"
        new PollingConditions(timeout: 40, delay: 0.5).eventually {
            assert restartedConsumerA.getBean(TestRecordCollector).readyCount >= 1
            assert restartedConsumerB.getBean(TestRecordCollector).readyCount >= 1
        }
        new PollingConditions(timeout: 40, delay: 0.5).eventually {
            def status = restartedBrokerCtx.getBean(RefreshCoordinator).getRefreshStatus('prices-v1')
            assert status == null || status.state == RefreshState.COMPLETED
        }

        when: "new records arrive after recovery"
        restartedConsumerA.getBean(TestRecordCollector).reset()
        restartedConsumerB.getBean(TestRecordCollector).reset()
        cloudServer.enqueueMessages([
            [offset: 60L, topic: 'prices-v1', partition: 0,
             msgKey: 'post-restart-refresh-60', eventType: 'MESSAGE', data: '{"post":60}'],
            [offset: 61L, topic: 'prices-v1', partition: 0,
             msgKey: 'post-restart-refresh-61', eventType: 'MESSAGE', data: '{"post":61}'],
        ])

        then: "both consumers receive post-refresh data and offsets/ACKs advance"
        new PollingConditions(timeout: 40, delay: 0.3).eventually {
            assert restartedConsumerA.getBean(TestRecordCollector).getAll().any { it.msgKey == 'post-restart-refresh-60' }
            assert restartedConsumerA.getBean(TestRecordCollector).getAll().any { it.msgKey == 'post-restart-refresh-61' }
            assert restartedConsumerB.getBean(TestRecordCollector).getAll().any { it.msgKey == 'post-restart-refresh-60' }
            assert restartedConsumerB.getBean(TestRecordCollector).getAll().any { it.msgKey == 'post-restart-refresh-61' }
        }

        and:
        def restartedOffsetTracker = restartedBrokerCtx.getBean(ConsumerOffsetTracker)
        new PollingConditions(timeout: 10, delay: 0.3).eventually {
            assert restartedOffsetTracker.getOffset('group-a:prices-v1') >= 61L
            assert restartedOffsetTracker.getOffset('group-b:prices-v1') >= 61L
        }

        and:
        def ackStore = restartedBrokerCtx.getBean(RocksDbAckStore)
        new PollingConditions(timeout: 10, delay: 0.3).eventually {
            assert ackStore.get('prices-v1', 'group-a', 60L) != null
            assert ackStore.get('prices-v1', 'group-a', 61L) != null
            assert ackStore.get('prices-v1', 'group-b', 60L) != null
            assert ackStore.get('prices-v1', 'group-b', 61L) != null
        }

        cleanup:
        restartedConsumerA?.close()
        restartedConsumerB?.close()
        brokerCtx = restartedBrokerCtx
        consumerCtx = restartedConsumerA
        consumerBCtx = restartedConsumerB
    }

    private ApplicationContext newConsumerB(int port) {
        ApplicationContext.run([
            'messaging.broker.host'  : '127.0.0.1',
            'messaging.broker.port'  : "${port}",
            'consumer.topics'        : 'prices-v1',
            'consumer.group'         : 'group-b',
            'consumer.legacy.enabled': 'false',
            'consumer.type'          : 'system-test-b',
            'micronaut.server.port'  : "${findFreePort()}",
            'broker.storage.dataDir' : "${dataDir}/consumer-b",
        ] as Map<String, Object>)
    }

    private Map<String, String> restartedBrokerProperties(int port) {
        def props = new LinkedHashMap<>(brokerProperties())
        props['broker.network.port'] = "${port}"
        props['micronaut.server.port'] = "${findFreePort()}"
        return props
    }

    private Map<String, String> restartedConsumerAProperties(int brokerPort) {
        [
            'messaging.broker.host'  : '127.0.0.1',
            'messaging.broker.port'  : "${brokerPort}",
            'consumer.topics'        : 'prices-v1',
            'consumer.group'         : 'group-a',
            'consumer.legacy.enabled': 'false',
            'consumer.type'          : 'system-test-a',
            'micronaut.server.port'  : "${findFreePort()}",
            'broker.storage.dataDir' : "${dataDir}/consumer-a-restarted",
        ]
    }

    protected static int findFreePort() {
        def s = new ServerSocket(0)
        try { s.localPort } finally { s.close() }
    }
}
