package com.messaging.broker.consumer

import com.messaging.broker.metrics.BrokerMetrics
import com.messaging.broker.metrics.DataRefreshMetrics
import com.messaging.common.api.NetworkServer
import com.messaging.common.api.StorageEngine
import spock.lang.Specification
import spock.lang.TempDir

import java.nio.file.Path

class RemoteConsumerRegistrySpec extends Specification {

    @TempDir
    Path tempDir

    def "registerConsumer returns true for new and false for duplicate"() {
        given:
        def storage = Stub(StorageEngine)
        def server = Stub(NetworkServer)
        def offsetTracker = new ConsumerOffsetTracker(tempDir.toString())
        offsetTracker.init()
        def metrics = Mock(BrokerMetrics)
        def refreshMetrics = Mock(DataRefreshMetrics)
        def stateStore = new DeliveryStateStore(tempDir.toString())
        def registry = new RemoteConsumerRegistry(storage, server, offsetTracker,
            metrics, refreshMetrics, stateStore, 1024L, 1000L)

        when:
        def first = registry.registerConsumer("client-1", "topic", "group")
        def second = registry.registerConsumer("client-1", "topic", "group")

        then:
        first
        !second
        2 * metrics.updateConsumerOffset("client-1", "topic", "group", 0L)
        2 * metrics.updateConsumerLag("client-1", "topic", "group", 0L)

        cleanup:
        offsetTracker.shutdown()
        stateStore.shutdown()
    }

    def "unregisterConsumer removes all topics for client"() {
        given:
        def storage = Stub(StorageEngine)
        def server = Stub(NetworkServer)
        def offsetTracker = new ConsumerOffsetTracker(tempDir.toString())
        offsetTracker.init()
        def metrics = Mock(BrokerMetrics)
        def refreshMetrics = Mock(DataRefreshMetrics)
        def stateStore = new DeliveryStateStore(tempDir.toString())
        def registry = new RemoteConsumerRegistry(storage, server, offsetTracker,
            metrics, refreshMetrics, stateStore, 1024L, 1000L)

        and:
        registry.registerConsumer("client-1", "topicA", "group")
        registry.registerConsumer("client-1", "topicB", "group")
        registry.registerConsumer("client-2", "topicA", "group")

        when:
        def removed = registry.unregisterConsumer("client-1")

        then:
        removed == 2
        2 * metrics.removeConsumerMetrics("client-1", _ as String, "group")
        2 * refreshMetrics.removeConsumerTimingData(_ as String, "client-1")

        cleanup:
        offsetTracker.shutdown()
        stateStore.shutdown()
    }
}
