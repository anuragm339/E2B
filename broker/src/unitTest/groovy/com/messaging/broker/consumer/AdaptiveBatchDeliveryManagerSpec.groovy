package com.messaging.broker.consumer

import com.messaging.broker.metrics.BrokerMetrics
import com.messaging.broker.refresh.DataRefreshContext
import com.messaging.broker.refresh.DataRefreshManager
import com.messaging.broker.refresh.DataRefreshState
import com.messaging.common.api.StorageEngine
import spock.lang.Specification

class AdaptiveBatchDeliveryManagerSpec extends Specification {

    def "skips delivery when topic is in RESET_SENT state"() {
        given:
        def consumerRegistry = Mock(RemoteConsumerRegistry)
        def storage = Mock(StorageEngine)
        def stateStore = Mock(DeliveryStateStore)
        def scheduler = Mock(TopicFairScheduler)
        def metrics = Mock(BrokerMetrics)
        def manager = new AdaptiveBatchDeliveryManager(consumerRegistry, storage, stateStore, scheduler, metrics, 1024L)

        and:
        def refreshManager = Mock(DataRefreshManager)
        def refreshContext = new DataRefreshContext("topic", ["group:topic"] as Set)
        refreshContext.setState(DataRefreshState.RESET_SENT)
        manager.setDataRefreshManager(refreshManager)
        refreshManager.getRefreshStatus("topic") >> refreshContext

        and:
        def consumer = new RemoteConsumerRegistry.RemoteConsumer("client", "topic", "group")

        when:
        def result = invokeTryDeliver(manager, consumer)

        then:
        !result
        1 * metrics.recordAdaptivePollSkipped("topic")
        0 * consumerRegistry.deliverBatch(_, _)
    }

    def "skips delivery when no new data is available"() {
        given:
        def consumerRegistry = Mock(RemoteConsumerRegistry)
        def storage = Mock(StorageEngine)
        def stateStore = Mock(DeliveryStateStore)
        def scheduler = Mock(TopicFairScheduler)
        def metrics = Mock(BrokerMetrics)
        def manager = new AdaptiveBatchDeliveryManager(consumerRegistry, storage, stateStore, scheduler, metrics, 1024L)

        and:
        def consumer = new RemoteConsumerRegistry.RemoteConsumer("client", "topic", "group")
        consumer.setCurrentOffset(5L)
        storage.getCurrentOffset("topic", 0) >> 5L

        when:
        def result = invokeTryDeliver(manager, consumer)

        then:
        !result
        1 * metrics.recordAdaptivePollSkipped("topic")
        0 * consumerRegistry.deliverBatch(_, _)
    }

    def "delivers when new data is available and delivery succeeds"() {
        given:
        def consumerRegistry = Mock(RemoteConsumerRegistry)
        def storage = Mock(StorageEngine)
        def stateStore = Mock(DeliveryStateStore)
        def scheduler = Mock(TopicFairScheduler)
        def metrics = Mock(BrokerMetrics)
        def manager = new AdaptiveBatchDeliveryManager(consumerRegistry, storage, stateStore, scheduler, metrics, 1024L)

        and:
        def consumer = new RemoteConsumerRegistry.RemoteConsumer("client", "topic", "group")
        consumer.setCurrentOffset(5L)
        storage.getCurrentOffset("topic", 0) >> 10L
        consumerRegistry.deliverBatch(consumer, 1024L) >> true

        when:
        def result = invokeTryDeliver(manager, consumer)

        then:
        result
        1 * metrics.recordAdaptivePollSuccess("topic")
    }

    def "delivery failure records skipped metric"() {
        given:
        def consumerRegistry = Mock(RemoteConsumerRegistry)
        def storage = Mock(StorageEngine)
        def stateStore = Mock(DeliveryStateStore)
        def scheduler = Mock(TopicFairScheduler)
        def metrics = Mock(BrokerMetrics)
        def manager = new AdaptiveBatchDeliveryManager(consumerRegistry, storage, stateStore, scheduler, metrics, 1024L)

        and:
        def consumer = new RemoteConsumerRegistry.RemoteConsumer("client", "topic", "group")
        consumer.setCurrentOffset(0L)
        storage.getCurrentOffset("topic", 0) >> 5L
        consumerRegistry.deliverBatch(consumer, 1024L) >> false

        when:
        def result = invokeTryDeliver(manager, consumer)

        then:
        !result
        1 * metrics.recordAdaptivePollSkipped("topic")
    }

    private static boolean invokeTryDeliver(AdaptiveBatchDeliveryManager manager,
                                            RemoteConsumerRegistry.RemoteConsumer consumer) {
        def method = AdaptiveBatchDeliveryManager.class.getDeclaredMethod(
            "tryDeliverBatch", RemoteConsumerRegistry.RemoteConsumer.class)
        method.setAccessible(true)
        return (boolean) method.invoke(manager, consumer)
    }
}
