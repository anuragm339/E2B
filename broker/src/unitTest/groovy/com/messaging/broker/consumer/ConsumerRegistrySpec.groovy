package com.messaging.broker.consumer

import com.fasterxml.jackson.databind.ObjectMapper
import com.messaging.broker.legacy.LegacyConsumerDeliveryManager
import com.messaging.broker.legacy.MergedBatch
import com.messaging.broker.monitoring.BrokerMetrics
import com.messaging.broker.monitoring.DataRefreshMetrics
import com.messaging.common.api.NetworkServer
import com.messaging.common.api.StorageEngine
import com.messaging.common.model.BrokerMessage
import com.messaging.common.model.EventType
import com.messaging.common.model.MessageRecord
import io.micrometer.core.instrument.Timer
import spock.lang.Specification

import java.time.Instant
import java.util.concurrent.CompletableFuture
import java.util.concurrent.Executors
import java.util.concurrent.ScheduledExecutorService

class ConsumerRegistrySpec extends Specification {

    ConsumerRegistrationService registrationService = Mock()
    ConsumerReadinessService readinessService = Mock()
    ConsumerDeliveryService deliveryService = Mock()
    ConsumerAckService ackService = Mock()
    ConsumerStateService stateService = Mock()
    LegacyConsumerDeliveryManager legacyDeliveryManager = Mock()
    PendingAckStore pendingAckStore = Mock()
    BrokerMetrics metrics = Mock()
    DataRefreshMetrics dataRefreshMetrics = Mock()
    NetworkServer server = Mock()
    StorageEngine storage = Mock()
    ConsumerOffsetTracker offsetTracker = Mock()
    ScheduledExecutorService scheduler = Executors.newSingleThreadScheduledExecutor()

    ConsumerRegistry registry = new ConsumerRegistry(
            registrationService,
            readinessService,
            deliveryService,
            ackService,
            stateService,
            legacyDeliveryManager,
            pendingAckStore,
            metrics,
            dataRefreshMetrics,
            server,
            storage,
            offsetTracker,
            scheduler,
            100
    )

    def cleanup() {
        scheduler.shutdownNow()
    }

    def "registerConsumer wires adaptive delivery manager only for new consumers"() {
        given:
        def adaptive = Mock(AdaptiveBatchDeliveryManager)
        def consumer = new RemoteConsumer("client-1", "prices-v1", "group-a")
        registry.setAdaptiveBatchDeliveryManager(adaptive)
        registrationService.registerConsumer("client-1", "prices-v1", "group-a", false, "trace-1") >>
                ConsumerRegistrationService.RegistrationResult.newRegistration(consumer, 0L)
        registrationService.registerConsumer("client-1", "prices-v1", "group-a", false, "trace-2") >>
                ConsumerRegistrationService.RegistrationResult.duplicate(consumer)

        when:
        def first = registry.registerConsumer("client-1", "prices-v1", "group-a", false, "trace-1")
        def second = registry.registerConsumer("client-1", "prices-v1", "group-a", false, "trace-2")

        then:
        first
        !second
        1 * adaptive.registerConsumer(consumer)
    }

    def "deliverBatch routes modern and legacy consumers through correct delivery paths"() {
        given:
        def modern = new RemoteConsumer("modern-client", "prices-v1", "group-a")
        def legacy = new RemoteConsumer("legacy-client", "prices-v1", "svc", true)
        deliveryService.deliverBatch(modern, 1024) >> ConsumerDeliveryService.DeliveryResult.success()
        readinessService.isLegacyConsumerReady("legacy-client") >> true
        pendingAckStore.getPendingBatch("legacy-client") >> null
        registrationService.getConsumersByClient("legacy-client") >> [legacy]
        legacyDeliveryManager.buildMergedBatch(["prices-v1"], "svc", 1024) >> new MergedBatch()

        expect:
        registry.deliverBatch(modern, 1024)
        !registry.deliverBatch(legacy, 1024)
    }

    def "startup ready senders schedule retries and swallow send failures"() {
        given:
        server.send("legacy-client", _ as BrokerMessage) >> CompletableFuture.completedFuture(null)
        server.send("modern-client", _ as BrokerMessage) >> CompletableFuture.failedFuture(new RuntimeException("boom"))

        when:
        registry.sendStartupReadyToLegacyConsumer("legacy-client")
        registry.sendStartupReadyToModernConsumer("modern-client", "prices-v1", "group-a")

        then:
        1 * readinessService.scheduleReadyRetry("legacy-client", null, null, 0)
        0 * readinessService.scheduleReadyRetry("modern-client", "prices-v1", "group-a", 0)
    }

    def "sendStartupReadyToAllConsumers routes by protocol"() {
        given:
        def legacy = new RemoteConsumer("legacy-client", "prices-v1", "svc", true)
        def modern = new RemoteConsumer("modern-client", "orders-v1", "group-a")
        registrationService.getAllConsumers() >> [legacy, modern]
        server.send("legacy-client", _ as BrokerMessage) >> CompletableFuture.completedFuture(null)
        server.send("modern-client", _ as BrokerMessage) >> CompletableFuture.completedFuture(null)

        when:
        registry.sendStartupReadyToAllConsumers()

        then:
        1 * readinessService.scheduleReadyRetry("legacy-client", null, null, 0)
        1 * readinessService.scheduleReadyRetry("modern-client", "orders-v1", "group-a", 0)
    }

    def "broadcast and targeted refresh ready methods send to consumers and tolerate failures"() {
        given:
        registrationService.getConsumersByTopic("prices-v1") >> [
                new RemoteConsumer("client-1", "prices-v1", "group-a"),
                new RemoteConsumer("client-2", "prices-v1", "group-b")
        ]
        server.send("client-1", _ as BrokerMessage) >> CompletableFuture.completedFuture(null)
        server.send("client-2", _ as BrokerMessage) >> CompletableFuture.failedFuture(new RuntimeException("boom"))

        when:
        registry.broadcastResetToTopic("prices-v1")
        registry.broadcastReadyToTopic("prices-v1")
        registry.sendRefreshReadyToConsumer("client-2", "prices-v1")

        then:
        5 * server.send(_ as String, _ as BrokerMessage)
    }

    def "legacy merged delivery blocks for readiness pending ack and missing topics"() {
        given:
        readinessService.isLegacyConsumerReady("legacy-client") >> false >> true >> true
        pendingAckStore.getPendingBatch("legacy-client") >> null >> new MergedBatch() >> null
        registrationService.getConsumersByClient("legacy-client") >> []

        expect:
        !registry.deliverMergedBatchToLegacy("legacy-client", "svc", 1024)
        !registry.deliverMergedBatchToLegacy("legacy-client", "svc", 1024)
        !registry.deliverMergedBatchToLegacy("legacy-client", "svc", 1024)
    }

    def "legacy merged delivery cleans pending state when send fails"() {
        given:
        def legacy = new RemoteConsumer("legacy-client", "prices-v1", "svc", true)
        def batch = new MergedBatch()
        batch.add("prices-v1", new MessageRecord(1L, "prices-v1", 0, "k1", EventType.MESSAGE, "v1", Instant.now()))
        readinessService.isLegacyConsumerReady("legacy-client") >> true
        pendingAckStore.getPendingBatch("legacy-client") >> null
        registrationService.getConsumersByClient("legacy-client") >> [legacy]
        legacyDeliveryManager.buildMergedBatch(["prices-v1"], "svc", 1024) >> batch
        metrics.startConsumerDeliveryTimer() >> Mock(Timer.Sample)
        server.send("legacy-client", _ as BrokerMessage) >> CompletableFuture.failedFuture(new RuntimeException("boom"))

        when:
        def delivered = registry.deliverMergedBatchToLegacy("legacy-client", "svc", 1024)

        then:
        !delivered
        1 * pendingAckStore.putPendingBatchIfAbsent("legacy-client", batch) >> true
        1 * pendingAckStore.recordSendTime("legacy-client", _ as Long)
        1 * pendingAckStore.startTimer("legacy-client", _ as Timer.Sample)
        1 * pendingAckStore.removePendingBatch("legacy-client")
        1 * pendingAckStore.removeTimer("legacy-client")
        1 * pendingAckStore.removeClient("legacy-client")
        1 * metrics.recordConsumerFailure("legacy-client", "prices-v1", "svc")
    }

    def "legacy merged delivery records refresh metrics on successful replay send"() {
        given:
        def legacy = new RemoteConsumer("legacy-client", "prices-v1", "svc", true)
        def batch = new MergedBatch()
        batch.add("prices-v1", new MessageRecord(1L, "prices-v1", 0, "k1", EventType.MESSAGE, "v1", Instant.now()))
        def refreshCoordinator = Mock(RefreshCoordinator)
        def refreshContext = new RefreshContext("prices-v1", [] as Set)
        refreshContext.setState(RefreshState.REPLAYING)
        refreshContext.setRefreshId("refresh-1")
        registry.setRefreshCoordinator(refreshCoordinator)

        readinessService.isLegacyConsumerReady("legacy-client") >> true
        pendingAckStore.getPendingBatch("legacy-client") >> null >>  batch >> batch
        pendingAckStore.putPendingBatchIfAbsent("legacy-client", batch) >> true
        pendingAckStore.getSendTime("legacy-client") >> 123L
        registrationService.getConsumersByClient("legacy-client") >> [legacy]
        legacyDeliveryManager.buildMergedBatch(["prices-v1"], "svc", 1024) >> batch
        metrics.startConsumerDeliveryTimer() >> Mock(Timer.Sample)
        server.send("legacy-client", _ as BrokerMessage) >> CompletableFuture.completedFuture(null)
        refreshCoordinator.getRefreshStatus("prices-v1") >> refreshContext

        when:
        def delivered = registry.deliverMergedBatchToLegacy("legacy-client", "svc", 1024)

        then:
        delivered
        1 * metrics.recordBatchSize(1)
        1 * metrics.startPendingAck("prices-v1", "svc")
        1 * dataRefreshMetrics.recordDataTransferred("prices-v1", "svc", batch.getBytesPerTopic().get("prices-v1"), 1, "refresh-1", "LOCAL")
    }

    def "sendReadyToAckedConsumers only sends to acked group topic pairs"() {
        given:
        registrationService.getConsumersByTopic("prices-v1") >> [
                new RemoteConsumer("client-1", "prices-v1", "group-a"),
                new RemoteConsumer("client-2", "prices-v1", "group-b")
        ]
        registrationService.getConsumersByClient("client-1") >> [new RemoteConsumer("client-1", "prices-v1", "group-a")]
        registrationService.getConsumersByClient("client-2") >> [new RemoteConsumer("client-2", "prices-v1", "group-b")]
        server.send("client-1", _ as BrokerMessage) >> CompletableFuture.completedFuture(null)

        when:
        registry.sendReadyToAckedConsumers("prices-v1", ["group-a:prices-v1"] as Set)

        then:
        1 * server.send("client-1", _ as BrokerMessage)
        0 * server.send("client-2", _ as BrokerMessage)
    }

    def "unregister clears delivery readiness ack and adaptive state"() {
        given:
        def legacy = new RemoteConsumer("client-1", "prices-v1", "group-a", true)
        def modern = new RemoteConsumer("client-1", "orders-v1", "group-b")
        registrationService.getConsumersByClient("client-1") >> [legacy, modern]
        registrationService.unregisterConsumer("client-1") >> 2
        def adaptive = Mock(AdaptiveBatchDeliveryManager)
        registry.setAdaptiveBatchDeliveryManager(adaptive)

        when:
        def removed = registry.unregisterConsumer("client-1")

        then:
        removed == 2
        1 * stateService.removeDeliveryState(_)
        1 * stateService.removeDeliveryState(_)
        1 * metrics.completePendingAck("prices-v1", "group-a")
        1 * metrics.completePendingAck("orders-v1", "group-b")
        1 * readinessService.removeClient("client-1")
        1 * ackService.clearPendingAcks("client-1")
        1 * adaptive.removeConsumer("client-1")
    }

    def "isLegacy resetConsumerOffset and consumer grouping utilities behave correctly"() {
        given:
        def consumer = new RemoteConsumer("client-1", "prices-v1", "group-a", true)
        registrationService.getConsumer(_) >> Optional.of(consumer) >> Optional.of(consumer) >> Optional.empty()
        registrationService.getConsumersByClient("client-1") >> [consumer]
        registrationService.getConsumersByTopic("prices-v1") >> [consumer]

        expect:
        registry.isLegacyConsumer("client-1:prices-v1:group-a")
        registry.getConsumerGroupTopic("client-1", "prices-v1") == "group-a:prices-v1"
        registry.getGroupTopicIdentifiers("prices-v1") == ["group-a:prices-v1"] as Set
        registry.getConsumerGroupTopicPairs("prices-v1")*.groupTopic == ["group-a:prices-v1"]
        registry.getLegacyConsumersForClient("client-1")*.group == ["group-a"]

        when:
        registry.resetConsumerOffset("client-1", "prices-v1", "group-a", 42L)
        registry.resetConsumerOffset("client-1", "orders-v1", "group-b", 1L)

        then:
        1 * offsetTracker.updateOffset("group-a:prices-v1", 42L)
    }

    def "allConsumersCaughtUp returns true false and handles exceptions"() {
        given:
        storage.getCurrentOffset("prices-v1", 0) >> 10L >> 10L >> { throw new RuntimeException("boom") }
        offsetTracker.getOffset("group-a:prices-v1") >> 10L >> 8L

        expect:
        registry.allConsumersCaughtUp("prices-v1", ["group-a:prices-v1"] as Set)
        !registry.allConsumersCaughtUp("prices-v1", ["group-a:prices-v1"] as Set)
        !registry.allConsumersCaughtUp("prices-v1", ["group-a:prices-v1"] as Set)
    }

    def "getRemoteConsumers returns formatted view for empty and populated topics"() {
        given:
        registrationService.getConsumersByTopic("empty") >> []
        registrationService.getConsumersByTopic("prices-v1") >> [new RemoteConsumer("client-1", "prices-v1", "group-a", true)]

        expect:
        registry.getRemoteConsumers("empty") == "No remote consumers for topic: empty"
        registry.getRemoteConsumers("prices-v1").contains("client-1:prices-v1:group-a")
    }
}
