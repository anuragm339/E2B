package com.messaging.broker.handler

import com.fasterxml.jackson.databind.ObjectMapper
import com.messaging.broker.consumer.ConsumerRegistry
import com.messaging.broker.consumer.RefreshContext
import com.messaging.broker.consumer.RefreshCoordinator
import com.messaging.broker.consumer.RefreshState
import com.messaging.broker.legacy.LegacyClientConfig
import com.messaging.broker.monitoring.BrokerMetrics
import com.messaging.common.api.NetworkServer
import com.messaging.common.model.BrokerMessage
import spock.lang.Specification

import java.util.concurrent.CompletableFuture

class SubscribeHandlerSpec extends Specification {
    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper()

    NetworkServer server = Mock()
    ConsumerRegistry remoteConsumers = Mock()
    BrokerMetrics metrics = Mock()
    LegacyClientConfig legacyClientConfig = new LegacyClientConfig()
    RefreshCoordinator refreshCoordinator = Mock()

    SubscribeHandler handler = new SubscribeHandler(
            server, remoteConsumers, metrics, legacyClientConfig, refreshCoordinator)

    def "modern subscribe registers new consumer sends ack and startup ready"() {
        given:
        remoteConsumers.registerConsumer("client-1", "prices-v1", "group-a", false, "trace-1") >> true

        when:
        handler.handle("client-1", subscribe([topic: "prices-v1", group: "group-a"]), "trace-1")

        then:
        1 * metrics.recordConsumerConnection()
        1 * remoteConsumers.sendStartupReadyToModernConsumer("client-1", "prices-v1", "group-a")
        1 * server.send("client-1", { BrokerMessage msg -> msg.type == BrokerMessage.MessageType.ACK }) >> CompletableFuture.completedFuture(null)
    }

    def "modern duplicate subscribe still acks but does not re-record connection or ready"() {
        given:
        remoteConsumers.registerConsumer("client-1", "prices-v1", "group-a", false, "trace-2") >> false

        when:
        handler.handle("client-1", subscribe([topic: "prices-v1", group: "group-a"]), "trace-2")

        then:
        0 * metrics.recordConsumerConnection()
        0 * remoteConsumers.sendStartupReadyToModernConsumer(_, _, _)
        1 * server.send("client-1", { BrokerMessage msg -> msg.type == BrokerMessage.MessageType.ACK }) >> CompletableFuture.completedFuture(null)
    }

    def "legacy subscribe rejects disabled and unknown services without side effects"() {
        given:
        legacyClientConfig.enabled = false

        when:
        handler.handle("legacy-client", subscribe([isLegacy: true, serviceName: "svc"]), "trace-3")

        then:
        0 * remoteConsumers._
        0 * server.send(_, _)

        when:
        legacyClientConfig.enabled = true
        legacyClientConfig.serviceTopics = [:]
        handler.handle("legacy-client", subscribe([isLegacy: true, serviceName: "svc"]), "trace-4")

        then:
        0 * remoteConsumers._
        0 * server.send(_, _)
    }

    def "legacy subscribe during active refresh bypasses startup ready and handles ready sent and replaying branches"() {
        given:
        legacyClientConfig.serviceTopics = [svc: ["prices-v1", "orders-v1"]]
        remoteConsumers.registerConsumer("legacy-client", "prices-v1", "svc", true, "trace-5") >> true
        remoteConsumers.registerConsumer("legacy-client", "orders-v1", "svc", true, "trace-5") >> true
        refreshCoordinator.isRefreshActive("prices-v1") >> true
        refreshCoordinator.isRefreshActive("orders-v1") >> true

        def readyContext = new RefreshContext("prices-v1", ["svc:prices-v1"] as Set)
        readyContext.setState(RefreshState.READY_SENT)
        def replayingContext = new RefreshContext("orders-v1", ["svc:orders-v1"] as Set)
        replayingContext.setState(RefreshState.REPLAYING)
        refreshCoordinator.getRefreshStatus("prices-v1") >> readyContext
        refreshCoordinator.getRefreshStatus("orders-v1") >> replayingContext

        when:
        handler.handle("legacy-client", subscribe([isLegacy: true, serviceName: "svc"]), "trace-5")

        then:
        2 * metrics.recordConsumerConnection()
        1 * remoteConsumers.markLegacyConsumerReady("legacy-client")
        1 * remoteConsumers.sendRefreshReadyToConsumer("legacy-client", "prices-v1")
        1 * refreshCoordinator.registerLateJoiningConsumer("orders-v1", "svc:orders-v1")
        0 * remoteConsumers.sendStartupReadyToLegacyConsumer(_)
        0 * server.send(_, _)
    }

    def "legacy subscribe without active refresh uses startup ready path"() {
        given:
        legacyClientConfig.serviceTopics = [svc: ["prices-v1"]]
        remoteConsumers.registerConsumer("legacy-client", "prices-v1", "svc", true, "trace-6") >> true
        refreshCoordinator.isRefreshActive("prices-v1") >> false

        when:
        handler.handle("legacy-client", subscribe([isLegacy: true, serviceName: "svc"]), "trace-6")

        then:
        1 * metrics.recordConsumerConnection()
        1 * remoteConsumers.sendStartupReadyToLegacyConsumer("legacy-client")
        0 * server.send(_, _)
    }

    def "invalid subscribe payload closes connection"() {
        when:
        handler.handle("client-1", subscribe([group: "group-a"]), "trace-7")
        handler.handle("client-1", new BrokerMessage(BrokerMessage.MessageType.SUBSCRIBE, 1L, '{"topic":"x","group":null}'.bytes), "trace-8")

        then:
        2 * server.closeConnection("client-1")
    }

    private static BrokerMessage subscribe(Map payload) {
        new BrokerMessage(BrokerMessage.MessageType.SUBSCRIBE, 1L, OBJECT_MAPPER.writeValueAsBytes(payload))
    }
}
