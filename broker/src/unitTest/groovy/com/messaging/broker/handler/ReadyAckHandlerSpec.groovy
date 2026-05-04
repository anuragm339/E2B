package com.messaging.broker.handler

import com.messaging.broker.consumer.ConsumerRegistry
import com.messaging.broker.consumer.RefreshCoordinator
import com.messaging.broker.consumer.RemoteConsumer
import com.messaging.common.api.NetworkServer
import com.messaging.common.model.BrokerMessage
import spock.lang.Specification

import java.nio.ByteBuffer
import java.nio.charset.StandardCharsets

class ReadyAckHandlerSpec extends Specification {

    NetworkServer server = Mock()
    ConsumerRegistry remoteConsumers = Mock()
    RefreshCoordinator refreshCoordinator = Mock()
    ReadyAckHandler handler = new ReadyAckHandler(server, remoteConsumers, refreshCoordinator)

    def "handles legacy startup ready ack with empty payload"() {
        when:
        handler.handle("client-1", new BrokerMessage(BrokerMessage.MessageType.READY_ACK, 1L, new byte[0]), "trace-1")

        then:
        1 * remoteConsumers.markLegacyConsumerReady("client-1")
        0 * server._
    }

    def "closes connection for malformed payload sizes"() {
        when:
        handler.handle("client-1", message([1, 2, 3] as byte[]), "trace-2")
        handler.handle("client-1", message(intPayload(-1)), "trace-3")
        handler.handle("client-1", message(intPayload(99, 1)), "trace-4")
        handler.handle("client-1", message(topicGroupPayload("prices-v1", "", 99, true)), "trace-5")

        then:
        4 * server.closeConnection("client-1")
    }

    def "handles refresh ready ack for active refresh topics"() {
        given:
        refreshCoordinator.isRefreshActive("prices-v1") >> true

        when:
        handler.handle("client-1", message(topicGroupPayload("prices-v1", "group-a", 0, false)), "trace-6")

        then:
        1 * refreshCoordinator.handleReadyAck("group-a:prices-v1", "prices-v1", "trace-6")
    }

    def "handles startup ready ack for legacy fallback and modern consumers"() {
        given:
        refreshCoordinator.isRefreshActive("prices-v1") >> false
        refreshCoordinator.isRefreshActive("orders-v1") >> false
        remoteConsumers.getLegacyConsumersForClient("legacy-client") >> [new RemoteConsumer("legacy-client", "prices-v1", "legacy-group", true)]
        remoteConsumers.isLegacyConsumer("legacy-client:prices-v1:legacy-group") >> true
        remoteConsumers.isLegacyConsumer("modern-client:orders-v1:group-a") >> false

        when:
        handler.handle("legacy-client", message(topicGroupPayload("prices-v1", "", 0, true)), "trace-7")
        handler.handle("modern-client", message(topicGroupPayload("orders-v1", "group-a", 0, false)), "trace-8")

        then:
        1 * remoteConsumers.markLegacyConsumerReady("legacy-client")
        1 * remoteConsumers.markModernConsumerTopicReady("modern-client", "orders-v1", "group-a")
    }

    def "legacy fallback without group mapping soft fails"() {
        given:
        refreshCoordinator.isRefreshActive("prices-v1") >> false
        remoteConsumers.getLegacyConsumersForClient("client-1") >> []

        when:
        handler.handle("client-1", message(topicGroupPayload("prices-v1", "", 0, true)), "trace-9")

        then:
        0 * server.closeConnection(_)
        0 * remoteConsumers.markLegacyConsumerReady(_)
    }

    private static BrokerMessage message(byte[] payload) {
        new BrokerMessage(BrokerMessage.MessageType.READY_ACK, 1L, payload)
    }

    private static byte[] intPayload(int... values) {
        def buffer = ByteBuffer.allocate(values.length * 4)
        values.each(buffer.&putInt)
        buffer.array()
    }

    private static byte[] topicGroupPayload(String topic, String group, int groupLenOverride, boolean omitGroupBytes) {
        byte[] topicBytes = topic.getBytes(StandardCharsets.UTF_8)
        byte[] groupBytes = group == null ? new byte[0] : group.getBytes(StandardCharsets.UTF_8)
        int groupLen = omitGroupBytes ? groupLenOverride : groupBytes.length
        def buffer = ByteBuffer.allocate(4 + topicBytes.length + 4 + (omitGroupBytes ? 0 : groupBytes.length))
        buffer.putInt(topicBytes.length)
        buffer.put(topicBytes)
        buffer.putInt(groupLen)
        if (!omitGroupBytes) {
            buffer.put(groupBytes)
        }
        buffer.array()
    }
}
