package com.messaging.broker.handler

import com.messaging.broker.consumer.ConsumerRegistrationService
import com.messaging.broker.consumer.RefreshCoordinator
import com.messaging.broker.consumer.RemoteConsumer
import com.messaging.common.api.NetworkServer
import com.messaging.common.model.BrokerMessage
import spock.lang.Specification

import java.nio.ByteBuffer
import java.nio.charset.StandardCharsets

class ResetAckHandlerSpec extends Specification {

    NetworkServer server = Mock()
    RefreshCoordinator refreshCoordinator = Mock()
    ConsumerRegistrationService registrationService = Mock()
    ResetAckHandler handler = new ResetAckHandler(server, refreshCoordinator, registrationService)

    def "closes connection for malformed payload sizes"() {
        when:
        handler.handle("client-1", message([1, 2, 3] as byte[]), "trace-1")
        handler.handle("client-1", message(intPayload(-1, 0)), "trace-2")
        handler.handle("client-1", message(intPayload(99, 1)), "trace-3")
        handler.handle("client-1", message(topicGroupPayload("prices-v1", "", 99, true)), "trace-4")

        then:
        4 * server.closeConnection("client-1")
    }

    def "legacy fallback without a mapped group soft fails"() {
        given:
        registrationService.getConsumersByClient("client-1") >> []

        when:
        handler.handle("client-1", message(topicGroupPayload("prices-v1", "", 0, true)), "trace-5")

        then:
        0 * server.closeConnection(_)
        0 * refreshCoordinator.handleResetAck(_, _, _, _)
    }

    def "legacy fallback resolves group and forwards reset ack"() {
        given:
        registrationService.getConsumersByClient("legacy-client") >> [
                new RemoteConsumer("legacy-client", "prices-v1", "legacy-group", true)
        ]

        when:
        handler.handle("legacy-client", message(topicGroupPayload("prices-v1", "", 0, true)), "trace-6")

        then:
        1 * refreshCoordinator.handleResetAck("legacy-group:prices-v1", "legacy-client", "prices-v1", "trace-6")
    }

    def "modern payload forwards directly to refresh coordinator"() {
        when:
        handler.handle("modern-client", message(topicGroupPayload("orders-v1", "group-a", 0, false)), "trace-7")

        then:
        1 * refreshCoordinator.handleResetAck("group-a:orders-v1", "modern-client", "orders-v1", "trace-7")
    }

    private static BrokerMessage message(byte[] payload) {
        new BrokerMessage(BrokerMessage.MessageType.RESET_ACK, 1L, payload)
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
