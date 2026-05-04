package com.messaging.broker.handler

import com.messaging.broker.consumer.ConsumerRegistry
import com.messaging.broker.consumer.RemoteConsumer
import com.messaging.common.api.NetworkServer
import com.messaging.common.model.BrokerMessage
import spock.lang.Specification

import java.nio.ByteBuffer
import java.nio.charset.StandardCharsets
import java.util.concurrent.ExecutorService

class BatchAckHandlerSpec extends Specification {

    NetworkServer server = Mock()
    ConsumerRegistry remoteConsumers = Mock()
    ExecutorService ackExecutor = Mock()
    BatchAckHandler handler = new BatchAckHandler(server, remoteConsumers, ackExecutor)

    def "routes legacy batch ack through legacy consumer group"() {
        given:
        remoteConsumers.getLegacyConsumersForClient("legacy-client") >> [new RemoteConsumer("legacy-client", "prices-v1", "legacy-group", true)]
        ackExecutor.execute(_ as Runnable) >> { Runnable task -> task.run() }

        when:
        handler.handle("legacy-client", new BrokerMessage(BrokerMessage.MessageType.BATCH_ACK, 1L, new byte[0]), "trace-1")

        then:
        1 * remoteConsumers.handleLegacyBatchAck("legacy-client", "legacy-group")
    }

    def "legacy batch ack from unknown client is ignored"() {
        given:
        remoteConsumers.getLegacyConsumersForClient("legacy-client") >> []

        when:
        handler.handle("legacy-client", new BrokerMessage(BrokerMessage.MessageType.BATCH_ACK, 1L, new byte[0]), "trace-2")

        then:
        0 * ackExecutor._
    }

    def "closes connection for malformed modern batch ack payloads"() {
        when:
        handler.handle("client-1", message([1, 2, 3] as byte[]), "trace-3")
        handler.handle("client-1", message(intPayload(-1)), "trace-4")
        handler.handle("client-1", message(intPayload(99, 1)), "trace-5")
        handler.handle("client-1", message(topicGroupPayload("prices-v1", null, 99, true)), "trace-6")

        then:
        4 * server.closeConnection("client-1")
    }

    def "routes modern batch ack to executor and swallows consumer exceptions"() {
        given:
        ackExecutor.execute(_ as Runnable) >> { Runnable task -> task.run() }
        remoteConsumers.handleBatchAck("client-1", "prices-v1", "group-a") >> { throw new IllegalStateException("boom") }

        when:
        handler.handle("client-1", message(topicGroupPayload("prices-v1", "group-a", 0, false)), "trace-7")

        then:
        noExceptionThrown()
        1 * remoteConsumers.handleBatchAck("client-1", "prices-v1", "group-a")
    }

    private static BrokerMessage message(byte[] payload) {
        new BrokerMessage(BrokerMessage.MessageType.BATCH_ACK, 1L, payload)
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
