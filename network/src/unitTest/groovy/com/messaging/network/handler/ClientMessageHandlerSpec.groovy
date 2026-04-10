package com.messaging.network.handler

import com.messaging.common.model.BrokerMessage
import com.messaging.common.model.ConsumerRecord
import com.messaging.common.model.EventType
import io.netty.channel.embedded.EmbeddedChannel
import io.netty.handler.codec.DecoderException
import spock.lang.Specification

import java.time.Instant

class ClientMessageHandlerSpec extends Specification {

    def "broker message is forwarded to the connection handler"() {
        given:
        def connection = new CapturingConnection()
        def channel = new EmbeddedChannel(new ClientMessageHandler(connection))
        def message = new BrokerMessage(BrokerMessage.MessageType.ACK, 42L, 'ok'.bytes)

        when:
        channel.writeInbound(message)

        then:
        connection.messages == [message]

        cleanup:
        channel.close()
    }

    def "decoded record list is converted into a synthetic data message"() {
        given:
        def connection = new CapturingConnection()
        def channel = new EmbeddedChannel(new ClientMessageHandler(connection))
        def records = [new ConsumerRecord('key-1', EventType.MESSAGE, '{"v":1}', Instant.parse('2024-01-01T00:00:00Z'))]

        when:
        channel.writeInbound(records)

        then:
        connection.messages.size() == 1
        connection.messages[0].type == BrokerMessage.MessageType.DATA
        new String(connection.messages[0].payload, 'UTF-8').contains('key-1')

        cleanup:
        channel.close()
    }

    def "decoder exception does not close the channel"() {
        given:
        def channel = new EmbeddedChannel(new ClientMessageHandler(new CapturingConnection()))

        when:
        channel.pipeline().fireExceptionCaught(new DecoderException('bad frame'))

        then:
        channel.isOpen()

        cleanup:
        channel.close()
    }

    private static class CapturingConnection {
        final List<BrokerMessage> messages = []

        void handleIncomingMessage(BrokerMessage message) {
            messages.add(message)
        }
    }
}
