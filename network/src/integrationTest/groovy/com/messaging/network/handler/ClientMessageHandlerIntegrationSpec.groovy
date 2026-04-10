package com.messaging.network.handler

import com.messaging.common.model.BrokerMessage
import com.messaging.common.model.ConsumerRecord
import com.messaging.common.model.EventType
import io.micronaut.test.extensions.spock.annotation.MicronautTest
import io.netty.channel.embedded.EmbeddedChannel
import spock.lang.Specification

import java.time.Instant

@MicronautTest(startApplication = false)
class ClientMessageHandlerIntegrationSpec extends Specification {

    def "client handler passes zero copy batches into the connection callback as DATA"() {
        given:
        def connection = new CapturingConnection()
        def channel = new EmbeddedChannel(new ClientMessageHandler(connection))
        def records = [new ConsumerRecord('key-1', EventType.MESSAGE, '{"v":1}', Instant.parse('2024-01-01T00:00:00Z'))]

        when:
        channel.writeInbound(records)

        then:
        connection.messages.size() == 1
        connection.messages[0].type == BrokerMessage.MessageType.DATA
        new String(connection.messages[0].payload, 'UTF-8').contains('"msgKey":"key-1"')

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
