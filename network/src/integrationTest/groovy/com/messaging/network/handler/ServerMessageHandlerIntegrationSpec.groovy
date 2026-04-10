package com.messaging.network.handler

import com.messaging.common.api.NetworkServer
import com.messaging.common.model.BrokerMessage
import io.micronaut.test.extensions.spock.annotation.MicronautTest
import io.netty.channel.embedded.EmbeddedChannel
import spock.lang.Specification

@MicronautTest(startApplication = false)
class ServerMessageHandlerIntegrationSpec extends Specification {

    def "server handler dispatches incoming broker messages to registered handlers"() {
        given:
        def seen = []
        def handlers = [
            ({ clientId, message -> seen << "${clientId}:${message.type}" } as NetworkServer.MessageHandler)
        ]
        def channel = new EmbeddedChannel(new ServerMessageHandler('client-a', handlers))

        when:
        channel.writeInbound(new BrokerMessage(BrokerMessage.MessageType.SUBSCRIBE, 123L, 'topic'.bytes))

        then:
        seen == ['client-a:SUBSCRIBE']

        cleanup:
        channel.close()
    }
}
