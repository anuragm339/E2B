package com.messaging.network.handler

import com.messaging.common.api.NetworkServer
import com.messaging.common.model.BrokerMessage
import io.netty.channel.embedded.EmbeddedChannel
import spock.lang.Specification

class ServerMessageHandlerSpec extends Specification {

    def "server handler delegates to all registered handlers even if one fails"() {
        given:
        def seen = []
        def handlers = [
            ({ clientId, msg -> seen << "${clientId}:${msg.messageId}" } as NetworkServer.MessageHandler),
            ({ clientId, msg -> throw new IllegalStateException('boom') } as NetworkServer.MessageHandler),
            ({ clientId, msg -> seen << "after:${msg.type}" } as NetworkServer.MessageHandler)
        ]
        def channel = new EmbeddedChannel(new ServerMessageHandler('client-1', handlers))

        when:
        channel.writeInbound(new BrokerMessage(BrokerMessage.MessageType.DATA, 7L, new byte[0]))

        then:
        seen == ['client-1:7', 'after:DATA']
        channel.isOpen()

        cleanup:
        channel.close()
    }
}
