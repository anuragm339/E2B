package com.messaging.network.codec

import com.messaging.common.model.BrokerMessage
import io.micronaut.test.extensions.spock.annotation.MicronautTest
import io.netty.buffer.ByteBuf
import io.netty.channel.embedded.EmbeddedChannel
import spock.lang.Specification

@MicronautTest(startApplication = false)
class JsonMessageDecoderIntegrationSpec extends Specification {

    def "JSON decoder round-trip preserves all fields"() {
        given:
        def payload = '{"key":"value"}'.bytes
        def original = new BrokerMessage(BrokerMessage.MessageType.DATA, 777L, payload)
        def encodeChannel = new EmbeddedChannel(new JsonMessageEncoder())
        encodeChannel.writeOutbound(original)
        ByteBuf encoded = encodeChannel.readOutbound()
        def decodeChannel = new EmbeddedChannel(new JsonMessageDecoder())

        when:
        decodeChannel.writeInbound(encoded)
        BrokerMessage decoded = decodeChannel.readInbound()

        then:
        decoded != null
        decoded.type == BrokerMessage.MessageType.DATA
        decoded.messageId == 777L
        new String(decoded.payload) == '{"key":"value"}'

        cleanup:
        encodeChannel.close()
        decodeChannel.close()
    }

    def "JSON decoder supports all relevant message types"() {
        given:
        def encodeChannel = new EmbeddedChannel(new JsonMessageEncoder())
        encodeChannel.writeOutbound(new BrokerMessage(msgType, 100L, new byte[0]))
        ByteBuf encoded = encodeChannel.readOutbound()
        def decodeChannel = new EmbeddedChannel(new JsonMessageDecoder())

        when:
        decodeChannel.writeInbound(encoded)
        BrokerMessage decoded = decodeChannel.readInbound()

        then:
        decoded.type == msgType

        cleanup:
        encodeChannel.close()
        decodeChannel.close()

        where:
        msgType << [
            BrokerMessage.MessageType.DATA,
            BrokerMessage.MessageType.ACK,
            BrokerMessage.MessageType.SUBSCRIBE,
            BrokerMessage.MessageType.RESET,
            BrokerMessage.MessageType.READY,
        ]
    }
}
