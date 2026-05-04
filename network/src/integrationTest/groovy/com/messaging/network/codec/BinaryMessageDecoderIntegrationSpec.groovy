package com.messaging.network.codec

import com.messaging.common.model.BrokerMessage
import io.micronaut.test.extensions.spock.annotation.MicronautTest
import io.netty.buffer.ByteBuf
import io.netty.buffer.Unpooled
import io.netty.channel.embedded.EmbeddedChannel
import spock.lang.Specification

@MicronautTest(startApplication = false)
class BinaryMessageDecoderIntegrationSpec extends Specification {

    def "binary decoder round-trip preserves type messageId and payload"() {
        given:
        def payload = 'round-trip-payload'.bytes
        def original = new BrokerMessage(BrokerMessage.MessageType.SUBSCRIBE, 9999L, payload)
        def encodeChannel = new EmbeddedChannel(new BinaryMessageEncoder())
        encodeChannel.writeOutbound(original)
        ByteBuf encoded = encodeChannel.readOutbound()

        def decodeChannel = new EmbeddedChannel(new BinaryMessageDecoder())

        when:
        decodeChannel.writeInbound(encoded)
        BrokerMessage decoded = decodeChannel.readInbound()

        then:
        decoded != null
        decoded.type == BrokerMessage.MessageType.SUBSCRIBE
        decoded.messageId == 9999L
        new String(decoded.payload) == 'round-trip-payload'

        cleanup:
        encodeChannel.close()
        decodeChannel.close()
    }

    def "binary decoder handles empty payload messages"() {
        given:
        def original = new BrokerMessage(BrokerMessage.MessageType.HEARTBEAT, 1L, new byte[0])
        def encodeChannel = new EmbeddedChannel(new BinaryMessageEncoder())
        encodeChannel.writeOutbound(original)
        ByteBuf encoded = encodeChannel.readOutbound()

        def decodeChannel = new EmbeddedChannel(new BinaryMessageDecoder())

        when:
        decodeChannel.writeInbound(encoded)
        BrokerMessage decoded = decodeChannel.readInbound()

        then:
        decoded != null
        decoded.type == BrokerMessage.MessageType.HEARTBEAT
        decoded.payload == null || decoded.payload.length == 0

        cleanup:
        encodeChannel.close()
        decodeChannel.close()
    }

    def "binary decoder preserves all message types"() {
        given:
        def encodeChannel = new EmbeddedChannel(new BinaryMessageEncoder())
        encodeChannel.writeOutbound(new BrokerMessage(msgType, 1L, new byte[0]))
        ByteBuf encoded = encodeChannel.readOutbound()

        def decodeChannel = new EmbeddedChannel(new BinaryMessageDecoder())

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
            BrokerMessage.MessageType.BATCH_ACK,
            BrokerMessage.MessageType.RESET_ACK,
            BrokerMessage.MessageType.READY_ACK,
        ]
    }

    def "binary decoder returns nothing when frame is incomplete"() {
        given:
        def channel = new EmbeddedChannel(new BinaryMessageDecoder())

        when:
        channel.writeInbound(Unpooled.wrappedBuffer(new byte[5]))

        then:
        channel.readInbound() == null

        cleanup:
        channel.close()
    }
}
