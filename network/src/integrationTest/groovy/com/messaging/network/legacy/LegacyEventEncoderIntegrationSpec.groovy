package com.messaging.network.legacy

import com.messaging.common.model.BrokerMessage
import com.messaging.network.legacy.events.EventType as LegacyEventType
import io.micronaut.test.extensions.spock.annotation.MicronautTest
import io.netty.buffer.ByteBuf
import io.netty.channel.embedded.EmbeddedChannel
import spock.lang.Specification

@MicronautTest(startApplication = false)
class LegacyEventEncoderIntegrationSpec extends Specification {

    def "RESET and READY messages encode to the expected single-byte legacy ordinals"() {
        expect:
        encode(msgType).getByte(0) as int == ordinal

        where:
        msgType                           || ordinal
        BrokerMessage.MessageType.RESET   || LegacyEventType.RESET.ordinal()
        BrokerMessage.MessageType.READY   || LegacyEventType.READY.ordinal()
    }

    def "DATA messages encode to legacy MESSAGE payloads"() {
        given:
        def payload = 'price-type|key-abc|application/json|{"v":1}'.bytes

        when:
        ByteBuf buf = encode(BrokerMessage.MessageType.DATA, payload)

        then:
        buf != null
        buf.readableBytes() > 1
        (buf.getByte(0) & 0xFF) == LegacyEventType.MESSAGE.ordinal()

        cleanup:
        buf?.release()
    }

    private static ByteBuf encode(BrokerMessage.MessageType type, byte[] payload = new byte[0]) {
        def channel = new EmbeddedChannel(new LegacyEventEncoder())
        try {
            channel.writeOutbound(new BrokerMessage(type, 1L, payload))
            return channel.readOutbound()
        } finally {
            channel.close()
        }
    }
}
