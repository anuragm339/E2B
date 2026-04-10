package com.messaging.network.legacy

import com.messaging.common.model.BrokerMessage
import com.messaging.network.legacy.events.EventType as LegacyEventType
import io.micronaut.test.extensions.spock.annotation.MicronautTest
import io.netty.buffer.ByteBuf
import io.netty.buffer.Unpooled
import io.netty.channel.embedded.EmbeddedChannel
import spock.lang.Specification

import java.io.ByteArrayOutputStream
import java.io.DataOutputStream

@MicronautTest(startApplication = false)
class LegacyEventDecoderIntegrationSpec extends Specification {

    private static ByteBuf legacyBytes(Closure<Void> write) {
        def baos = new ByteArrayOutputStream()
        def dos = new DataOutputStream(baos)
        write(dos)
        dos.flush()
        Unpooled.wrappedBuffer(baos.toByteArray())
    }

    def "REGISTER bytes decode to SUBSCRIBE with legacy service payload"() {
        given:
        def buf = legacyBytes { DataOutputStream dos ->
            dos.writeByte(LegacyEventType.REGISTER.ordinal())
            dos.writeInt(1)
            dos.writeUTF('my-service')
        }
        def channel = new EmbeddedChannel(new LegacyEventDecoder())

        when:
        channel.writeInbound(buf)
        BrokerMessage msg = channel.readInbound()

        then:
        msg != null
        msg.type == BrokerMessage.MessageType.SUBSCRIBE
        def json = new String(msg.payload, 'UTF-8')
        json.contains('"isLegacy":true')
        json.contains('"serviceName":"my-service"')

        cleanup:
        channel.close()
    }

    def "RESET READY ACK and EOF bytes decode to the expected broker message types"() {
        expect:
        decodeLegacy(singleByteEvent(eventType)).type == brokerType

        where:
        eventType             || brokerType
        LegacyEventType.RESET || BrokerMessage.MessageType.RESET
        LegacyEventType.READY || BrokerMessage.MessageType.READY
        LegacyEventType.ACK   || BrokerMessage.MessageType.ACK
        LegacyEventType.EOF   || BrokerMessage.MessageType.DISCONNECT
    }

    def "incomplete REGISTER frame is buffered and not decoded yet"() {
        given:
        def buf = legacyBytes { DataOutputStream dos ->
            dos.writeByte(LegacyEventType.REGISTER.ordinal())
            dos.writeShort(0)
        }
        def channel = new EmbeddedChannel(new LegacyEventDecoder())

        when:
        channel.writeInbound(buf)

        then:
        channel.readInbound() == null

        cleanup:
        channel.close()
    }

    private static BrokerMessage decodeLegacy(ByteBuf buf) {
        def channel = new EmbeddedChannel(new LegacyEventDecoder())
        try {
            channel.writeInbound(buf)
            return channel.readInbound()
        } finally {
            channel.close()
        }
    }

    private static ByteBuf singleByteEvent(LegacyEventType eventType) {
        legacyBytes { DataOutputStream dos ->
            dos.writeByte(eventType.ordinal())
        }
    }
}
