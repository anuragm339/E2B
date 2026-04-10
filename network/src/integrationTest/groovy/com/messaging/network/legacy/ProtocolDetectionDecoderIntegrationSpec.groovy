package com.messaging.network.legacy

import com.messaging.common.model.BrokerMessage
import com.messaging.network.codec.BinaryMessageEncoder
import com.messaging.network.legacy.events.EventType as LegacyEventType
import io.micronaut.test.extensions.spock.annotation.MicronautTest
import io.netty.buffer.ByteBuf
import io.netty.buffer.Unpooled
import io.netty.channel.ChannelHandlerContext
import io.netty.channel.ChannelInboundHandlerAdapter
import io.netty.channel.ChannelOutboundHandlerAdapter
import io.netty.channel.embedded.EmbeddedChannel
import spock.lang.Specification

import java.io.ByteArrayOutputStream
import java.io.DataOutputStream
import java.util.concurrent.CopyOnWriteArrayList

@MicronautTest(startApplication = false)
class ProtocolDetectionDecoderIntegrationSpec extends Specification {

    def "modern binary bytes switch protocol decoder to the modern path"() {
        given:
        def encodeChannel = new EmbeddedChannel(new BinaryMessageEncoder())
        encodeChannel.writeOutbound(new BrokerMessage(BrokerMessage.MessageType.DATA, 555L, 'payload'.bytes))
        ByteBuf encoded = encodeChannel.readOutbound()
        encodeChannel.close()

        def channel = new EmbeddedChannel(new ProtocolDetectionDecoder())

        when:
        channel.writeInbound(encoded)
        BrokerMessage decoded = channel.readInbound()

        then:
        decoded != null
        decoded.type == BrokerMessage.MessageType.DATA
        decoded.messageId == 555L
        new String(decoded.payload) == 'payload'

        cleanup:
        channel.close()
    }

    def "legacy REGISTER bytes switch protocol decoder to the legacy path"() {
        given:
        def captured = new CopyOnWriteArrayList<BrokerMessage>()
        def channel = new EmbeddedChannel()
        channel.pipeline().addLast('decoder', new ProtocolDetectionDecoder())
        channel.pipeline().addLast('encoder', new ChannelOutboundHandlerAdapter())
        channel.pipeline().addLast('handler', new ChannelInboundHandlerAdapter() {
            @Override
            void channelRead(ChannelHandlerContext ctx, Object msg) {
                if (msg instanceof BrokerMessage) {
                    captured.add(msg)
                }
            }
        })

        def buf = legacyBytes { DataOutputStream dos ->
            dos.writeByte(LegacyEventType.REGISTER.ordinal())
            dos.writeInt(1)
            dos.writeUTF('test-svc')
        }

        when:
        channel.writeInbound(buf)

        then:
        captured.size() == 1
        captured[0].type == BrokerMessage.MessageType.SUBSCRIBE
        new String(captured[0].payload, 'UTF-8').contains('"serviceName":"test-svc"')

        cleanup:
        channel.close()
    }

    private static ByteBuf legacyBytes(Closure<Void> write) {
        def baos = new ByteArrayOutputStream()
        def dos = new DataOutputStream(baos)
        write(dos)
        dos.flush()
        Unpooled.wrappedBuffer(baos.toByteArray())
    }
}
