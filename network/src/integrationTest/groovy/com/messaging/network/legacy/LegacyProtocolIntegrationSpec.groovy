package com.messaging.network.legacy

import com.messaging.common.model.BrokerMessage
import com.messaging.network.legacy.events.EventType as LegacyEventType
import io.micronaut.test.extensions.spock.annotation.MicronautTest
import io.netty.buffer.ByteBuf
import io.netty.buffer.Unpooled
import io.netty.channel.ChannelHandlerContext
import io.netty.channel.ChannelInboundHandlerAdapter
import io.netty.channel.ChannelOutboundHandlerAdapter
import io.netty.channel.ChannelPromise
import io.netty.channel.embedded.EmbeddedChannel
import jakarta.inject.Inject
import spock.lang.Specification

import java.io.ByteArrayOutputStream
import java.io.DataOutputStream
import java.util.concurrent.CopyOnWriteArrayList

/**
 * Integration tests for the legacy protocol components.
 *
 * Covers:
 *   - LegacyEventDecoder: legacy wire bytes → BrokerMessage conversions
 *   - LegacyEventEncoder: BrokerMessage → legacy wire bytes conversions
 *   - DefaultProtocolDetectionService: first-byte protocol detection
 *   - LegacyConnectionState: stateful ACK resolution (RESET → RESET_ACK, DATA → BATCH_ACK)
 *   - ProtocolDetectionDecoder: pipeline switching for modern and legacy first bytes
 *
 * All tests use Netty EmbeddedChannel — no real TCP sockets required.
 *
 * Legacy wire format: [EventType ordinal:1B][event-specific payload...]
 *   ordinal 0 = REGISTER, 1 = MESSAGE, 2 = RESET, 3 = READY, 4 = ACK, 5 = EOF, 6 = DELETE, 7 = BATCH
 */
@MicronautTest
class LegacyProtocolIntegrationSpec extends Specification {

    /** Injected from Micronaut context — @Singleton registered by DefaultProtocolDetectionService. */
    @Inject DefaultProtocolDetectionService detectionService

    // =========================================================================
    // Helpers
    // =========================================================================

    /** Build legacy wire bytes using Java DataOutputStream (big-endian, matching Netty ByteBuf). */
    private static ByteBuf legacyBytes(Closure<Void> write) {
        def baos = new ByteArrayOutputStream()
        def dos  = new DataOutputStream(baos)
        write(dos)
        dos.flush()
        Unpooled.wrappedBuffer(baos.toByteArray())
    }

    // =========================================================================
    // LegacyEventDecoder
    // =========================================================================

    def "REGISTER bytes → BrokerMessage(SUBSCRIBE) with JSON serviceName payload"() {
        given:
        def buf = legacyBytes { DataOutputStream dos ->
            dos.writeByte(LegacyEventType.REGISTER.ordinal())  // 0
            dos.writeInt(1)                  // version
            dos.writeUTF("my-service")       // clientId
        }
        def channel = new EmbeddedChannel(new LegacyEventDecoder())
        channel.writeInbound(buf)
        BrokerMessage msg = channel.readInbound()

        expect:
        msg != null
        msg.type == BrokerMessage.MessageType.SUBSCRIBE
        def json = new String(msg.payload, 'UTF-8')
        json.contains('"isLegacy":true')
        json.contains('"serviceName":"my-service"')

        cleanup:
        channel.close()
    }

    def "RESET byte → BrokerMessage(RESET) with empty payload"() {
        given:
        def buf = legacyBytes { DataOutputStream dos ->
            dos.writeByte(LegacyEventType.RESET.ordinal())  // 2
            // ResetEvent.toWire writes nothing
        }
        def channel = new EmbeddedChannel(new LegacyEventDecoder())
        channel.writeInbound(buf)
        BrokerMessage msg = channel.readInbound()

        expect:
        msg != null
        msg.type == BrokerMessage.MessageType.RESET

        cleanup:
        channel.close()
    }

    def "READY byte → BrokerMessage(READY)"() {
        given:
        def buf = legacyBytes { DataOutputStream dos ->
            dos.writeByte(LegacyEventType.READY.ordinal())  // 3
        }
        def channel = new EmbeddedChannel(new LegacyEventDecoder())
        channel.writeInbound(buf)
        BrokerMessage msg = channel.readInbound()

        expect:
        msg != null
        msg.type == BrokerMessage.MessageType.READY

        cleanup:
        channel.close()
    }

    def "ACK byte → BrokerMessage(ACK)"() {
        given:
        def buf = legacyBytes { DataOutputStream dos ->
            dos.writeByte(LegacyEventType.ACK.ordinal())  // 4
        }
        def channel = new EmbeddedChannel(new LegacyEventDecoder())
        channel.writeInbound(buf)
        BrokerMessage msg = channel.readInbound()

        expect:
        msg != null
        msg.type == BrokerMessage.MessageType.ACK

        cleanup:
        channel.close()
    }

    def "EOF byte → BrokerMessage(DISCONNECT)"() {
        given:
        def buf = legacyBytes { DataOutputStream dos ->
            dos.writeByte(LegacyEventType.EOF.ordinal())  // 5
        }
        def channel = new EmbeddedChannel(new LegacyEventDecoder())
        channel.writeInbound(buf)
        BrokerMessage msg = channel.readInbound()

        expect:
        msg != null
        msg.type == BrokerMessage.MessageType.DISCONNECT

        cleanup:
        channel.close()
    }

    def "incomplete REGISTER frame is buffered and not decoded yet"() {
        given:
        // 3 bytes — not enough: 1 ordinal + at least 4 version + 2 UTF-length = 7 min
        def buf = legacyBytes { DataOutputStream dos ->
            dos.writeByte(LegacyEventType.REGISTER.ordinal())
            dos.writeShort(0)   // only 2 bytes of version
        }
        def channel = new EmbeddedChannel(new LegacyEventDecoder())
        channel.writeInbound(buf)

        expect:
        channel.readInbound() == null   // decoder waits for more data

        cleanup:
        channel.close()
    }

    // =========================================================================
    // LegacyEventEncoder
    // =========================================================================

    def "BrokerMessage(RESET) encodes to single RESET ordinal byte"() {
        given:
        def channel = new EmbeddedChannel(new LegacyEventEncoder())
        def msg     = new BrokerMessage(BrokerMessage.MessageType.RESET, 1L, new byte[0])

        when:
        channel.writeOutbound(msg)
        ByteBuf buf = channel.readOutbound()

        then:
        buf != null
        buf.readableBytes() == 1
        (buf.getByte(0) & 0xFF) == LegacyEventType.RESET.ordinal()   // 2

        cleanup:
        buf?.release()
        channel.close()
    }

    def "BrokerMessage(READY) encodes to single READY ordinal byte"() {
        given:
        def channel = new EmbeddedChannel(new LegacyEventEncoder())
        def msg     = new BrokerMessage(BrokerMessage.MessageType.READY, 1L, new byte[0])

        when:
        channel.writeOutbound(msg)
        ByteBuf buf = channel.readOutbound()

        then:
        buf != null
        buf.readableBytes() == 1
        (buf.getByte(0) & 0xFF) == LegacyEventType.READY.ordinal()   // 3

        cleanup:
        buf?.release()
        channel.close()
    }

    def "BrokerMessage(DATA) encodes to MESSAGE ordinal + data payload"() {
        given:
        // DATA payload format used by LegacyEventEncoder: {type}|{key}|{contentType}|{content}
        def payload = "price-type|key-abc|application/json|{\"v\":1}".bytes
        def channel = new EmbeddedChannel(new LegacyEventEncoder())
        def msg     = new BrokerMessage(BrokerMessage.MessageType.DATA, 1L, payload)

        when:
        channel.writeOutbound(msg)
        ByteBuf buf = channel.readOutbound()

        then:
        buf != null
        buf.readableBytes() > 1
        (buf.getByte(0) & 0xFF) == LegacyEventType.MESSAGE.ordinal()   // 1

        cleanup:
        buf?.release()
        channel.close()
    }

    // =========================================================================
    // DefaultProtocolDetectionService
    // =========================================================================

    def "first byte 0x00 (REGISTER ordinal) is detected as LEGACY"() {
        expect:
        detectionService.detectProtocol((byte) 0x00) == ProtocolDetectionService.ProtocolType.LEGACY
    }

    def "first byte 0x01 (DATA code) is detected as MODERN"() {
        expect:
        detectionService.detectProtocol((byte) 0x01) == ProtocolDetectionService.ProtocolType.MODERN
    }

    def "first byte 0x03 (SUBSCRIBE code) is detected as MODERN"() {
        expect:
        detectionService.detectProtocol((byte) 0x03) == ProtocolDetectionService.ProtocolType.MODERN
    }

    def "unknown first byte defaults to MODERN"() {
        expect:
        detectionService.detectProtocol((byte) 0x7F) == ProtocolDetectionService.ProtocolType.MODERN
    }

    // =========================================================================
    // LegacyConnectionState
    // =========================================================================

    def "after RESET sent outbound, inbound ACK is resolved to RESET_ACK with topic payload"() {
        given:
        def channel  = new EmbeddedChannel(new LegacyConnectionState())
        def resetMsg = new BrokerMessage(BrokerMessage.MessageType.RESET, 100L, "test-topic".bytes)
        def ackMsg   = new BrokerMessage(BrokerMessage.MessageType.ACK,   200L, new byte[0])

        when:
        channel.writeOutbound(resetMsg)   // broker sends RESET → state records expectation
        channel.writeInbound(ackMsg)      // client sends ACK → resolved to RESET_ACK

        then:
        BrokerMessage resolved = channel.readInbound()
        resolved != null
        resolved.type == BrokerMessage.MessageType.RESET_ACK

        // Payload: [topicLen:4][topic:var][groupLen:4 = 0 (legacy)]
        def bb       = java.nio.ByteBuffer.wrap(resolved.payload)
        def topicLen = bb.getInt()
        new String(resolved.payload, 4, topicLen, 'UTF-8') == 'test-topic'

        cleanup:
        channel.close()
    }

    def "after DATA sent outbound, inbound ACK is resolved to BATCH_ACK"() {
        given:
        def channel  = new EmbeddedChannel(new LegacyConnectionState())
        def dataMsg  = new BrokerMessage(BrokerMessage.MessageType.DATA, 100L, "payload".bytes)
        def ackMsg   = new BrokerMessage(BrokerMessage.MessageType.ACK,  200L, new byte[0])

        when:
        channel.writeOutbound(dataMsg)
        channel.writeInbound(ackMsg)

        then:
        BrokerMessage resolved = channel.readInbound()
        resolved != null
        resolved.type == BrokerMessage.MessageType.BATCH_ACK

        cleanup:
        channel.close()
    }

    def "after READY sent outbound, inbound ACK is resolved to READY_ACK"() {
        given:
        def channel  = new EmbeddedChannel(new LegacyConnectionState())
        def readyMsg = new BrokerMessage(BrokerMessage.MessageType.READY, 1L, new byte[0])
        def ackMsg   = new BrokerMessage(BrokerMessage.MessageType.ACK,   2L, new byte[0])

        when:
        channel.writeOutbound(readyMsg)
        channel.writeInbound(ackMsg)

        then:
        BrokerMessage resolved = channel.readInbound()
        resolved != null
        resolved.type == BrokerMessage.MessageType.READY_ACK

        cleanup:
        channel.close()
    }

    def "inbound READY from legacy client is converted to READY_ACK (backwards compatibility)"() {
        given:
        def channel  = new EmbeddedChannel(new LegacyConnectionState())
        def readyMsg = new BrokerMessage(BrokerMessage.MessageType.READY, 1L, new byte[0])

        when:
        channel.writeInbound(readyMsg)

        then:
        BrokerMessage resolved = channel.readInbound()
        resolved != null
        resolved.type == BrokerMessage.MessageType.READY_ACK

        cleanup:
        channel.close()
    }

    def "non-ACK inbound messages pass through LegacyConnectionState unchanged"() {
        given:
        def channel   = new EmbeddedChannel(new LegacyConnectionState())
        def heartbeat = new BrokerMessage(BrokerMessage.MessageType.HEARTBEAT, 1L, new byte[0])

        when:
        channel.writeInbound(heartbeat)

        then:
        BrokerMessage passthrough = channel.readInbound()
        passthrough != null
        passthrough.type == BrokerMessage.MessageType.HEARTBEAT

        cleanup:
        channel.close()
    }

    def "FIFO ordering: multiple pending ACKs resolved in send order"() {
        given:
        def channel = new EmbeddedChannel(new LegacyConnectionState())

        when:
        channel.writeOutbound(new BrokerMessage(BrokerMessage.MessageType.RESET, 1L, "t".bytes))
        channel.writeOutbound(new BrokerMessage(BrokerMessage.MessageType.DATA,  2L, "p".bytes))
        channel.writeInbound(new BrokerMessage(BrokerMessage.MessageType.ACK, 10L, new byte[0]))
        channel.writeInbound(new BrokerMessage(BrokerMessage.MessageType.ACK, 20L, new byte[0]))

        then:
        (channel.readInbound() as BrokerMessage).type == BrokerMessage.MessageType.RESET_ACK
        (channel.readInbound() as BrokerMessage).type == BrokerMessage.MessageType.BATCH_ACK

        cleanup:
        channel.close()
    }

    // =========================================================================
    // ProtocolDetectionDecoder (pipeline integration)
    // =========================================================================

    def "modern binary bytes: ProtocolDetectionDecoder switches to BinaryMessageDecoder"() {
        given:
        // Encode a real BrokerMessage (first byte will be DATA = 0x01)
        def encodeChannel = new EmbeddedChannel(new com.messaging.network.codec.BinaryMessageEncoder())
        encodeChannel.writeOutbound(new BrokerMessage(BrokerMessage.MessageType.DATA, 555L, "payload".bytes))
        ByteBuf encoded = encodeChannel.readOutbound()
        encodeChannel.close()

        // ProtocolDetectionDecoder as the only handler — modern path replaces(this, ...)
        def channel = new EmbeddedChannel(new ProtocolDetectionDecoder())
        channel.writeInbound(encoded)
        BrokerMessage decoded = channel.readInbound()

        expect:
        decoded != null
        decoded.type      == BrokerMessage.MessageType.DATA
        decoded.messageId == 555L
        new String(decoded.payload) == 'payload'

        cleanup:
        channel.close()
    }

    def "legacy REGISTER bytes: ProtocolDetectionDecoder switches to LegacyEventDecoder"() {
        given:
        // Build a full pipeline with the handler names the decoder expects
        def captured = new CopyOnWriteArrayList<BrokerMessage>()
        def channel  = new EmbeddedChannel()
        channel.pipeline().addLast("decoder", new ProtocolDetectionDecoder())
        channel.pipeline().addLast("encoder", new ChannelOutboundHandlerAdapter())  // placeholder
        channel.pipeline().addLast("handler", new ChannelInboundHandlerAdapter() {
            @Override
            void channelRead(ChannelHandlerContext ctx, Object msg) {
                if (msg instanceof BrokerMessage) captured.add(msg)
            }
        })

        def buf = legacyBytes { DataOutputStream dos ->
            dos.writeByte(LegacyEventType.REGISTER.ordinal())  // 0
            dos.writeInt(1)
            dos.writeUTF("test-svc")
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
}
