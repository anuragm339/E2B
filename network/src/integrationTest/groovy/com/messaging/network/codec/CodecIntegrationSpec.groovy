package com.messaging.network.codec

import com.messaging.common.model.BrokerMessage
import com.messaging.common.model.ConsumerRecord
import com.messaging.common.model.EventType
import io.micronaut.test.extensions.spock.annotation.MicronautTest
import io.netty.buffer.ByteBuf
import io.netty.buffer.Unpooled
import io.netty.channel.embedded.EmbeddedChannel
import spock.lang.Specification

import java.time.Instant

/**
 * Integration tests for network codec pipeline components.
 *
 * Uses Netty EmbeddedChannel (in-process pipeline, no real TCP) to test:
 *   - BinaryMessageEncoder + BinaryMessageDecoder round-trip
 *   - JsonMessageEncoder + JsonMessageDecoder round-trip
 *   - BatchAckHandler: sends BATCH_ACK and forwards records downstream
 */
@MicronautTest
class CodecIntegrationSpec extends Specification {

    // =========================================================================
    // BinaryMessageEncoder / BinaryMessageDecoder
    // =========================================================================

    def "binary encoder produces a 13-byte header followed by the payload"() {
        given:
        def channel = new EmbeddedChannel(new BinaryMessageEncoder())
        def msg = new BrokerMessage(BrokerMessage.MessageType.DATA, 42L, "hello".bytes)

        when:
        channel.writeOutbound(msg)
        ByteBuf buf = channel.readOutbound()

        then:
        buf != null
        buf.readableBytes() == 1 + 8 + 4 + 5   // type(1) + messageId(8) + payloadLen(4) + "hello"(5)

        cleanup:
        buf?.release()
        channel.close()
    }

    def "binary encoder → decoder round-trip preserves type, messageId and payload"() {
        given:
        def payload  = "round-trip-payload".bytes
        def original = new BrokerMessage(BrokerMessage.MessageType.SUBSCRIBE, 9999L, payload)

        // Encode
        def encodeChannel = new EmbeddedChannel(new BinaryMessageEncoder())
        encodeChannel.writeOutbound(original)
        ByteBuf encoded = encodeChannel.readOutbound()

        // Decode
        def decodeChannel = new EmbeddedChannel(new BinaryMessageDecoder())
        decodeChannel.writeInbound(encoded)
        BrokerMessage decoded = decodeChannel.readInbound()

        expect:
        decoded != null
        decoded.type      == BrokerMessage.MessageType.SUBSCRIBE
        decoded.messageId == 9999L
        new String(decoded.payload) == "round-trip-payload"

        cleanup:
        encodeChannel.close()
        decodeChannel.close()
    }

    def "binary round-trip works for message with empty payload"() {
        given:
        def original = new BrokerMessage(BrokerMessage.MessageType.HEARTBEAT, 1L, new byte[0])

        def encodeChannel = new EmbeddedChannel(new BinaryMessageEncoder())
        encodeChannel.writeOutbound(original)
        ByteBuf encoded = encodeChannel.readOutbound()

        def decodeChannel = new EmbeddedChannel(new BinaryMessageDecoder())
        decodeChannel.writeInbound(encoded)
        BrokerMessage decoded = decodeChannel.readInbound()

        expect:
        decoded != null
        decoded.type == BrokerMessage.MessageType.HEARTBEAT
        (decoded.payload == null || decoded.payload.length == 0)

        cleanup:
        encodeChannel.close()
        decodeChannel.close()
    }

    def "binary round-trip preserves all MessageType values"() {
        given:
        def channel = new EmbeddedChannel(new BinaryMessageEncoder())
        channel.writeOutbound(new BrokerMessage(msgType, 1L, new byte[0]))
        ByteBuf encoded = channel.readOutbound()

        def decodeChannel = new EmbeddedChannel(new BinaryMessageDecoder())
        decodeChannel.writeInbound(encoded)
        BrokerMessage decoded = decodeChannel.readInbound()

        expect:
        decoded.type == msgType

        cleanup:
        channel.close()
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
        // Only 5 bytes – less than the 13-byte minimum header
        def channel = new EmbeddedChannel(new BinaryMessageDecoder())
        channel.writeInbound(Unpooled.wrappedBuffer(new byte[5]))

        expect:
        channel.readInbound() == null

        cleanup:
        channel.close()
    }

    // =========================================================================
    // JsonMessageEncoder / JsonMessageDecoder
    // =========================================================================

    def "JSON encoder → decoder round-trip preserves all fields"() {
        given:
        def payload  = '{"key":"value"}'.bytes
        def original = new BrokerMessage(BrokerMessage.MessageType.DATA, 777L, payload)

        def encodeChannel = new EmbeddedChannel(new JsonMessageEncoder())
        encodeChannel.writeOutbound(original)
        ByteBuf encoded = encodeChannel.readOutbound()

        def decodeChannel = new EmbeddedChannel(new JsonMessageDecoder())
        decodeChannel.writeInbound(encoded)
        BrokerMessage decoded = decodeChannel.readInbound()

        expect:
        decoded != null
        decoded.type      == BrokerMessage.MessageType.DATA
        decoded.messageId == 777L
        new String(decoded.payload) == '{"key":"value"}'

        cleanup:
        encodeChannel.close()
        decodeChannel.close()
    }

    def "JSON encoder output is newline-delimited and contains type and messageId fields"() {
        given:
        def channel = new EmbeddedChannel(new JsonMessageEncoder())
        def msg     = new BrokerMessage(BrokerMessage.MessageType.ACK, 1L, new byte[0])

        when:
        channel.writeOutbound(msg)
        ByteBuf buf   = channel.readOutbound()
        byte[] bytes  = new byte[buf.readableBytes()]
        buf.readBytes(bytes)
        def text = new String(bytes, "UTF-8")

        then:
        text.endsWith("\n")
        text.contains('"type"')
        text.contains('"messageId"')

        cleanup:
        buf?.release()
        channel.close()
    }

    def "JSON round-trip works for all relevant message types"() {
        given:
        def encodeChannel = new EmbeddedChannel(new JsonMessageEncoder())
        encodeChannel.writeOutbound(new BrokerMessage(msgType, 100L, new byte[0]))
        ByteBuf encoded = encodeChannel.readOutbound()

        def decodeChannel = new EmbeddedChannel(new JsonMessageDecoder())
        decodeChannel.writeInbound(encoded)
        BrokerMessage decoded = decodeChannel.readInbound()

        expect:
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

    // =========================================================================
    // BatchAckHandler
    // =========================================================================

    def "BatchAckHandler sends BATCH_ACK with topic+group payload and forwards records"() {
        given:
        def records = [
            new ConsumerRecord('key1', EventType.MESSAGE, '{"v":1}', Instant.now()),
            new ConsumerRecord('key2', EventType.DELETE,  null,      Instant.now()),
        ]
        def event   = new BatchDecodedEvent(records, 'test-topic', 'test-group')
        def channel = new EmbeddedChannel(new BatchAckHandler())

        when:
        channel.writeInbound(event)

        then:
        // Outbound: BATCH_ACK sent back to broker
        BrokerMessage ack = channel.readOutbound()
        ack != null
        ack.type == BrokerMessage.MessageType.BATCH_ACK

        // BATCH_ACK payload: [topicLen:4][topic:var][groupLen:4][group:var]
        def bb       = java.nio.ByteBuffer.wrap(ack.payload)
        def topicLen = bb.getInt()                              // bytes 0-3
        def topic    = new String(ack.payload, 4, topicLen, 'UTF-8')
        topic == 'test-topic'

        bb.position(4 + topicLen)                               // skip topic bytes
        def groupLen = bb.getInt()                              // next 4 bytes
        def group    = new String(ack.payload, 4 + topicLen + 4, groupLen, 'UTF-8')
        group == 'test-group'

        // Inbound: records forwarded to next handler
        channel.readInbound() == records

        cleanup:
        channel.close()
    }

    def "BatchAckHandler passes through non-BatchDecodedEvent messages unchanged"() {
        given:
        def channel = new EmbeddedChannel(new BatchAckHandler())
        def msg     = new BrokerMessage(BrokerMessage.MessageType.ACK, 1L, new byte[0])

        when:
        channel.writeInbound(msg)

        then:
        channel.readInbound() == msg      // passed through
        channel.readOutbound() == null    // no outbound write for non-batch messages

        cleanup:
        channel.close()
    }

    def "BatchAckHandler handles multiple consecutive batches"() {
        given:
        def records1 = [new ConsumerRecord('k1', EventType.MESSAGE, '{}', Instant.now())]
        def records2 = [new ConsumerRecord('k2', EventType.MESSAGE, '{}', Instant.now())]
        def channel  = new EmbeddedChannel(new BatchAckHandler())

        when:
        channel.writeInbound(new BatchDecodedEvent(records1, 'topic-a', 'grp'))
        channel.writeInbound(new BatchDecodedEvent(records2, 'topic-b', 'grp'))

        then:
        (channel.readOutbound() as BrokerMessage).type == BrokerMessage.MessageType.BATCH_ACK
        (channel.readOutbound() as BrokerMessage).type == BrokerMessage.MessageType.BATCH_ACK
        channel.readInbound() == records1
        channel.readInbound() == records2

        cleanup:
        channel.close()
    }
}
