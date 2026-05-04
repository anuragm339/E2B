package com.messaging.network.legacy

import com.messaging.common.model.BrokerMessage
import io.micronaut.test.extensions.spock.annotation.MicronautTest
import io.netty.channel.embedded.EmbeddedChannel
import spock.lang.Specification

@MicronautTest(startApplication = false)
class LegacyConnectionStateIntegrationSpec extends Specification {

    def "RESET outbound followed by generic ACK resolves to RESET_ACK with topic payload"() {
        given:
        def channel = new EmbeddedChannel(new LegacyConnectionState())

        when:
        channel.writeOutbound(new BrokerMessage(BrokerMessage.MessageType.RESET, 100L, 'test-topic'.bytes))
        channel.writeInbound(new BrokerMessage(BrokerMessage.MessageType.ACK, 200L, new byte[0]))

        then:
        BrokerMessage resolved = channel.readInbound()
        resolved != null
        resolved.type == BrokerMessage.MessageType.RESET_ACK
        def bb = java.nio.ByteBuffer.wrap(resolved.payload)
        def topicLen = bb.getInt()
        new String(resolved.payload, 4, topicLen, 'UTF-8') == 'test-topic'

        cleanup:
        channel.close()
    }

    def "DATA outbound followed by generic ACK resolves to BATCH_ACK"() {
        given:
        def channel = new EmbeddedChannel(new LegacyConnectionState())

        when:
        channel.writeOutbound(new BrokerMessage(BrokerMessage.MessageType.DATA, 100L, 'payload'.bytes))
        channel.writeInbound(new BrokerMessage(BrokerMessage.MessageType.ACK, 200L, new byte[0]))

        then:
        (channel.readInbound() as BrokerMessage).type == BrokerMessage.MessageType.BATCH_ACK

        cleanup:
        channel.close()
    }

    def "READY outbound followed by generic ACK resolves to READY_ACK"() {
        given:
        def channel = new EmbeddedChannel(new LegacyConnectionState())

        when:
        channel.writeOutbound(new BrokerMessage(BrokerMessage.MessageType.READY, 1L, new byte[0]))
        channel.writeInbound(new BrokerMessage(BrokerMessage.MessageType.ACK, 2L, new byte[0]))

        then:
        (channel.readInbound() as BrokerMessage).type == BrokerMessage.MessageType.READY_ACK

        cleanup:
        channel.close()
    }

    def "inbound READY is converted to READY_ACK for backwards compatibility"() {
        given:
        def channel = new EmbeddedChannel(new LegacyConnectionState())

        when:
        channel.writeInbound(new BrokerMessage(BrokerMessage.MessageType.READY, 1L, new byte[0]))

        then:
        (channel.readInbound() as BrokerMessage).type == BrokerMessage.MessageType.READY_ACK

        cleanup:
        channel.close()
    }

    def "non-ACK inbound messages pass through unchanged"() {
        given:
        def channel = new EmbeddedChannel(new LegacyConnectionState())
        def heartbeat = new BrokerMessage(BrokerMessage.MessageType.HEARTBEAT, 1L, new byte[0])

        when:
        channel.writeInbound(heartbeat)

        then:
        channel.readInbound() == heartbeat

        cleanup:
        channel.close()
    }

    def "ACK expectations resolve in FIFO order"() {
        given:
        def channel = new EmbeddedChannel(new LegacyConnectionState())

        when:
        channel.writeOutbound(new BrokerMessage(BrokerMessage.MessageType.RESET, 1L, 't'.bytes))
        channel.writeOutbound(new BrokerMessage(BrokerMessage.MessageType.DATA, 2L, 'p'.bytes))
        channel.writeInbound(new BrokerMessage(BrokerMessage.MessageType.ACK, 10L, new byte[0]))
        channel.writeInbound(new BrokerMessage(BrokerMessage.MessageType.ACK, 20L, new byte[0]))

        then:
        (channel.readInbound() as BrokerMessage).type == BrokerMessage.MessageType.RESET_ACK
        (channel.readInbound() as BrokerMessage).type == BrokerMessage.MessageType.BATCH_ACK

        cleanup:
        channel.close()
    }
}
