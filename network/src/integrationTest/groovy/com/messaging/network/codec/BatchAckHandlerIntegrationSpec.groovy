package com.messaging.network.codec

import com.messaging.common.model.BrokerMessage
import com.messaging.common.model.ConsumerRecord
import com.messaging.common.model.EventType
import io.micronaut.test.extensions.spock.annotation.MicronautTest
import io.netty.channel.embedded.EmbeddedChannel
import spock.lang.Specification

import java.time.Instant

@MicronautTest(startApplication = false)
class BatchAckHandlerIntegrationSpec extends Specification {

    def "BatchAckHandler sends BATCH_ACK with topic+group payload and forwards records"() {
        given:
        def records = [
            new ConsumerRecord('key1', EventType.MESSAGE, '{"v":1}', Instant.now()),
            new ConsumerRecord('key2', EventType.DELETE, null, Instant.now()),
        ]
        def event = new BatchDecodedEvent(records, 'test-topic', 'test-group')
        def channel = new EmbeddedChannel(new BatchAckHandler())

        when:
        channel.writeInbound(event)

        then:
        BrokerMessage ack = channel.readOutbound()
        ack != null
        ack.type == BrokerMessage.MessageType.BATCH_ACK

        def bb = java.nio.ByteBuffer.wrap(ack.payload)
        def topicLen = bb.getInt()
        def topic = new String(ack.payload, 4, topicLen, 'UTF-8')
        topic == 'test-topic'

        bb.position(4 + topicLen)
        def groupLen = bb.getInt()
        def group = new String(ack.payload, 4 + topicLen + 4, groupLen, 'UTF-8')
        group == 'test-group'

        channel.readInbound() == records

        cleanup:
        channel.close()
    }

    def "BatchAckHandler passes through non-BatchDecodedEvent messages unchanged"() {
        given:
        def channel = new EmbeddedChannel(new BatchAckHandler())
        def msg = new BrokerMessage(BrokerMessage.MessageType.ACK, 1L, new byte[0])

        when:
        channel.writeInbound(msg)

        then:
        channel.readInbound() == msg
        channel.readOutbound() == null

        cleanup:
        channel.close()
    }

    def "BatchAckHandler handles multiple consecutive batches"() {
        given:
        def records1 = [new ConsumerRecord('k1', EventType.MESSAGE, '{}', Instant.now())]
        def records2 = [new ConsumerRecord('k2', EventType.MESSAGE, '{}', Instant.now())]
        def channel = new EmbeddedChannel(new BatchAckHandler())

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
