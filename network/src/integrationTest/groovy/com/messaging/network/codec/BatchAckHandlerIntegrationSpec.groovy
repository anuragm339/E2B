package com.messaging.network.codec

import com.messaging.common.model.BrokerMessage
import com.messaging.common.model.ConsumerRecord
import com.messaging.common.model.EventType
import io.micronaut.test.extensions.spock.annotation.MicronautTest
import io.netty.channel.embedded.EmbeddedChannel
import spock.lang.Specification

import java.time.Instant

/**
 * #1 (at-least-once): BatchAckHandler must NOT send a BATCH_ACK — that ack moved to the application
 * layer (ClientConsumerManager) and is now sent only AFTER successful processing. Here the handler's
 * sole job is to unwrap the BatchDecodedEvent into its records and forward them downstream; no
 * outbound message must ever be produced.
 */
@MicronautTest(startApplication = false)
class BatchAckHandlerIntegrationSpec extends Specification {

    def "BatchAckHandler forwards records and does NOT send a BATCH_ACK (#1)"() {
        given:
        def records = [
            new ConsumerRecord('key1', EventType.MESSAGE, '{"v":1}', Instant.now()),
            new ConsumerRecord('key2', EventType.DELETE, null, Instant.now()),
        ]
        def event = new BatchDecodedEvent(records, 'test-topic', 'test-group')
        def channel = new EmbeddedChannel(new BatchAckHandler())

        when:
        channel.writeInbound(event)

        then: "the unwrapped records are forwarded downstream"
        channel.readInbound() == records

        and: "no BATCH_ACK (or any other outbound message) is emitted on the wire"
        channel.readOutbound() == null

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

    def "BatchAckHandler unwraps multiple consecutive batches without acking"() {
        given:
        def records1 = [new ConsumerRecord('k1', EventType.MESSAGE, '{}', Instant.now())]
        def records2 = [new ConsumerRecord('k2', EventType.MESSAGE, '{}', Instant.now())]
        def channel = new EmbeddedChannel(new BatchAckHandler())

        when:
        channel.writeInbound(new BatchDecodedEvent(records1, 'topic-a', 'grp'))
        channel.writeInbound(new BatchDecodedEvent(records2, 'topic-b', 'grp'))

        then: "both batches are forwarded as their record lists"
        channel.readInbound() == records1
        channel.readInbound() == records2

        and: "and nothing is sent back to the broker"
        channel.readOutbound() == null

        cleanup:
        channel.close()
    }
}
