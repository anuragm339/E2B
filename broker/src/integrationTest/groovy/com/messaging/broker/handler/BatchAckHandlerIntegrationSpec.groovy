package com.messaging.broker.handler

import com.messaging.broker.support.BrokerHandlerSpecSupport
import com.messaging.common.model.BrokerMessage
import io.micronaut.test.extensions.spock.annotation.MicronautTest
import spock.util.concurrent.PollingConditions

import java.nio.ByteBuffer
import java.nio.charset.StandardCharsets

@MicronautTest
class BatchAckHandlerIntegrationSpec extends BrokerHandlerSpecSupport {

    def "BatchAckHandler processes modern BATCH_ACK after delivery"() {
        given:
        def conditions = new PollingConditions(timeout: 5)
        consumer.send(new BrokerMessage(BrokerMessage.MessageType.DATA, 4000L,
            toJson([msg_key: 'ba-key', event_type: 'MESSAGE', data: [v: 1], topic: 'ba-topic']).bytes))
        conditions.eventually { assert storage.getCurrentOffset('ba-topic', 0) >= 0 }
        consumer.subscribe('ba-topic', 'ba-group')
        conditions.eventually {
            assert consumer.received.any { it.type == BrokerMessage.MessageType.READY }
        }
        consumer.sendReadyAck('ba-topic', 'ba-group')
        conditions.eventually {
            assert consumer.received.any { it.type == BrokerMessage.MessageType.DATA }
        }

        when:
        def topicBytes = 'ba-topic'.getBytes(StandardCharsets.UTF_8)
        def groupBytes = 'ba-group'.getBytes(StandardCharsets.UTF_8)
        def buf = ByteBuffer.allocate(4 + topicBytes.length + 4 + groupBytes.length)
        buf.putInt(topicBytes.length)
        buf.put(topicBytes)
        buf.putInt(groupBytes.length)
        buf.put(groupBytes)
        consumer.send(new BrokerMessage(BrokerMessage.MessageType.BATCH_ACK, 4001L, buf.array()))

        then:
        conditions.eventually {
            assert offsetTracker.getOffset('ba-group:ba-topic') >= 0
        }
    }
}
