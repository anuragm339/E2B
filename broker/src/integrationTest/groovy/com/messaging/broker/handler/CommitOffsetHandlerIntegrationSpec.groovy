package com.messaging.broker.handler

import com.messaging.broker.support.BrokerHandlerSpecSupport
import com.messaging.common.model.BrokerMessage
import io.micronaut.test.extensions.spock.annotation.MicronautTest
import spock.util.concurrent.PollingConditions

@MicronautTest
class CommitOffsetHandlerIntegrationSpec extends BrokerHandlerSpecSupport {

    def "CommitOffsetHandler persists valid offset and sends ACK"() {
        given:
        def conditions = new PollingConditions(timeout: 3)
        consumer.send(new BrokerMessage(BrokerMessage.MessageType.DATA, 3000L,
            toJson([msg_key: 'co-key', event_type: 'MESSAGE', data: [v: 1], topic: 'co-topic']).bytes))
        conditions.eventually { assert storage.getCurrentOffset('co-topic', 0) >= 0 }

        when:
        consumer.send(new BrokerMessage(BrokerMessage.MessageType.COMMIT_OFFSET, 3001L,
            toJson([topic: 'co-topic', group: 'co-group', offset: 0]).bytes))

        then:
        conditions.eventually {
            assert consumer.received.any { it.type == BrokerMessage.MessageType.ACK && it.messageId == 3001L }
            assert offsetTracker.getOffset('co-group:co-topic') == 0L
        }
    }

    def "CommitOffsetHandler closes connection on negative offset"() {
        given:
        def conditions = new PollingConditions(timeout: 3)
        consumer.send(new BrokerMessage(BrokerMessage.MessageType.DATA, 3100L,
            toJson([msg_key: 'co-neg-key', event_type: 'MESSAGE', data: [v: 1], topic: 'co-neg-topic']).bytes))
        conditions.eventually { assert storage.getCurrentOffset('co-neg-topic', 0) >= 0 }

        when:
        consumer.send(new BrokerMessage(BrokerMessage.MessageType.COMMIT_OFFSET, 3101L,
            toJson([topic: 'co-neg-topic', group: 'co-neg-group', offset: -1]).bytes))

        then:
        conditions.eventually { assert !consumer.connected }
    }

    def "CommitOffsetHandler clamps offset exceeding storage head"() {
        given:
        def conditions = new PollingConditions(timeout: 3)
        consumer.send(new BrokerMessage(BrokerMessage.MessageType.DATA, 3200L,
            toJson([msg_key: 'co-clamp-key', event_type: 'MESSAGE', data: [v: 1], topic: 'co-clamp-topic']).bytes))
        conditions.eventually { assert storage.getCurrentOffset('co-clamp-topic', 0) >= 0 }
        long head = storage.getCurrentOffset('co-clamp-topic', 0)

        when:
        consumer.send(new BrokerMessage(BrokerMessage.MessageType.COMMIT_OFFSET, 3201L,
            toJson([topic: 'co-clamp-topic', group: 'co-clamp-group', offset: head + 9999]).bytes))

        then:
        conditions.eventually {
            assert consumer.received.any { it.type == BrokerMessage.MessageType.ACK && it.messageId == 3201L }
            assert offsetTracker.getOffset('co-clamp-group:co-clamp-topic') == head
        }
    }

    def "CommitOffsetHandler closes connection when topic field is missing"() {
        when:
        consumer.send(new BrokerMessage(BrokerMessage.MessageType.COMMIT_OFFSET, 3300L,
            toJson([group: 'co-miss-group', offset: 0]).bytes))

        then:
        new PollingConditions(timeout: 3).eventually { assert !consumer.connected }
    }
}
