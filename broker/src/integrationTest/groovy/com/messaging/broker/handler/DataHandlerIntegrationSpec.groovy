package com.messaging.broker.handler

import com.messaging.broker.support.BrokerHandlerSpecSupport
import com.messaging.common.model.BrokerMessage
import com.messaging.common.model.EventType
import io.micronaut.test.extensions.spock.annotation.MicronautTest
import spock.util.concurrent.PollingConditions

@MicronautTest
class DataHandlerIntegrationSpec extends BrokerHandlerSpecSupport {

    def "DataHandler stores message and sends ACK"() {
        when:
        consumer.send(new BrokerMessage(BrokerMessage.MessageType.DATA, 1001L,
            toJson([msg_key: 'dh-key-1', event_type: 'MESSAGE', data: [v: 1], topic: 'dh-store-topic']).bytes))

        then:
        new PollingConditions(timeout: 3).eventually {
            assert consumer.received.any { it.type == BrokerMessage.MessageType.ACK && it.messageId == 1001L }
            def records = storage.read('dh-store-topic', 0, 0, 10)
            assert records.size() == 1
            assert records[0].msgKey == 'dh-key-1'
            assert records[0].eventType == EventType.MESSAGE
        }
    }

    def "DataHandler stores DELETE event with null data"() {
        when:
        consumer.send(new BrokerMessage(BrokerMessage.MessageType.DATA, 1002L,
            toJson([msg_key: 'dh-key-del', event_type: 'DELETE', topic: 'dh-delete-topic']).bytes))

        then:
        new PollingConditions(timeout: 3).eventually {
            assert consumer.received.any { it.type == BrokerMessage.MessageType.ACK && it.messageId == 1002L }
            def records = storage.read('dh-delete-topic', 0, 0, 10)
            assert records.size() == 1
            assert records[0].eventType == EventType.DELETE
            assert records[0].data == null
        }
    }

    def "DataHandler closes connection when topic field is missing"() {
        when:
        consumer.send(new BrokerMessage(BrokerMessage.MessageType.DATA, 1003L,
            toJson([msg_key: 'dh-key-notopic', event_type: 'MESSAGE', data: [v: 1]]).bytes))

        then:
        new PollingConditions(timeout: 3).eventually {
            assert !consumer.connected
        }
    }
}
