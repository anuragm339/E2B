package com.messaging.broker.handler

import com.messaging.broker.support.BrokerHandlerSpecSupport
import com.messaging.common.model.BrokerMessage
import io.micronaut.test.extensions.spock.annotation.MicronautTest
import spock.util.concurrent.PollingConditions

@MicronautTest
class SubscribeHandlerIntegrationSpec extends BrokerHandlerSpecSupport {

    def "SubscribeHandler registers consumer and sends ACK plus READY"() {
        when:
        consumer.subscribe('sh-topic', 'sh-group')

        then:
        new PollingConditions(timeout: 3).eventually {
            assert consumer.received.any { it.type == BrokerMessage.MessageType.ACK }
            assert consumer.received.any { it.type == BrokerMessage.MessageType.READY }
            assert remoteConsumers.getAllConsumers().any { it.topic == 'sh-topic' && it.group == 'sh-group' }
        }
    }

    def "SubscribeHandler closes connection when topic is missing"() {
        when:
        consumer.send(new BrokerMessage(BrokerMessage.MessageType.SUBSCRIBE, 2001L,
            toJson([group: 'sh-err-group']).bytes))

        then:
        new PollingConditions(timeout: 3).eventually { assert !consumer.connected }
    }

    def "SubscribeHandler closes connection when group is null"() {
        when:
        consumer.send(new BrokerMessage(BrokerMessage.MessageType.SUBSCRIBE, 2002L,
            '{"topic":"sh-null-topic","group":null}'.bytes))

        then:
        new PollingConditions(timeout: 3).eventually { assert !consumer.connected }
    }

    def "SubscribeHandler duplicate subscription does not double-register"() {
        given:
        def conditions = new PollingConditions(timeout: 3)
        consumer.subscribe('sh-dup-topic', 'sh-dup-group')
        conditions.eventually {
            assert consumer.received.any { it.type == BrokerMessage.MessageType.ACK }
        }
        int countBefore = remoteConsumers.getAllConsumers().count { it.topic == 'sh-dup-topic' && it.group == 'sh-dup-group' }

        when:
        consumer.received.clear()
        consumer.subscribe('sh-dup-topic', 'sh-dup-group')

        then:
        conditions.eventually {
            assert consumer.received.any { it.type == BrokerMessage.MessageType.ACK }
        }
        remoteConsumers.getAllConsumers().count { it.topic == 'sh-dup-topic' && it.group == 'sh-dup-group' } == countBefore
    }
}
