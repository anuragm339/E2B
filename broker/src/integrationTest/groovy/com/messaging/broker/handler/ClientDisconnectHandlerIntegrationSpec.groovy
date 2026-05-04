package com.messaging.broker.handler

import com.messaging.broker.support.BrokerHandlerSpecSupport
import io.micronaut.test.extensions.spock.annotation.MicronautTest
import spock.util.concurrent.PollingConditions

@MicronautTest
class ClientDisconnectHandlerIntegrationSpec extends BrokerHandlerSpecSupport {

    def "ClientDisconnectHandler unregisters consumer on disconnect"() {
        given:
        def conditions = new PollingConditions(timeout: 3)
        consumer.subscribe('cd-topic', 'cd-group')
        conditions.eventually {
            assert remoteConsumers.getAllConsumers().any { it.topic == 'cd-topic' && it.group == 'cd-group' }
        }

        when:
        consumer.close()
        consumer = null

        then:
        conditions.eventually {
            assert !remoteConsumers.getAllConsumers().any { it.topic == 'cd-topic' && it.group == 'cd-group' }
        }
    }
}
