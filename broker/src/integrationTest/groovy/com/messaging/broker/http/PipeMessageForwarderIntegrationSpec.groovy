package com.messaging.broker.http

import com.messaging.broker.support.BrokerHttpSpecSupport
import io.micronaut.test.extensions.spock.annotation.MicronautTest

@MicronautTest
class PipeMessageForwarderIntegrationSpec extends BrokerHttpSpecSupport {

    def "PipeMessageForwarder returns empty results for unknown topics"() {
        when:
        def records = pipeForwarder.getMessagesForChild('no-such-topic', 0L, 10)

        then:
        records != null
        records.isEmpty()
        pipeForwarder.getCurrentOffset('any-topic') == 0L
    }

    def "PipeMessageForwarder returns stored records for known topics"() {
        given:
        storage.append('pipe-fwd-topic', 0,
            new com.messaging.common.model.MessageRecord(
                'pf-key', com.messaging.common.model.EventType.MESSAGE, '{"v":1}', java.time.Instant.now()))

        when:
        def records = pipeForwarder.getMessagesForChild('pipe-fwd-topic', 0L, 10)

        then:
        records.size() == 1
        records[0].msgKey == 'pf-key'
    }
}
