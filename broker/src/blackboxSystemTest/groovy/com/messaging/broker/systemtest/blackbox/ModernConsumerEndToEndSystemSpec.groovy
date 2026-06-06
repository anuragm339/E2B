package com.messaging.broker.systemtest.blackbox

import com.messaging.broker.systemtest.support.ProcessBackedBrokerSystemTestSupport

class ModernConsumerEndToEndSystemSpec extends ProcessBackedBrokerSystemTestSupport {

    def "broker and consumer exchange data across real process boundaries"() {
        when: "the mock cloud publishes a real broker record"
        cloudServer.enqueueMessages([
            [offset: 1L, topic: 'prices-v1', partition: 0,
             msgKey: 'bbx-1', eventType: 'MESSAGE', data: '{"value":1}'],
            [offset: 2L, topic: 'prices-v1', partition: 0,
             msgKey: 'bbx-2', eventType: 'MESSAGE', data: '{"value":2}'],
        ])

        then: "the external consumer process logs receipt of the batch"
        waitForAnyLogContains(consumerProcess, ['event=consumer.progress', 'event=consumer.batch_received'])
        waitForLogContains(consumerProcess, 'firstKey=bbx-1')
        waitForLogContains(consumerProcess, 'lastKey=bbx-2')

        and: "the broker observes the consumer protocol handshake"
        waitForLogContains(brokerProcess, 'event=ready_ack.processed mode=modern')

        and: "the broker persists the topic data on disk"
        waitForFileSize(
            brokerDataDir.resolve('prices-v1/partition-0/00000000000000000000.log')
        ) { size ->
            size > 0L
        }

        and: "the broker process remains alive throughout the flow"
        assert brokerProcess.isAlive()
        assert consumerProcess.isAlive()
    }
}
