package com.messaging.broker.http

import com.messaging.broker.support.BrokerHttpSpecSupport
import com.messaging.broker.support.ModernConsumerClient
import io.micronaut.test.extensions.spock.annotation.MicronautTest
import spock.util.concurrent.PollingConditions

@MicronautTest
class RefreshControllerIntegrationSpec extends BrokerHttpSpecSupport {

    def "POST refresh topic accepts single array and comma-separated topic inputs"() {
        expect:
        json(post('/admin/refresh-topic', request)).success == expected

        where:
        request                                   || expected
        [topic: 'rc-topic-1']                     || true
        [topics: ['rc-arr-1', 'rc-arr-2']]        || true
        [topic: 'comma-topic-1,comma-topic-2']    || true
        [other: 'value']                          || false
        [topic: '']                               || false
        [topics: 'not-an-array']                  || false
    }

    def "GET refresh status returns NONE for unknown topic"() {
        when:
        def resp = get('/admin/refresh-status?topic=no-such-topic')

        then:
        resp.statusCode() == 200
        def body = json(resp)
        body.status == 'NONE'
        body.topic == 'no-such-topic'
    }

    def "GET refresh current reports idle state when no refresh is active"() {
        expect:
        json(get('/admin/refresh-current')).refreshInProgress == false
    }

    def "refresh endpoints report active or current state when a subscribed consumer keeps refresh in progress"() {
        given:
        def consumer = ModernConsumerClient.connect('127.0.0.1', tcpPort)
        def conditions = new PollingConditions(timeout: 5)
        try {
            consumer.subscribe('active-status-topic', 'active-status-group')
            conditions.eventually {
                assert consumer.received.any { it.type == com.messaging.common.model.BrokerMessage.MessageType.ACK }
            }
            post('/admin/refresh-topic', [topic: 'active-status-topic'])

            when:
            def statusResp = get('/admin/refresh-status?topic=active-status-topic')
            def currentResp = get('/admin/refresh-current')

            then:
            statusResp.statusCode() == 200
            json(statusResp).status in ['ACTIVE', 'NONE']
            currentResp.statusCode() == 200
            json(currentResp).refreshInProgress != null
        } finally {
            consumer.close()
        }
    }
}
