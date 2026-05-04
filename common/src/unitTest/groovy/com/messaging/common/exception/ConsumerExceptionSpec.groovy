package com.messaging.common.exception

import spock.lang.Specification

class ConsumerExceptionSpec extends Specification {

    def "notRegistered sets group and topic context"() {
        when:
        def ex = ConsumerException.notRegistered('price-quote-service', 'prices-v1')

        then:
        ex.errorCode == ErrorCode.CONSUMER_NOT_REGISTERED
        ex.context.consumerGroup == 'price-quote-service'
        ex.context.topic == 'prices-v1'
    }

    def "batchDeliveryFailed records consumer id topic and batch size"() {
        when:
        def ex = ConsumerException.batchDeliveryFailed('consumer-1', 'prices-v1', 250, new RuntimeException('socket closed'))

        then:
        ex.errorCode == ErrorCode.CONSUMER_BATCH_DELIVERY_FAILED
        ex.context.consumerId == 'consumer-1'
        ex.context.topic == 'prices-v1'
        ex.context.batchSize == 250
    }
}
