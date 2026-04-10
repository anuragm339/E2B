package com.messaging.broker.consumer

import spock.lang.Specification

class RefreshResultSpec extends Specification {

    def "success factory preserves topic state and consumer count"() {
        when:
        def result = RefreshResult.success('prices-v1', RefreshState.REPLAYING, 3)

        then:
        result.success
        result.topic == 'prices-v1'
        result.state == RefreshState.REPLAYING
        result.consumerCount == 3
        result.error == null
    }

    def "error factory creates a failed result without topic state"() {
        when:
        def result = RefreshResult.error('refresh already running')

        then:
        !result.success
        result.topic == null
        result.state == null
        result.consumerCount == 0
        result.error == 'refresh already running'
    }
}
