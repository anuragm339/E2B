package com.messaging.common.exception

import spock.lang.Specification

class DataRefreshExceptionSpec extends Specification {

    def "timeout stores refresh id topic and timeout seconds"() {
        when:
        def ex = DataRefreshException.timeout('refresh-1', 'prices-v1', 120L)

        then:
        ex.errorCode == ErrorCode.DATA_REFRESH_TIMEOUT
        ex.context.refreshId == 'refresh-1'
        ex.context.topic == 'prices-v1'
        ex.context.timeoutSeconds == 120L
    }

    def "replayFailed stores replay range and cause"() {
        given:
        def cause = new IllegalStateException('read failed')

        when:
        def ex = DataRefreshException.replayFailed('refresh-2', 'orders-v1', 100L, 200L, cause)

        then:
        ex.errorCode == ErrorCode.DATA_REFRESH_REPLAY_FAILED
        ex.context.startOffset == 100L
        ex.context.endOffset == 200L
        ex.cause.is(cause)
    }
}
