package com.messaging.common.exception

import org.slf4j.Logger
import spock.lang.Specification

class ExceptionLoggerSpec extends Specification {

    def "logConditional uses warn for retriable exceptions"() {
        given:
        Logger log = Mock()
        def ex = new MessagingException(ErrorCode.STORAGE_IO_ERROR, 'temporary issue')

        when:
        ExceptionLogger.logConditional(log, ex)

        then:
        1 * log.warn({ it.contains('STORAGE_IO_ERROR') }, ex)
        0 * _
    }

    def "logConditional uses error for non-retriable exceptions"() {
        given:
        Logger log = Mock()
        def ex = new MessagingException(ErrorCode.STORAGE_CORRUPTION, 'broken file')

        when:
        ExceptionLogger.logConditional(log, ex)

        then:
        1 * log.error({ it.contains('STORAGE_CORRUPTION') }, ex)
        0 * _
    }

    def "logAndThrow rethrows the same exception instance"() {
        given:
        Logger log = Mock()
        def ex = new MessagingException(ErrorCode.NETWORK_TIMEOUT, 'timeout')

        when:
        ExceptionLogger.logAndThrow(log, ex, ExceptionLogger.LogLevel.INFO)

        then:
        def thrownEx = thrown(MessagingException)
        thrownEx.is(ex)
        1 * log.info({ it.contains('NETWORK_TIMEOUT') }, ex)
    }

    def "monitoring summary contains code category and retriable flag"() {
        given:
        def ex = new MessagingException(ErrorCode.CONSUMER_BATCH_DELIVERY_FAILED, 'send failed')

        when:
        def summary = ExceptionLogger.getMonitoringSummary(ex)

        then:
        summary.contains('error_code=CONSUMER_BATCH_DELIVERY_FAILED')
        summary.contains('category=CONSUMER')
        summary.contains('retriable=true')
    }
}
