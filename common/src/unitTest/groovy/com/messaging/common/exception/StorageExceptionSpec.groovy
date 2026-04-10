package com.messaging.common.exception

import spock.lang.Specification

class StorageExceptionSpec extends Specification {

    def "offsetOutOfRange populates fields and context"() {
        when:
        def ex = StorageException.offsetOutOfRange('prices-v1', 2, 25L, 10L, 20L)

        then:
        ex.errorCode == ErrorCode.STORAGE_OFFSET_OUT_OF_RANGE
        ex.topic == 'prices-v1'
        ex.partition == 2
        ex.offset == 25L
        ex.context.minOffset == 10L
        ex.context.maxOffset == 20L
    }

    def "writeFailed captures topic partition and cause"() {
        given:
        def cause = new IllegalStateException('disk full')

        when:
        def ex = StorageException.writeFailed('orders-v1', 1, cause)

        then:
        ex.errorCode == ErrorCode.STORAGE_WRITE_FAILED
        ex.topic == 'orders-v1'
        ex.partition == 1
        ex.cause.is(cause)
    }
}
