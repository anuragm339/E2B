package com.messaging.common.model

import spock.lang.Specification

import java.nio.channels.Channels

class ByteArrayDeliveryBatchSpec extends Specification {

    def "transferTo copies bytes from an arbitrary position"() {
        given:
        def batch = new ByteArrayDeliveryBatch('prices-v1', 'abcdef'.bytes, 2, 10L, 11L)
        def output = new ByteArrayOutputStream()
        def channel = Channels.newChannel(output)

        when:
        def written = batch.transferTo(channel, 2)

        then:
        written == 4L
        output.toString('UTF-8') == 'cdef'
    }

    def "isEmpty reflects record count"() {
        expect:
        new ByteArrayDeliveryBatch('topic', new byte[0], 0, -1L, 0L).isEmpty()
        !new ByteArrayDeliveryBatch('topic', 'x'.bytes, 1, 0L, 0L).isEmpty()
    }
}
