package com.messaging.common.model

import io.micronaut.test.extensions.spock.annotation.MicronautTest
import spock.lang.Specification

import java.nio.channels.Channels

@MicronautTest(startApplication = false)
class ByteArrayDeliveryBatchIntegrationSpec extends Specification {

    def "byte array delivery batch transfers bytes and reports completion"() {
        given:
        byte[] payload = 'batch-payload'.bytes
        def batch = new ByteArrayDeliveryBatch('prices-v1', payload, 1, 98L, 99L)
        def output = new ByteArrayOutputStream()
        def channel = Channels.newChannel(output)

        when:
        long written = batch.transferTo(channel, 0)
        long exhausted = batch.transferTo(channel, written)

        then:
        batch.topic == 'prices-v1'
        batch.recordCount == 1
        batch.totalBytes == payload.length
        batch.firstOffset == 98L
        batch.lastOffset == 99L
        written == payload.length
        exhausted == -1L
        output.toByteArray() == payload
    }
}
