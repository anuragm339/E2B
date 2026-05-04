package com.messaging.broker.ack

import spock.lang.Specification

class AckRecordSpec extends Specification {

    def "toBytes and fromBytes round-trip the fixed binary format"() {
        given:
        def record = new AckRecord(42L, 123456789L)

        when:
        def encoded = record.toBytes()
        def decoded = AckRecord.fromBytes(encoded)

        then:
        encoded.length == 16
        decoded.offset == 42L
        decoded.ackedAtMs == 123456789L
        decoded.toString().contains('offset=42')
    }
}
