package com.messaging.network.legacy.events

import spock.lang.Specification

class EventTypeSpec extends Specification {

    def "get returns the matching enum constant for valid ordinals"() {
        expect:
        EventType.get(0) == EventType.REGISTER
        EventType.get(4) == EventType.ACK
        EventType.get(7) == EventType.BATCH
    }

    def "get throws for invalid ordinals"() {
        when:
        EventType.get(99)

        then:
        def ex = thrown(IllegalArgumentException)
        ex.message.contains('Invalid event type ordinal')
    }
}
