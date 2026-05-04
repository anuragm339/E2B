package com.messaging.network.legacy.events

import spock.lang.Specification

class EventFactorySpec extends Specification {

    def "create returns EOFEvent singleton at end of stream"() {
        given:
        def input = new DataInputStream(new ByteArrayInputStream(new byte[0]))

        when:
        def event = EventFactory.create(input)

        then:
        event.is(EOFEvent.INSTANCE)
    }

    def "create parses a register event from the wire"() {
        given:
        def bytes = new ByteArrayOutputStream()
        def out = new DataOutputStream(bytes)
        out.writeByte(EventType.REGISTER.ordinal())
        out.writeInt(2)
        out.writeUTF('consumer-a')
        out.flush()

        when:
        def event = EventFactory.create(new DataInputStream(new ByteArrayInputStream(bytes.toByteArray())))

        then:
        event instanceof RegisterEvent
        event.version == 2
        event.clientId == 'consumer-a'
    }

    def "create parses an ack event from the wire"() {
        given:
        def bytes = new byte[] { (byte) EventType.ACK.ordinal() }

        when:
        def event = EventFactory.create(new DataInputStream(new ByteArrayInputStream(bytes)))

        then:
        event.is(AckEvent.INSTANCE)
    }
}
