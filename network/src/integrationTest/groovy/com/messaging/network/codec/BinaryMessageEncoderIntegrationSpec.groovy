package com.messaging.network.codec

import com.messaging.common.model.BrokerMessage
import io.micronaut.test.extensions.spock.annotation.MicronautTest
import io.netty.buffer.ByteBuf
import io.netty.channel.embedded.EmbeddedChannel
import spock.lang.Specification

@MicronautTest(startApplication = false)
class BinaryMessageEncoderIntegrationSpec extends Specification {

    def "binary encoder produces a 13-byte header followed by the payload"() {
        given:
        def channel = new EmbeddedChannel(new BinaryMessageEncoder())
        def msg = new BrokerMessage(BrokerMessage.MessageType.DATA, 42L, 'hello'.bytes)

        when:
        channel.writeOutbound(msg)
        ByteBuf buf = channel.readOutbound()

        then:
        buf != null
        buf.readableBytes() == 1 + 8 + 4 + 5

        cleanup:
        buf?.release()
        channel.close()
    }
}
