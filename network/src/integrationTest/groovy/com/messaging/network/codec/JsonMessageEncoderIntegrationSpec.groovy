package com.messaging.network.codec

import com.messaging.common.model.BrokerMessage
import io.micronaut.test.extensions.spock.annotation.MicronautTest
import io.netty.buffer.ByteBuf
import io.netty.channel.embedded.EmbeddedChannel
import spock.lang.Specification

@MicronautTest(startApplication = false)
class JsonMessageEncoderIntegrationSpec extends Specification {

    def "JSON encoder output is newline-delimited and contains type and messageId fields"() {
        given:
        def channel = new EmbeddedChannel(new JsonMessageEncoder())
        def msg = new BrokerMessage(BrokerMessage.MessageType.ACK, 1L, new byte[0])

        when:
        channel.writeOutbound(msg)
        ByteBuf buf = channel.readOutbound()
        byte[] bytes = new byte[buf.readableBytes()]
        buf.readBytes(bytes)
        def text = new String(bytes, 'UTF-8')

        then:
        text.endsWith('\n')
        text.contains('"type"')
        text.contains('"messageId"')

        cleanup:
        buf?.release()
        channel.close()
    }
}
