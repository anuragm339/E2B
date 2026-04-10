package com.messaging.network.codec

import com.messaging.common.model.BrokerMessage
import com.messaging.common.model.EventType
import io.micronaut.test.extensions.spock.annotation.MicronautTest
import io.netty.buffer.Unpooled
import io.netty.channel.embedded.EmbeddedChannel
import spock.lang.Specification

import java.nio.ByteBuffer
import java.time.Instant

@MicronautTest(startApplication = false)
class ZeroCopyBatchDecoderIntegrationSpec extends Specification {

    def "decoder turns batch header plus raw payload into BatchDecodedEvent"() {
        given:
        def channel = new EmbeddedChannel(new ZeroCopyBatchDecoder())
        def payload = batchPayload('key-1', 'test-group', '{"v":1}')
        def header = batchHeader('prices-v1', 'test-group', 1, payload.length)

        when:
        channel.writeInbound(Unpooled.wrappedBuffer(header))
        channel.writeInbound(Unpooled.wrappedBuffer(payload))
        def decoded = channel.readInbound() as BatchDecodedEvent

        then:
        decoded.topic() == 'prices-v1'
        decoded.group() == 'test-group'
        decoded.records().size() == 1
        decoded.records()[0].msgKey == 'key-1'
        decoded.records()[0].eventType == EventType.MESSAGE

        cleanup:
        channel.close()
    }

    private static byte[] batchHeader(String topic, String group, int recordCount, long totalBytes) {
        byte[] topicBytes = topic.getBytes('UTF-8')
        byte[] groupBytes = group.getBytes('UTF-8')
        ByteBuffer payload = ByteBuffer.allocate(4 + 8 + 4 + topicBytes.length + 4 + groupBytes.length)
        payload.putInt(recordCount)
        payload.putLong(totalBytes)
        payload.putInt(topicBytes.length)
        payload.put(topicBytes)
        payload.putInt(groupBytes.length)
        payload.put(groupBytes)

        byte[] payloadBytes = payload.array()
        ByteBuffer frame = ByteBuffer.allocate(1 + 8 + 4 + payloadBytes.length)
        frame.put(BrokerMessage.MessageType.BATCH_HEADER.code)
        frame.putLong(1L)
        frame.putInt(payloadBytes.length)
        frame.put(payloadBytes)
        frame.array()
    }

    private static byte[] batchPayload(String key, String group, String data) {
        byte[] keyBytes = key.getBytes('UTF-8')
        byte[] dataBytes = data.getBytes('UTF-8')
        ByteBuffer payload = ByteBuffer.allocate(4 + keyBytes.length + 1 + 4 + dataBytes.length + 8)
        payload.putInt(keyBytes.length)
        payload.put(keyBytes)
        payload.put((byte) 'M')
        payload.putInt(dataBytes.length)
        payload.put(dataBytes)
        payload.putLong(Instant.parse('2024-01-01T00:00:00Z').toEpochMilli())
        payload.array()
    }
}
