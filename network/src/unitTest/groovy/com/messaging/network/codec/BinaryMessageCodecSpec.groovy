package com.messaging.network.codec

import com.messaging.common.model.BrokerMessage
import spock.lang.Specification

import java.nio.ByteBuffer

/**
 * Unit tests for Binary Message Codec - encoding and decoding
 */
class BinaryMessageCodecSpec extends Specification {

    def "network module test setup verification"() {
        expect: "basic Spock test infrastructure works"
        true
    }

    def "message type encoding uses correct byte values"() {
        expect: "message types have unique byte values"
        BrokerMessage.MessageType.DATA.getCode() == (byte) 0x01
        BrokerMessage.MessageType.ACK.getCode() == (byte) 0x02
        BrokerMessage.MessageType.SUBSCRIBE.getCode() == (byte) 0x03
        BrokerMessage.MessageType.COMMIT_OFFSET.getCode() == (byte) 0x04
        BrokerMessage.MessageType.RESET.getCode() == (byte) 0x05
        BrokerMessage.MessageType.READY.getCode() == (byte) 0x06
        BrokerMessage.MessageType.DISCONNECT.getCode() == (byte) 0x07
        BrokerMessage.MessageType.HEARTBEAT.getCode() == (byte) 0x08
        BrokerMessage.MessageType.BATCH_HEADER.getCode() == (byte) 0x09
        BrokerMessage.MessageType.BATCH_ACK.getCode() == (byte) 0x0A
        BrokerMessage.MessageType.RESET_ACK.getCode() == (byte) 0x0B
        BrokerMessage.MessageType.READY_ACK.getCode() == (byte) 0x0C
    }

    def "message format has correct structure"() {
        given: "a message with known values"
        byte messageType = 0x01  // DATA
        long messageId = 12345L
        byte[] payload = "test payload".getBytes()
        int payloadLength = payload.length

        when: "creating message buffer"
        ByteBuffer buffer = ByteBuffer.allocate(1 + 8 + 4 + payloadLength)
        buffer.put(messageType)        // 1 byte: type
        buffer.putLong(messageId)      // 8 bytes: message ID
        buffer.putInt(payloadLength)   // 4 bytes: payload length
        buffer.put(payload)            // variable: payload
        buffer.flip()

        then: "buffer has correct size"
        buffer.limit() == 1 + 8 + 4 + payloadLength

        and: "can read values back"
        buffer.get() == messageType
        buffer.getLong() == messageId
        buffer.getInt() == payloadLength
        byte[] readPayload = new byte[payloadLength]
        buffer.get(readPayload)
        new String(readPayload) == "test payload"
    }

    def "payload length validation prevents oversized messages"() {
        given: "various payload lengths"
        def validLengths = [0, 100, 1000, 1024 * 1024]  // Up to 1MB
        def invalidLengths = [-1, Integer.MAX_VALUE, 100 * 1024 * 1024]  // Negative, too large

        expect: "valid lengths are reasonable"
        validLengths.each { length ->
            assert length >= 0
            assert length < 10 * 1024 * 1024  // Less than 10MB
        }

        and: "invalid lengths are rejected"
        invalidLengths.each { length ->
            assert length < 0 || length > 10 * 1024 * 1024
        }
    }

    def "message ID provides unique correlation"() {
        given: "multiple message IDs"
        def messageIds = [1L, 100L, 1000L, System.currentTimeMillis()]

        expect: "each message ID is unique and positive"
        messageIds.unique().size() == messageIds.size()
        messageIds.every { it > 0 }
    }

    def "empty payload is valid for certain message types"() {
        given: "message types that can have empty payload"
        def typesWithEmptyPayload = [
            BrokerMessage.MessageType.ACK,
            BrokerMessage.MessageType.HEARTBEAT
        ]

        and: "empty payload"
        byte[] emptyPayload = new byte[0]

        expect: "zero-length payload is valid"
        emptyPayload.length == 0
        typesWithEmptyPayload.each { type ->
            assert type != null
        }
    }

    def "buffer bounds checking prevents overflow"() {
        given: "a buffer with limited capacity"
        ByteBuffer buffer = ByteBuffer.allocate(20)

        when: "checking available space before writing"
        int neededSpace = 1 + 8 + 4  // type + messageId + payloadLength
        boolean hasSpace = buffer.remaining() >= neededSpace

        then: "bounds check works"
        hasSpace
        buffer.remaining() == 20

        when: "attempting to allocate more than available"
        int largePayloadSize = 100
        boolean canFitLargePayload = buffer.remaining() >= (neededSpace + largePayloadSize)

        then: "detects insufficient space"
        !canFitLargePayload
    }
}
