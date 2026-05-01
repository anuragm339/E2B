package com.messaging.network.codec

import com.messaging.common.model.BrokerMessage
import com.messaging.common.model.EventType
import com.messaging.network.metrics.DecodeErrorRecorder
import io.netty.buffer.Unpooled
import io.netty.channel.embedded.EmbeddedChannel
import spock.lang.Specification

import java.nio.ByteBuffer
import java.nio.charset.StandardCharsets

/**
 * Unit tests for ZeroCopyBatchDecoder covering all branch paths in:
 *  - decodeBrokerMessage()
 *  - decodeZeroCopyBatch()
 *  - parseBatchData()
 */
class ZeroCopyBatchDecoderSpec extends Specification {

    // ── Encoding helpers ─────────────────────────────────────────────────────

    /**
     * Build a complete BrokerMessage wire frame.
     * Format: [type:1][messageId:8][payloadLen:4][payload:var]
     */
    static byte[] brokerFrame(BrokerMessage.MessageType type, long msgId, byte[] payload) {
        def buf = ByteBuffer.allocate(13 + payload.length)
        buf.put(type.getCode())
        buf.putLong(msgId)
        buf.putInt(payload.length)
        buf.put(payload)
        buf.array()
    }

    /**
     * Build a single record in parseBatchData() format (used inside inline DATA messages).
     * Format: [offset:8][keyLen:4][key:var][eventType:1][dataLen:4][data:var][createdAt:8][crc32:4]
     * eventType should be 'M' for MESSAGE or 'D' for DELETE (passed as String in Groovy)
     */
    static byte[] inlineRecord(long offset, String key, String eventType, String data, long createdAt) {
        def keyBytes = key.getBytes(StandardCharsets.UTF_8)
        def dataBytes = data != null ? data.getBytes(StandardCharsets.UTF_8) : new byte[0]
        def buf = ByteBuffer.allocate(8 + 4 + keyBytes.length + 1 + 4 + dataBytes.length + 8 + 4)
        buf.putLong(offset)
        buf.putInt(keyBytes.length)
        buf.put(keyBytes)
        buf.put((byte) (eventType as char))
        buf.putInt(dataBytes.length)
        if (dataBytes.length > 0) buf.put(dataBytes)
        buf.putLong(createdAt)
        buf.putInt(0) // crc32
        buf.array()
    }

    /**
     * Build a single record in decodeZeroCopyBatch() format (used after BATCH_HEADER).
     * Format: [keyLen:4][key:var][eventType:1][dataLen:4][data:var][createdAt:8]
     * (no offset, no CRC32)
     * eventType should be 'M' for MESSAGE or 'D' for DELETE (passed as String in Groovy)
     */
    static byte[] batchRecord(String key, String eventType, String data, long createdAt) {
        def keyBytes = key.getBytes(StandardCharsets.UTF_8)
        def dataBytes = data != null ? data.getBytes(StandardCharsets.UTF_8) : new byte[0]
        def buf = ByteBuffer.allocate(4 + keyBytes.length + 1 + 4 + dataBytes.length + 8)
        buf.putInt(keyBytes.length)
        buf.put(keyBytes)
        buf.put((byte) (eventType as char))
        buf.putInt(dataBytes.length)
        if (dataBytes.length > 0) buf.put(dataBytes)
        buf.putLong(createdAt)
        buf.array()
    }

    /**
     * Build a BATCH_HEADER payload.
     * Format: [recordCount:4][totalBytes:8][topicLen:4][topic:var][groupLen:4][group:var]
     */
    static byte[] batchHeaderPayload(int recordCount, long totalBytes, String topic, String group) {
        def topicBytes = topic.getBytes(StandardCharsets.UTF_8)
        def groupBytes = group.getBytes(StandardCharsets.UTF_8)
        def buf = ByteBuffer.allocate(4 + 8 + 4 + topicBytes.length + 4 + groupBytes.length)
        buf.putInt(recordCount)
        buf.putLong(totalBytes)
        buf.putInt(topicBytes.length)
        buf.put(topicBytes)
        buf.putInt(groupBytes.length)
        buf.put(groupBytes)
        buf.array()
    }

    /**
     * Build a zero-copy DATA payload (inline: header + record bytes in one DATA message).
     * Payload format: [recordCount:4][totalBytes:8][lastOffset:8][record_bytes:var]
     */
    static byte[] zeroCopyDataPayload(int recordCount, byte[] recordBytes, long lastOffset) {
        def buf = ByteBuffer.allocate(4 + 8 + 8 + recordBytes.length)
        buf.putInt(recordCount)
        buf.putLong(recordBytes.length) // totalBytes
        buf.putLong(lastOffset)
        buf.put(recordBytes)
        buf.array()
    }

    // ── decodeBrokerMessage — basic message types ────────────────────────────

    def "regular ACK message is decoded as BrokerMessage"() {
        given:
        def channel = new EmbeddedChannel(new ZeroCopyBatchDecoder())
        def frame = brokerFrame(BrokerMessage.MessageType.ACK, 42L, new byte[0])

        when:
        channel.writeInbound(Unpooled.wrappedBuffer(frame))

        then:
        def msg = channel.readInbound() as BrokerMessage
        msg.type == BrokerMessage.MessageType.ACK
        msg.messageId == 42L
        msg.payload.length == 0

        cleanup:
        channel.close()
    }

    def "SUBSCRIBE message with payload is decoded as BrokerMessage"() {
        given:
        def channel = new EmbeddedChannel(new ZeroCopyBatchDecoder())
        def payload = "prices-v1:group-a".getBytes(StandardCharsets.UTF_8)
        def frame = brokerFrame(BrokerMessage.MessageType.SUBSCRIBE, 7L, payload)

        when:
        channel.writeInbound(Unpooled.wrappedBuffer(frame))

        then:
        def msg = channel.readInbound() as BrokerMessage
        msg.type == BrokerMessage.MessageType.SUBSCRIBE
        msg.payload == payload

        cleanup:
        channel.close()
    }

    def "HEARTBEAT with empty payload is decoded as BrokerMessage"() {
        given:
        def channel = new EmbeddedChannel(new ZeroCopyBatchDecoder())
        def frame = brokerFrame(BrokerMessage.MessageType.HEARTBEAT, 1L, new byte[0])

        when:
        channel.writeInbound(Unpooled.wrappedBuffer(frame))

        then:
        def msg = channel.readInbound() as BrokerMessage
        msg.type == BrokerMessage.MessageType.HEARTBEAT

        cleanup:
        channel.close()
    }

    def "DATA message with payload <= 20 bytes decoded as regular BrokerMessage"() {
        // DATA with exactly 20-byte payload is NOT > ZERO_COPY_HEADER_SIZE, so takes the else branch
        given:
        def channel = new EmbeddedChannel(new ZeroCopyBatchDecoder())
        def payload = new byte[20]  // exactly 20 bytes — not > 20
        def frame = brokerFrame(BrokerMessage.MessageType.DATA, 1L, payload)

        when:
        channel.writeInbound(Unpooled.wrappedBuffer(frame))

        then:
        def msg = channel.readInbound() as BrokerMessage
        msg.type == BrokerMessage.MessageType.DATA
        msg.payload.length == 20

        cleanup:
        channel.close()
    }

    // ── decodeBrokerMessage — invalid payload length ─────────────────────────

    def "negative payload length causes NetworkException and records error"() {
        given:
        def errorRecorder = Mock(DecodeErrorRecorder)
        def channel = new EmbeddedChannel(new ZeroCopyBatchDecoder(errorRecorder))

        // Manually craft: type(ACK) + msgId + payloadLen(-1)
        def buf = ByteBuffer.allocate(13)
        buf.put(BrokerMessage.MessageType.ACK.getCode())
        buf.putLong(1L)
        buf.putInt(-1)  // negative payload length

        when:
        channel.writeInbound(Unpooled.wrappedBuffer(buf.array()))

        then:
        thrown(Exception)  // NetworkException propagated through Netty as DecoderException
        1 * errorRecorder.record("unknown", "invalid_payload_length")

        cleanup:
        channel.close()
    }

    def "oversized payload length causes NetworkException and records error"() {
        given:
        def errorRecorder = Mock(DecodeErrorRecorder)
        def channel = new EmbeddedChannel(new ZeroCopyBatchDecoder(errorRecorder))

        // payloadLength > 10MB
        def buf = ByteBuffer.allocate(13)
        buf.put(BrokerMessage.MessageType.ACK.getCode())
        buf.putLong(1L)
        buf.putInt(11 * 1024 * 1024)  // 11MB — exceeds 10MB limit

        when:
        channel.writeInbound(Unpooled.wrappedBuffer(buf.array()))

        then:
        thrown(Exception)
        1 * errorRecorder.record("unknown", "invalid_payload_length")

        cleanup:
        channel.close()
    }

    def "unknown message type code causes a decoding exception"() {
        // NetworkException extends Exception (not RuntimeException), so the catch(RuntimeException)
        // block in decodeBrokerMessage is NOT triggered — the exception propagates directly through
        // Netty's pipeline as a DecoderException.
        given:
        def channel = new EmbeddedChannel(new ZeroCopyBatchDecoder())

        // type=0xFF (unknown), zero payload
        def buf = ByteBuffer.allocate(13)
        buf.put((byte) 0xFF)  // unknown type
        buf.putLong(1L)
        buf.putInt(0)

        when:
        channel.writeInbound(Unpooled.wrappedBuffer(buf.array()))

        then:
        thrown(Exception)

        cleanup:
        channel.close()
    }

    def "decoder waits when fewer than 13 header bytes are available"() {
        // Only 5 bytes arrive — decoder should buffer and not emit anything
        given:
        def channel = new EmbeddedChannel(new ZeroCopyBatchDecoder())
        def partial = new byte[5]

        when:
        channel.writeInbound(Unpooled.wrappedBuffer(partial))

        then:
        channel.readInbound() == null  // nothing emitted — waiting for rest of header

        cleanup:
        channel.close()
    }

    def "decoder waits when header arrives but payload is incomplete"() {
        given:
        def channel = new EmbeddedChannel(new ZeroCopyBatchDecoder())

        // Header says payload is 100 bytes but we only send 13-byte header with 0 extra bytes
        def buf = ByteBuffer.allocate(13)
        buf.put(BrokerMessage.MessageType.ACK.getCode())
        buf.putLong(1L)
        buf.putInt(100)  // claims 100 bytes payload, none provided

        when:
        channel.writeInbound(Unpooled.wrappedBuffer(buf.array()))

        then:
        channel.readInbound() == null  // waiting for payload

        cleanup:
        channel.close()
    }

    // ── DATA message — inline zero-copy path (parseBatchData) ────────────────

    def "DATA message with zero-copy header and matching bytes decoded as records"() {
        given:
        def record = inlineRecord(1L, "msg-1", 'M', "data1", 1_000_000L)
        def payload = zeroCopyDataPayload(1, record, 1L)
        def frame = brokerFrame(BrokerMessage.MessageType.DATA, 1L, payload)
        def channel = new EmbeddedChannel(new ZeroCopyBatchDecoder())

        when:
        channel.writeInbound(Unpooled.wrappedBuffer(frame))

        then:
        def records = channel.readInbound() as List
        records.size() == 1
        records[0].msgKey == "msg-1"
        records[0].eventType == EventType.MESSAGE
        records[0].data == "data1"

        cleanup:
        channel.close()
    }

    def "parseBatchData - DELETE event type is decoded correctly"() {
        given:
        def record = inlineRecord(5L, "key-del", 'D', null, 2_000_000L)
        def payload = zeroCopyDataPayload(1, record, 5L)
        def frame = brokerFrame(BrokerMessage.MessageType.DATA, 1L, payload)
        def channel = new EmbeddedChannel(new ZeroCopyBatchDecoder())

        when:
        channel.writeInbound(Unpooled.wrappedBuffer(frame))

        then:
        def records = channel.readInbound() as List
        records.size() == 1
        records[0].eventType == EventType.DELETE
        records[0].data == null

        cleanup:
        channel.close()
    }

    def "parseBatchData - dataLen == 0 produces record with null data"() {
        given:
        // dataLen=0 means the data branch (dataLen > 0) is skipped → data stays null
        def record = inlineRecord(1L, "empty-data", 'M', null, 999L)
        def payload = zeroCopyDataPayload(1, record, 1L)
        def frame = brokerFrame(BrokerMessage.MessageType.DATA, 1L, payload)
        def channel = new EmbeddedChannel(new ZeroCopyBatchDecoder())

        when:
        channel.writeInbound(Unpooled.wrappedBuffer(frame))

        then:
        def records = channel.readInbound() as List
        records.size() == 1
        records[0].data == null

        cleanup:
        channel.close()
    }

    def "parseBatchData - multiple records in a single DATA frame are all decoded"() {
        given:
        def r1 = inlineRecord(1L, "k1", 'M', "v1", 1000L)
        def r2 = inlineRecord(2L, "k2", 'D', null, 2000L)
        def r3 = inlineRecord(3L, "k3", 'M', "v3", 3000L)
        def combined = new byte[r1.length + r2.length + r3.length]
        System.arraycopy(r1, 0, combined, 0, r1.length)
        System.arraycopy(r2, 0, combined, r1.length, r2.length)
        System.arraycopy(r3, 0, combined, r1.length + r2.length, r3.length)
        def payload = zeroCopyDataPayload(3, combined, 3L)
        def frame = brokerFrame(BrokerMessage.MessageType.DATA, 1L, payload)
        def channel = new EmbeddedChannel(new ZeroCopyBatchDecoder())

        when:
        channel.writeInbound(Unpooled.wrappedBuffer(frame))

        then:
        def records = channel.readInbound() as List
        records.size() == 3
        records[0].msgKey == "k1"
        records[1].eventType == EventType.DELETE
        records[2].msgKey == "k3"

        cleanup:
        channel.close()
    }

    def "parseBatchData - invalid keyLen records error and stops parsing"() {
        given:
        def errorRecorder = Mock(DecodeErrorRecorder)
        def channel = new EmbeddedChannel(new ZeroCopyBatchDecoder(errorRecorder))

        // Craft a record with keyLen = -1 (invalid)
        def badRecord = ByteBuffer.allocate(8 + 4)  // offset + bad keyLen only
        badRecord.putLong(1L)         // offset
        badRecord.putInt(-1)          // invalid keyLen

        // Build the payload so remainingPayload == badRecord.length
        def recordBytes = badRecord.array()
        def payload = zeroCopyDataPayload(1, recordBytes, 1L)
        def frame = brokerFrame(BrokerMessage.MessageType.DATA, 1L, payload)

        when:
        channel.writeInbound(Unpooled.wrappedBuffer(frame))

        then:
        def records = channel.readInbound() as List
        records.isEmpty()  // invalid keyLen → break → 0 records returned
        1 * errorRecorder.record("unknown", "invalid_key_length")

        cleanup:
        channel.close()
    }

    def "parseBatchData - invalid dataLen records error and stops parsing"() {
        given:
        def errorRecorder = Mock(DecodeErrorRecorder)
        def channel = new EmbeddedChannel(new ZeroCopyBatchDecoder(errorRecorder))

        def keyBytes = "k".getBytes(StandardCharsets.UTF_8)
        def badRecord = ByteBuffer.allocate(8 + 4 + keyBytes.length + 1 + 4)
        badRecord.putLong(1L)
        badRecord.putInt(keyBytes.length)
        badRecord.put(keyBytes)
        badRecord.put((byte) 'M')
        badRecord.putInt(11 * 1024 * 1024)  // dataLen > 10MB

        def payload = zeroCopyDataPayload(1, badRecord.array(), 1L)
        def frame = brokerFrame(BrokerMessage.MessageType.DATA, 1L, payload)

        when:
        channel.writeInbound(Unpooled.wrappedBuffer(frame))

        then:
        def records = channel.readInbound() as List
        records.isEmpty()
        1 * errorRecorder.record("unknown", "invalid_data_length")

        cleanup:
        channel.close()
    }

    def "DATA message where remainingPayload != headerTotalBytes is decoded as regular BrokerMessage"() {
        // If remainingPayload != headerTotalBytes the else branch is taken — treated as regular DATA
        given:
        def channel = new EmbeddedChannel(new ZeroCopyBatchDecoder())

        // Build a DATA payload where declared totalBytes (inside payload) != actual remaining bytes
        def buf = ByteBuffer.allocate(4 + 8 + 8 + 10)  // 30 bytes payload
        buf.putInt(1)          // recordCount
        buf.putLong(9999L)     // totalBytes — intentionally does NOT match remaining (10 bytes)
        buf.putLong(1L)        // lastOffset
        buf.put(new byte[10])  // 10 bytes of data (remainingPayload = 10, headerTotalBytes = 9999 → mismatch)
        def payload = buf.array()
        def frame = brokerFrame(BrokerMessage.MessageType.DATA, 1L, payload)

        when:
        channel.writeInbound(Unpooled.wrappedBuffer(frame))

        then:
        def msg = channel.readInbound() as BrokerMessage
        msg.type == BrokerMessage.MessageType.DATA

        cleanup:
        channel.close()
    }

    // ── BATCH_HEADER → decodeZeroCopyBatch path ──────────────────────────────

    def "BATCH_HEADER transitions state — no output until batch data arrives"() {
        given:
        def channel = new EmbeddedChannel(new ZeroCopyBatchDecoder())
        def headerPayload = batchHeaderPayload(1, 30L, "prices-v1", "group-a")
        def headerFrame = brokerFrame(BrokerMessage.MessageType.BATCH_HEADER, 1L, headerPayload)

        when:
        channel.writeInbound(Unpooled.wrappedBuffer(headerFrame))

        then:
        channel.readInbound() == null  // state transitioned, but no output yet

        cleanup:
        channel.close()
    }

    def "BATCH_HEADER then batch data emits BatchDecodedEvent with correct topic and group"() {
        given:
        def record = batchRecord("msg-key", 'M', "payload-data", 5_000_000L)
        def headerPayload = batchHeaderPayload(1, record.length, "prices-v1", "group-a")
        def headerFrame = brokerFrame(BrokerMessage.MessageType.BATCH_HEADER, 1L, headerPayload)
        def channel = new EmbeddedChannel(new ZeroCopyBatchDecoder())

        when:
        channel.writeInbound(Unpooled.wrappedBuffer(headerFrame))
        channel.writeInbound(Unpooled.wrappedBuffer(record))

        then:
        def event = channel.readInbound() as BatchDecodedEvent
        event.topic() == "prices-v1"
        event.group() == "group-a"
        event.records().size() == 1
        event.records()[0].msgKey == "msg-key"
        event.records()[0].eventType == EventType.MESSAGE
        event.records()[0].data == "payload-data"

        cleanup:
        channel.close()
    }

    def "decodeZeroCopyBatch - DELETE event type decoded correctly"() {
        given:
        def record = batchRecord("del-key", 'D', null, 1000L)
        def headerPayload = batchHeaderPayload(1, record.length, "ref-data", "group-b")
        def headerFrame = brokerFrame(BrokerMessage.MessageType.BATCH_HEADER, 1L, headerPayload)
        def channel = new EmbeddedChannel(new ZeroCopyBatchDecoder())

        when:
        channel.writeInbound(Unpooled.wrappedBuffer(headerFrame))
        channel.writeInbound(Unpooled.wrappedBuffer(record))

        then:
        def event = channel.readInbound() as BatchDecodedEvent
        event.records().size() == 1
        event.records()[0].eventType == EventType.DELETE
        event.records()[0].data == null

        cleanup:
        channel.close()
    }

    def "decodeZeroCopyBatch - dataLen == 0 produces null data in record"() {
        given:
        def record = batchRecord("k", 'M', null, 1000L)
        def headerPayload = batchHeaderPayload(1, record.length, "topic", "grp")
        def headerFrame = brokerFrame(BrokerMessage.MessageType.BATCH_HEADER, 1L, headerPayload)
        def channel = new EmbeddedChannel(new ZeroCopyBatchDecoder())

        when:
        channel.writeInbound(Unpooled.wrappedBuffer(headerFrame))
        channel.writeInbound(Unpooled.wrappedBuffer(record))

        then:
        def event = channel.readInbound() as BatchDecodedEvent
        event.records()[0].data == null

        cleanup:
        channel.close()
    }

    def "decodeZeroCopyBatch - multiple records decoded in sequence"() {
        given:
        def r1 = batchRecord("k1", 'M', "v1", 1000L)
        def r2 = batchRecord("k2", 'D', null, 2000L)
        def combined = new byte[r1.length + r2.length]
        System.arraycopy(r1, 0, combined, 0, r1.length)
        System.arraycopy(r2, 0, combined, r1.length, r2.length)
        def headerPayload = batchHeaderPayload(2, combined.length, "prices-v1", "grp")
        def headerFrame = brokerFrame(BrokerMessage.MessageType.BATCH_HEADER, 1L, headerPayload)
        def channel = new EmbeddedChannel(new ZeroCopyBatchDecoder())

        when:
        channel.writeInbound(Unpooled.wrappedBuffer(headerFrame))
        channel.writeInbound(Unpooled.wrappedBuffer(combined))

        then:
        def event = channel.readInbound() as BatchDecodedEvent
        event.records().size() == 2
        event.records()[0].msgKey == "k1"
        event.records()[1].eventType == EventType.DELETE

        cleanup:
        channel.close()
    }

    def "decodeZeroCopyBatch - decoder waits when batch data bytes are incomplete"() {
        given:
        // Declare 100 bytes in header, send only 10 bytes of data
        def headerPayload = batchHeaderPayload(1, 100L, "topic", "grp")
        def headerFrame = brokerFrame(BrokerMessage.MessageType.BATCH_HEADER, 1L, headerPayload)
        def channel = new EmbeddedChannel(new ZeroCopyBatchDecoder())

        when:
        channel.writeInbound(Unpooled.wrappedBuffer(headerFrame))
        channel.writeInbound(Unpooled.wrappedBuffer(new byte[10]))  // partial data

        then:
        channel.readInbound() == null  // still waiting

        cleanup:
        channel.close()
    }

    def "decodeZeroCopyBatch - partial parse closes channel and records error"() {
        // Declare 2 records but provide only enough bytes for 1 → partial parse
        given:
        def errorRecorder = Mock(DecodeErrorRecorder)
        def record = batchRecord("k1", 'M', "v1", 1000L)
        // totalBytes = one record's worth — but recordCount = 2
        def headerPayload = batchHeaderPayload(2, record.length, "prices-v1", "group-a")
        def headerFrame = brokerFrame(BrokerMessage.MessageType.BATCH_HEADER, 1L, headerPayload)
        def channel = new EmbeddedChannel(new ZeroCopyBatchDecoder(errorRecorder))

        when:
        channel.writeInbound(Unpooled.wrappedBuffer(headerFrame))
        channel.writeInbound(Unpooled.wrappedBuffer(record))

        then:
        channel.readInbound() == null       // no BatchDecodedEvent emitted
        !channel.isOpen()                   // channel closed to trigger reconnect
        1 * errorRecorder.record("prices-v1", "partial_batch_parse")

        cleanup:
        channel.close()
    }

    def "decodeZeroCopyBatch - invalid keyLen records error and causes partial parse"() {
        given:
        def errorRecorder = Mock(DecodeErrorRecorder)

        // A record with invalid keyLen (-1)
        def badRecord = ByteBuffer.allocate(4)
        badRecord.putInt(-1)  // invalid keyLen

        def headerPayload = batchHeaderPayload(1, badRecord.array().length, "prices-v1", "group-a")
        def headerFrame = brokerFrame(BrokerMessage.MessageType.BATCH_HEADER, 1L, headerPayload)
        def channel = new EmbeddedChannel(new ZeroCopyBatchDecoder(errorRecorder))

        when:
        channel.writeInbound(Unpooled.wrappedBuffer(headerFrame))
        channel.writeInbound(Unpooled.wrappedBuffer(badRecord.array()))

        then:
        channel.readInbound() == null
        !channel.isOpen()
        1 * errorRecorder.record("prices-v1", "invalid_key_length")

        cleanup:
        channel.close()
    }

    def "decodeZeroCopyBatch - invalid dataLen records error and causes partial parse"() {
        given:
        def errorRecorder = Mock(DecodeErrorRecorder)
        def keyBytes = "k".getBytes(StandardCharsets.UTF_8)

        def badRecord = ByteBuffer.allocate(4 + keyBytes.length + 1 + 4)
        badRecord.putInt(keyBytes.length)
        badRecord.put(keyBytes)
        badRecord.put((byte) 'M')
        badRecord.putInt(-1)  // invalid dataLen

        def headerPayload = batchHeaderPayload(1, badRecord.array().length, "topic-x", "grp")
        def headerFrame = brokerFrame(BrokerMessage.MessageType.BATCH_HEADER, 1L, headerPayload)
        def channel = new EmbeddedChannel(new ZeroCopyBatchDecoder(errorRecorder))

        when:
        channel.writeInbound(Unpooled.wrappedBuffer(headerFrame))
        channel.writeInbound(Unpooled.wrappedBuffer(badRecord.array()))

        then:
        channel.readInbound() == null
        !channel.isOpen()
        1 * errorRecorder.record("topic-x", "invalid_data_length")

        cleanup:
        channel.close()
    }

    def "decoder state resets after a successful batch — next message decoded normally"() {
        // After a full BATCH_HEADER + data cycle, the decoder returns to READING_BROKER_MESSAGE state
        given:
        def record = batchRecord("k1", 'M', "v1", 1000L)
        def headerPayload = batchHeaderPayload(1, record.length, "prices-v1", "grp")
        def headerFrame = brokerFrame(BrokerMessage.MessageType.BATCH_HEADER, 1L, headerPayload)
        def ackFrame = brokerFrame(BrokerMessage.MessageType.ACK, 99L, new byte[0])
        def channel = new EmbeddedChannel(new ZeroCopyBatchDecoder())

        when:
        channel.writeInbound(Unpooled.wrappedBuffer(headerFrame))
        channel.writeInbound(Unpooled.wrappedBuffer(record))
        channel.writeInbound(Unpooled.wrappedBuffer(ackFrame))  // follow-up message

        then:
        channel.readInbound() instanceof BatchDecodedEvent   // first output: batch event
        def msg = channel.readInbound() as BrokerMessage
        msg.type == BrokerMessage.MessageType.ACK            // second output: follow-up ACK
        msg.messageId == 99L

        cleanup:
        channel.close()
    }

    def "constructor with null DecodeErrorRecorder defaults to noop"() {
        // Passing null should not cause NPE — the constructor sets noop() as fallback
        given:
        def channel = new EmbeddedChannel(new ZeroCopyBatchDecoder(null))
        def frame = brokerFrame(BrokerMessage.MessageType.ACK, 1L, new byte[0])

        when:
        channel.writeInbound(Unpooled.wrappedBuffer(frame))

        then:
        noExceptionThrown()
        channel.readInbound() instanceof BrokerMessage

        cleanup:
        channel.close()
    }
}
