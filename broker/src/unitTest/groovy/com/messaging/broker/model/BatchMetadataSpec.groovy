package com.messaging.broker.model

import spock.lang.Specification

class BatchMetadataSpec extends Specification {

    def "should create single message metadata"() {
        when:
        def metadata = BatchMetadata.singleMessage("topic-1", 0, 100L, 256L)

        then:
        metadata.topic() == "topic-1"
        metadata.partition() == 0
        metadata.startOffset() == 100L
        metadata.endOffset() == 100L
        metadata.messageCount() == 1
        metadata.totalBytes() == 256L
        metadata.isSingleMessage()
        !metadata.isBatch()
    }

    def "should create batch metadata"() {
        when:
        def metadata = BatchMetadata.batch("topic-1", 0, 100L, 110L, 11, 2560L)

        then:
        metadata.topic() == "topic-1"
        metadata.partition() == 0
        metadata.startOffset() == 100L
        metadata.endOffset() == 110L
        metadata.messageCount() == 11
        metadata.totalBytes() == 2560L
        !metadata.isSingleMessage()
        metadata.isBatch()
    }

    def "should calculate offset range correctly"() {
        when:
        def singleMsg = BatchMetadata.singleMessage("topic-1", 0, 100L, 256L)
        def batch = BatchMetadata.batch("topic-1", 0, 100L, 110L, 11, 2560L)

        then:
        singleMsg.offsetRange() == 1L
        batch.offsetRange() == 11L
    }

    def "should calculate average message size"() {
        when:
        def metadata = BatchMetadata.batch("topic-1", 0, 0L, 9L, 10, 1000L)

        then:
        metadata.averageMessageSize() == 100L
    }

    def "should handle zero messages for average size"() {
        when:
        def metadata = new BatchMetadata("topic-1", 0, 0L, 0L, 0, 0L)

        then:
        metadata.averageMessageSize() == 0L
    }

    def "should implement equals correctly"() {
        given:
        def metadata1 = BatchMetadata.batch("topic-1", 0, 100L, 110L, 11, 2560L)
        def metadata2 = BatchMetadata.batch("topic-1", 0, 100L, 110L, 11, 2560L)
        def metadata3 = BatchMetadata.batch("topic-2", 0, 100L, 110L, 11, 2560L)

        expect:
        metadata1 == metadata2
        metadata1 != metadata3
    }

    def "should implement hashCode correctly"() {
        given:
        def metadata1 = BatchMetadata.batch("topic-1", 0, 100L, 110L, 11, 2560L)
        def metadata2 = BatchMetadata.batch("topic-1", 0, 100L, 110L, 11, 2560L)

        expect:
        metadata1.hashCode() == metadata2.hashCode()
    }

    def "should reject null topic"() {
        when:
        new BatchMetadata(null, 0, 0L, 10L, 10, 1000L)

        then:
        thrown(NullPointerException)
    }

    def "should reject negative startOffset"() {
        when:
        new BatchMetadata("topic-1", 0, -1L, 10L, 10, 1000L)

        then:
        def e = thrown(IllegalArgumentException)
        e.message.contains("startOffset cannot be negative")
    }

    def "should reject endOffset less than startOffset"() {
        when:
        new BatchMetadata("topic-1", 0, 100L, 50L, 10, 1000L)

        then:
        def e = thrown(IllegalArgumentException)
        e.message.contains("endOffset")
        e.message.contains("cannot be less than startOffset")
    }

    def "should reject negative messageCount"() {
        when:
        new BatchMetadata("topic-1", 0, 0L, 10L, -1, 1000L)

        then:
        def e = thrown(IllegalArgumentException)
        e.message.contains("messageCount cannot be negative")
    }

    def "should reject negative totalBytes"() {
        when:
        new BatchMetadata("topic-1", 0, 0L, 10L, 10, -1L)

        then:
        def e = thrown(IllegalArgumentException)
        e.message.contains("totalBytes cannot be negative")
    }

    def "should allow equal startOffset and endOffset for single message"() {
        when:
        def metadata = new BatchMetadata("topic-1", 0, 100L, 100L, 1, 256L)

        then:
        metadata.isSingleMessage()
        metadata.offsetRange() == 1L
    }

    def "should handle large batch"() {
        when:
        def metadata = BatchMetadata.batch(
            "prices-v1",
            0,
            0L,
            999999L,
            1000000,
            100000000L
        )

        then:
        metadata.messageCount() == 1000000
        metadata.totalBytes() == 100000000L
        metadata.offsetRange() == 1000000L
        metadata.averageMessageSize() == 100L
    }

    def "should handle real-world scenario"() {
        when:
        def metadata = BatchMetadata.batch(
            "prices-v1",
            0,
            12345L,
            12354L,
            10,
            5120L
        )

        then:
        metadata.topic() == "prices-v1"
        metadata.partition() == 0
        metadata.startOffset() == 12345L
        metadata.endOffset() == 12354L
        metadata.messageCount() == 10
        metadata.totalBytes() == 5120L
        metadata.isBatch()
        metadata.offsetRange() == 10L
        metadata.averageMessageSize() == 512L
    }

    def "should distinguish between single message and batch"() {
        when:
        def single = BatchMetadata.singleMessage("topic-1", 0, 100L, 256L)
        def batch = BatchMetadata.batch("topic-1", 0, 100L, 101L, 2, 512L)

        then:
        single.isSingleMessage()
        !single.isBatch()
        !batch.isSingleMessage()
        batch.isBatch()
    }
}
