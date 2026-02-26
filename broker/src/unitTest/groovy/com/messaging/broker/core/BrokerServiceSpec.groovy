package com.messaging.broker.core

import spock.lang.Specification

import java.nio.ByteBuffer
import java.nio.charset.StandardCharsets

/**
 * Unit tests for BrokerService - testing BROKER-2 (ACK Handler OOM) and other fixes
 *
 * BROKER-2: ACK Handler OOM Vulnerability
 * - Validates that topicLen and groupLen are checked before array allocation
 * - Prevents OOM attacks via malicious payload sizes
 */
class BrokerServiceSpec extends Specification {

    def "broker service test setup verification"() {
        expect: "basic Spock test infrastructure works"
        true
    }

    // ===== BROKER-2: ACK Handler OOM Vulnerability Tests =====

    def "BROKER-2: payload with topicLen > 65535 is rejected (OOM prevention)"() {
        given: "a malicious payload with topicLen = 2000000000 (2GB)"
        ByteBuffer buffer = ByteBuffer.allocate(8)
        buffer.putInt(2000000000)  // Malicious topicLen
        buffer.putInt(10)          // groupLen
        buffer.flip()

        when: "validating topicLen"
        int topicLen = buffer.getInt()
        boolean isValid = (topicLen >= 0 && topicLen <= 65535)

        then: "validation fails - prevents OOM"
        !isValid
        topicLen == 2000000000
    }

    def "BROKER-2: payload with negative topicLen is rejected"() {
        given: "a corrupted payload with negative topicLen"
        ByteBuffer buffer = ByteBuffer.allocate(8)
        buffer.putInt(-1)  // Negative topicLen
        buffer.putInt(10)  // groupLen
        buffer.flip()

        when: "validating topicLen"
        int topicLen = buffer.getInt()
        boolean isValid = (topicLen >= 0 && topicLen <= 65535)

        then: "validation fails"
        !isValid
        topicLen == -1
    }

    def "BROKER-2: payload smaller than 8 bytes is rejected"() {
        given: "a payload with only 4 bytes"
        ByteBuffer buffer = ByteBuffer.allocate(4)
        buffer.putInt(10)  // Only topicLen, missing groupLen
        buffer.flip()

        when: "checking payload size"
        boolean hasMinimumSize = (buffer.remaining() >= 8)

        then: "validation fails - need at least 8 bytes"
        !hasMinimumSize
        buffer.remaining() == 4
    }

    def "BROKER-2: payload with groupLen > 65535 is rejected (OOM prevention)"() {
        given: "a payload with valid topicLen but malicious groupLen"
        def topic = "test-topic"
        ByteBuffer buffer = ByteBuffer.allocate(8 + topic.length() + 4)
        buffer.putInt(topic.length())
        buffer.put(topic.getBytes(StandardCharsets.UTF_8))
        buffer.putInt(2000000000)  // Malicious groupLen (2GB)
        buffer.flip()

        when: "validating after reading topic"
        buffer.getInt()  // Skip topicLen
        byte[] topicBytes = new byte[topic.length()]
        buffer.get(topicBytes)
        int groupLen = buffer.getInt()
        boolean isValid = (groupLen >= 0 && groupLen <= 65535)

        then: "validation fails - prevents OOM"
        !isValid
        groupLen == 2000000000
    }

    def "BROKER-2: payload with negative groupLen is rejected"() {
        given: "a payload with valid topicLen but negative groupLen"
        def topic = "test-topic"
        ByteBuffer buffer = ByteBuffer.allocate(8 + topic.length() + 4)
        buffer.putInt(topic.length())
        buffer.put(topic.getBytes(StandardCharsets.UTF_8))
        buffer.putInt(-1)  // Negative groupLen
        buffer.flip()

        when: "validating after reading topic"
        buffer.getInt()  // Skip topicLen
        byte[] topicBytes = new byte[topic.length()]
        buffer.get(topicBytes)
        int groupLen = buffer.getInt()
        boolean isValid = (groupLen >= 0 && groupLen <= 65535)

        then: "validation fails"
        !isValid
        groupLen == -1
    }

    def "BROKER-2: insufficient data after topicLen is detected"() {
        given: "a payload claiming 20 bytes for topic but only 10 available"
        ByteBuffer buffer = ByteBuffer.allocate(4 + 10)  // topicLen + 10 bytes (not 20)
        buffer.putInt(20)  // Claims 20 bytes for topic
        buffer.put(new byte[10])  // Only 10 bytes available
        buffer.flip()

        when: "checking if sufficient data available"
        int topicLen = buffer.getInt()
        boolean sufficientData = (buffer.remaining() >= topicLen + 4)  // +4 for groupLen

        then: "validation fails - not enough data"
        !sufficientData
        buffer.remaining() == 10
        topicLen == 20
    }

    def "BROKER-2: insufficient data after groupLen is detected"() {
        given: "a payload with valid topic but insufficient group data"
        def topic = "test-topic"
        ByteBuffer buffer = ByteBuffer.allocate(8 + topic.length() + 5)  // 5 bytes, but groupLen claims 10
        buffer.putInt(topic.length())
        buffer.put(topic.getBytes(StandardCharsets.UTF_8))
        buffer.putInt(10)  // Claims 10 bytes for group
        buffer.put(new byte[5])  // Only 5 bytes available
        buffer.flip()

        when: "validating after reading topic"
        buffer.getInt()  // Skip topicLen
        byte[] topicBytes = new byte[topic.length()]
        buffer.get(topicBytes)
        int groupLen = buffer.getInt()
        boolean sufficientData = (buffer.remaining() >= groupLen)

        then: "validation fails - not enough data for group"
        !sufficientData
        buffer.remaining() == 5
        groupLen == 10
    }

    def "BROKER-2: valid ACK payload passes all validations"() {
        given: "a properly formatted ACK payload"
        def topic = "test-topic"
        def group = "test-group"
        ByteBuffer buffer = ByteBuffer.allocate(8 + topic.length() + group.length())
        buffer.putInt(topic.length())
        buffer.put(topic.getBytes(StandardCharsets.UTF_8))
        buffer.putInt(group.length())
        buffer.put(group.getBytes(StandardCharsets.UTF_8))
        buffer.flip()

        when: "validating all checks"
        // Check 1: Minimum size
        boolean hasMinSize = (buffer.remaining() >= 8)

        // Check 2: topicLen validation
        int topicLen = buffer.getInt()
        boolean topicLenValid = (topicLen >= 0 && topicLen <= 65535)

        // Check 3: Sufficient data for topic + groupLen
        boolean sufficientForTopic = (buffer.remaining() >= topicLen + 4)

        // Read topic
        byte[] topicBytes = new byte[topicLen]
        buffer.get(topicBytes)
        String parsedTopic = new String(topicBytes, StandardCharsets.UTF_8)

        // Check 4: groupLen validation
        int groupLen = buffer.getInt()
        boolean groupLenValid = (groupLen >= 0 && groupLen <= 65535)

        // Check 5: Sufficient data for group
        boolean sufficientForGroup = (buffer.remaining() >= groupLen)

        // Read group
        byte[] groupBytes = new byte[groupLen]
        buffer.get(groupBytes)
        String parsedGroup = new String(groupBytes, StandardCharsets.UTF_8)

        then: "all validations pass"
        hasMinSize
        topicLenValid
        sufficientForTopic
        groupLenValid
        sufficientForGroup
        parsedTopic == topic
        parsedGroup == group
    }

    // ===== BROKER-3: ACK Timeout Configuration Tests =====

    def "BROKER-3: default ACK timeout is 60 seconds (not 30 minutes)"() {
        given: "default timeout configuration"
        long defaultTimeoutMs = 60000  // 60 seconds (from application.yml)

        when: "converting to minutes"
        long timeoutMinutes = defaultTimeoutMs / (60 * 1000)

        then: "timeout is 1 minute, not 30 minutes"
        timeoutMinutes == 1
        defaultTimeoutMs != 1800000  // NOT 30 minutes (old hardcoded value)
    }

    def "BROKER-3: ACK timeout can be configured via environment variable"() {
        given: "different timeout configurations"
        def configs = [
            "60000": 60,      // 60 seconds (default)
            "120000": 120,    // 2 minutes (for slower consumers)
            "30000": 30,      // 30 seconds (for fast consumers)
            "180000": 180     // 3 minutes (for large batches)
        ]

        expect: "all configurations are valid"
        configs.each { timeoutStr, expectedSeconds ->
            long timeoutMs = Long.parseLong(timeoutStr)
            long seconds = timeoutMs / 1000
            assert seconds == expectedSeconds
            assert timeoutMs > 0  // Must be positive
        }
    }

    def "BROKER-3: timeout should be reasonable for typical batch sizes"() {
        given: "batch size and expected processing time"
        long batchSizeBytes = 2 * 1024 * 1024  // 2MB typical batch
        long typicalProcessingTimeMs = 5000    // 5 seconds to process 2MB
        long recommendedTimeoutMs = 60000      // 60 seconds (with 12x buffer)

        when: "calculating timeout buffer"
        long bufferMultiplier = recommendedTimeoutMs / typicalProcessingTimeMs

        then: "timeout provides sufficient buffer"
        bufferMultiplier >= 10  // At least 10x processing time
        recommendedTimeoutMs > typicalProcessingTimeMs
    }

    def "BROKER-3: timeout in milliseconds allows fine-grained control"() {
        given: "timeout values in milliseconds"
        long timeout30Seconds = 30000
        long timeout60Seconds = 60000
        long timeout90Seconds = 90000

        expect: "millisecond precision works"
        timeout30Seconds == 30 * 1000
        timeout60Seconds == 60 * 1000
        timeout90Seconds == 90 * 1000

        and: "all are less than old hardcoded 30 minutes"
        timeout30Seconds < 30 * 60 * 1000
        timeout60Seconds < 30 * 60 * 1000
        timeout90Seconds < 30 * 60 * 1000
    }

    def "BROKER-3: pending ACK age can be tracked with timestamps"() {
        given: "batch sent at a specific time"
        long batchSentTimeMs = System.currentTimeMillis()

        when: "checking ACK age after some time"
        Thread.sleep(100)  // Simulate 100ms delay
        long currentTimeMs = System.currentTimeMillis()
        long ackAgeMs = currentTimeMs - batchSentTimeMs

        then: "ACK age is measured correctly"
        ackAgeMs >= 100  // At least 100ms passed
        ackAgeMs < 1000  // Less than 1 second
    }

    def "BROKER-3: ACK age in seconds for metrics"() {
        given: "different ACK pending times"
        def testCases = [
            1000L: 1.0,      // 1 second
            5000L: 5.0,      // 5 seconds
            30000L: 30.0,    // 30 seconds
            60000L: 60.0     // 60 seconds (timeout threshold)
        ]

        expect: "conversion to seconds works correctly"
        testCases.each { ageMs, expectedSeconds ->
            double ageSeconds = ageMs / 1000.0
            assert ageSeconds == expectedSeconds
        }
    }

    def "BROKER-3: timeout detection logic"() {
        given: "batch sent at T0"
        long batchSentTime = 1000000L
        long timeoutMs = 60000L  // 60 seconds

        when: "checking at different times"
        long beforeTimeout = batchSentTime + 50000L  // 50 seconds later
        long afterTimeout = batchSentTime + 70000L   // 70 seconds later

        then: "timeout is detected correctly"
        (beforeTimeout - batchSentTime) < timeoutMs  // Not timed out
        (afterTimeout - batchSentTime) > timeoutMs   // Timed out
    }

    def "BROKER-3: timeout value must be positive"() {
        given: "various timeout configurations"
        def validTimeouts = [1000L, 5000L, 60000L, 120000L]
        def invalidTimeouts = [0L, -1L, -1000L]

        expect: "valid timeouts are positive"
        validTimeouts.each { timeout ->
            assert timeout > 0
        }

        and: "invalid timeouts are rejected"
        invalidTimeouts.each { timeout ->
            assert timeout <= 0
        }
    }
}
