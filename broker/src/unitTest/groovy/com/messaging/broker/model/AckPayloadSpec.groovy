package com.messaging.broker.model

import spock.lang.Specification

class AckPayloadSpec extends Specification {

    def "should create successful acknowledgment"() {
        when:
        def ack = AckPayload.success(12345L)

        then:
        ack.offset() == 12345L
        ack.success()
        ack.isSuccess()
        !ack.isFailure()
        !ack.hasErrorMessage()
        ack.errorMessage() == null
    }

    def "should create failed acknowledgment with error message"() {
        when:
        def ack = AckPayload.failure(12345L, "Database connection failed")

        then:
        ack.offset() == 12345L
        !ack.success()
        ack.isFailure()
        !ack.isSuccess()
        ack.hasErrorMessage()
        ack.errorMessage() == "Database connection failed"
    }

    def "should create acknowledgment with success flag"() {
        when:
        def successAck = AckPayload.of(100L, true)
        def failureAck = AckPayload.of(200L, false)

        then:
        successAck.isSuccess()
        !failureAck.isSuccess()
    }

    def "should handle zero offset"() {
        when:
        def ack = AckPayload.success(0L)

        then:
        ack.offset() == 0L
        ack.isSuccess()
    }

    def "should handle large offset"() {
        when:
        def ack = AckPayload.success(Long.MAX_VALUE)

        then:
        ack.offset() == Long.MAX_VALUE
        ack.isSuccess()
    }

    def "should reject null error message in failure factory"() {
        when:
        AckPayload.failure(100L, null)

        then:
        thrown(NullPointerException)
    }

    def "should allow null error message in constructor for success"() {
        when:
        def ack = new AckPayload(100L, true, null)

        then:
        ack.isSuccess()
        !ack.hasErrorMessage()
    }

    def "should implement equals correctly"() {
        given:
        def ack1 = AckPayload.success(100L)
        def ack2 = AckPayload.success(100L)
        def ack3 = AckPayload.success(200L)
        def ack4 = AckPayload.failure(100L, "error")

        expect:
        ack1 == ack2
        ack1 != ack3  // Different offset
        ack1 != ack4  // Different success/error
    }

    def "should implement hashCode correctly"() {
        given:
        def ack1 = AckPayload.success(100L)
        def ack2 = AckPayload.success(100L)

        expect:
        ack1.hashCode() == ack2.hashCode()
    }

    def "successful ack should have no error message"() {
        when:
        def ack = AckPayload.success(100L)

        then:
        !ack.hasErrorMessage()
        ack.errorMessage() == null
    }

    def "failed ack with error message should have hasErrorMessage true"() {
        when:
        def ack = AckPayload.failure(100L, "Processing error")

        then:
        ack.hasErrorMessage()
        ack.errorMessage() != null
    }

    def "should handle real-world success scenario"() {
        when:
        def ack = AckPayload.success(987654L)

        then:
        ack.offset() == 987654L
        ack.isSuccess()
        !ack.isFailure()
        !ack.hasErrorMessage()
    }

    def "should handle real-world failure scenario"() {
        when:
        def ack = AckPayload.failure(
            987654L,
            "Failed to process batch: ValidationException - Invalid product ID"
        )

        then:
        ack.offset() == 987654L
        ack.isFailure()
        !ack.isSuccess()
        ack.hasErrorMessage()
        ack.errorMessage().contains("ValidationException")
    }

    def "should create failure ack without using factory"() {
        when:
        def ack = new AckPayload(100L, false, "Custom error")

        then:
        ack.isFailure()
        ack.errorMessage() == "Custom error"
    }
}
