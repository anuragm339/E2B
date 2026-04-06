package com.messaging.broker.handler

import spock.lang.Specification

class ValidationErrorSpec extends Specification {

    def "should create validation error with context"() {
        given:
        def context = Map.of("offset", -5L, "topic", "prices-v1")

        when:
        def error = ValidationError.of(
            ValidationErrorCode.NEGATIVE_OFFSET,
            "Offset cannot be negative: -5",
            context
        )

        then:
        error.code() == ValidationErrorCode.NEGATIVE_OFFSET
        error.message() == "Offset cannot be negative: -5"
        error.context() == context
        error.context().get("offset") == -5L
        error.context().get("topic") == "prices-v1"
    }

    def "should create validation error without context"() {
        when:
        def error = ValidationError.of(
            ValidationErrorCode.NEGATIVE_OFFSET,
            "Offset cannot be negative"
        )

        then:
        error.code() == ValidationErrorCode.NEGATIVE_OFFSET
        error.message() == "Offset cannot be negative"
        error.context().isEmpty()
    }

    def "should create immutable copy of context"() {
        given:
        def mutableContext = new HashMap<String, Object>()
        mutableContext.put("key", "value")

        when:
        def error = ValidationError.of(ValidationErrorCode.UNKNOWN_ERROR, "test", mutableContext)
        mutableContext.put("key2", "value2")

        then:
        error.context().size() == 1
        error.context().get("key") == "value"
        !error.context().containsKey("key2")
    }

    def "context accessor should return immutable map"() {
        given:
        def error = ValidationError.of(
            ValidationErrorCode.UNKNOWN_ERROR,
            "test",
            Map.of("key", "value")
        )

        when:
        def context = error.context()

        then:
        context == Map.of("key", "value")
        context.getClass().getName().contains("Immutable") || context.getClass().getName().contains("Unmodifiable")
    }

    def "should implement equals correctly"() {
        given:
        def error1 = ValidationError.of(
            ValidationErrorCode.NEGATIVE_OFFSET,
            "test",
            Map.of("offset", -1L)
        )
        def error2 = ValidationError.of(
            ValidationErrorCode.NEGATIVE_OFFSET,
            "test",
            Map.of("offset", -1L)
        )
        def error3 = ValidationError.of(
            ValidationErrorCode.STORAGE_ERROR,
            "test",
            Map.of("offset", -1L)
        )

        expect:
        error1 == error2
        error1 != error3
    }

    def "should implement hashCode correctly"() {
        given:
        def error1 = ValidationError.of(ValidationErrorCode.NEGATIVE_OFFSET, "test", Map.of())
        def error2 = ValidationError.of(ValidationErrorCode.NEGATIVE_OFFSET, "test", Map.of())

        expect:
        error1.hashCode() == error2.hashCode()
    }

    def "should reject null code"() {
        when:
        ValidationError.of(null, "message", Map.of())

        then:
        thrown(NullPointerException)
    }

    def "should reject null message"() {
        when:
        ValidationError.of(ValidationErrorCode.UNKNOWN_ERROR, null, Map.of())

        then:
        thrown(NullPointerException)
    }

    def "should reject null context"() {
        when:
        ValidationError.of(ValidationErrorCode.UNKNOWN_ERROR, "message", null)

        then:
        thrown(NullPointerException)
    }
}
