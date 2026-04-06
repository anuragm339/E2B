package com.messaging.broker.handler

import spock.lang.Specification

class ValidationResultSpec extends Specification {

    def "should create success result"() {
        when:
        def result = ValidationResult.success()

        then:
        result.isSuccess()
        !result.isWarning()
        !result.isFailure()
        result instanceof ValidationResult.Success
    }

    def "should create warning result"() {
        given:
        def validationWarning = ValidationWarning.of(
            ValidationWarningCode.OFFSET_CLAMPED_TO_STORAGE_HEAD,
            "Offset clamped",
            Map.of("original", 200L, "clamped", 100L)
        )

        when:
        def result = ValidationResult.warning(validationWarning, 100L)

        then:
        !result.isSuccess()
        result.isWarning()
        !result.isFailure()
        result instanceof ValidationResult.Warning
        with(result as ValidationResult.Warning) {
            warning() == validationWarning
            correctedValue() == 100L
            correctedOffset() == 100L
        }
    }

    def "should create failure result"() {
        given:
        def validationError = ValidationError.of(
            ValidationErrorCode.NEGATIVE_OFFSET,
            "Offset cannot be negative",
            Map.of("offset", -5L)
        )

        when:
        def result = ValidationResult.failure(validationError)

        then:
        !result.isSuccess()
        !result.isWarning()
        result.isFailure()
        result instanceof ValidationResult.Failure
        with(result as ValidationResult.Failure) {
            error() == validationError
        }
    }

    def "warning should provide correctedOffset convenience method"() {
        given:
        def validationWarning = ValidationWarning.of(ValidationWarningCode.OFFSET_CLAMPED_TO_ZERO, "Clamped")
        def result = ValidationResult.warning(validationWarning, 0L)

        when:
        def offset = (result as ValidationResult.Warning).correctedOffset()

        then:
        offset == 0L
    }

    def "correctedOffset should throw if value is not Long"() {
        given:
        def validationWarning = ValidationWarning.of(ValidationWarningCode.AUTO_CORRECTED, "Corrected")
        def result = ValidationResult.warning(validationWarning, "not a long")

        when:
        (result as ValidationResult.Warning).correctedOffset()

        then:
        thrown(IllegalStateException)
    }

    def "success result should be singleton-like"() {
        when:
        def result1 = ValidationResult.success()
        def result2 = ValidationResult.success()

        then:
        result1.isSuccess()
        result2.isSuccess()
        result1.class == result2.class
    }

    def "should distinguish between different result types"() {
        given:
        def success = ValidationResult.success()
        def warningResult = ValidationResult.warning(
            ValidationWarning.of(ValidationWarningCode.AUTO_CORRECTED, "test"),
            "value"
        )
        def failureResult = ValidationResult.failure(
            ValidationError.of(ValidationErrorCode.UNKNOWN_ERROR, "test")
        )

        expect:
        success instanceof ValidationResult.Success
        warningResult instanceof ValidationResult.Warning
        failureResult instanceof ValidationResult.Failure

        and:
        success.isSuccess()
        warningResult.isWarning()
        failureResult.isFailure()
    }
}
