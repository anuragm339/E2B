package com.messaging.broker.handler;

/**
 * Sealed interface representing the result of a validation operation.
 *
 * Can be Success, Warning (with corrected value), or Failure.
 */
public sealed interface ValidationResult
    permits ValidationResult.Success, ValidationResult.Warning, ValidationResult.Failure {

    /**
     * Check if validation succeeded.
     */
    boolean isSuccess();

    /**
     * Check if validation resulted in a warning.
     */
    boolean isWarning();

    /**
     * Check if validation failed.
     */
    boolean isFailure();

    /**
     * Validation succeeded without issues.
     */
    record Success() implements ValidationResult {
        @Override
        public boolean isSuccess() {
            return true;
        }

        @Override
        public boolean isWarning() {
            return false;
        }

        @Override
        public boolean isFailure() {
            return false;
        }
    }

    /**
     * Validation succeeded but with a warning (value was auto-corrected).
     */
    record Warning(ValidationWarning warning, Object correctedValue) implements ValidationResult {
        @Override
        public boolean isSuccess() {
            return false;
        }

        @Override
        public boolean isWarning() {
            return true;
        }

        @Override
        public boolean isFailure() {
            return false;
        }

        /**
         * Get corrected offset value (convenience method).
         */
        public long correctedOffset() {
            if (correctedValue instanceof Long) {
                return (Long) correctedValue;
            }
            throw new IllegalStateException("Corrected value is not a Long: " + correctedValue.getClass());
        }
    }

    /**
     * Validation failed.
     */
    record Failure(ValidationError error) implements ValidationResult {
        @Override
        public boolean isSuccess() {
            return false;
        }

        @Override
        public boolean isWarning() {
            return false;
        }

        @Override
        public boolean isFailure() {
            return true;
        }
    }

    // Factory methods

    /**
     * Create a success result.
     */
    static ValidationResult success() {
        return new Success();
    }

    /**
     * Create a warning result with corrected value.
     */
    static ValidationResult warning(ValidationWarning warning, Object correctedValue) {
        return new Warning(warning, correctedValue);
    }

    /**
     * Create a failure result.
     */
    static ValidationResult failure(ValidationError error) {
        return new Failure(error);
    }
}
