package com.messaging.broker.handler;

import java.util.Map;
import java.util.Objects;

/**
 * Represents a validation failure.
 *
 * Contains error code, message, and contextual details.
 */
public record ValidationError(
    ValidationErrorCode code,
    String message,
    Map<String, Object> context
) {

    public ValidationError {
        Objects.requireNonNull(code, "code cannot be null");
        Objects.requireNonNull(message, "message cannot be null");
        Objects.requireNonNull(context, "context cannot be null");
        // Make defensive copy
        context = Map.copyOf(context);
    }

    /**
     * Create a validation error.
     *
     * @param code Error code
     * @param message Error message
     * @param context Contextual details
     * @return ValidationError instance
     */
    public static ValidationError of(ValidationErrorCode code, String message, Map<String, Object> context) {
        return new ValidationError(code, message, context);
    }

    /**
     * Create a validation error without context.
     *
     * @param code Error code
     * @param message Error message
     * @return ValidationError instance
     */
    public static ValidationError of(ValidationErrorCode code, String message) {
        return new ValidationError(code, message, Map.of());
    }

    @Override
    public Map<String, Object> context() {
        return Map.copyOf(context);
    }
}
