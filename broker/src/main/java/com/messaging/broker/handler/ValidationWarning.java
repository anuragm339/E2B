package com.messaging.broker.handler;

import java.util.Map;
import java.util.Objects;

/**
 * Represents a validation warning (non-fatal issue).
 *
 * Warnings indicate issues that were auto-corrected or can be safely ignored.
 */
public record ValidationWarning(
    ValidationWarningCode code,
    String message,
    Map<String, Object> context
) {

    public ValidationWarning {
        Objects.requireNonNull(code, "code cannot be null");
        Objects.requireNonNull(message, "message cannot be null");
        Objects.requireNonNull(context, "context cannot be null");
        // Make defensive copy
        context = Map.copyOf(context);
    }

    /**
     * Create a validation warning.
     *
     * @param code Warning code
     * @param message Warning message
     * @param context Contextual details
     * @return ValidationWarning instance
     */
    public static ValidationWarning of(ValidationWarningCode code, String message, Map<String, Object> context) {
        return new ValidationWarning(code, message, context);
    }

    /**
     * Create a validation warning without context.
     *
     * @param code Warning code
     * @param message Warning message
     * @return ValidationWarning instance
     */
    public static ValidationWarning of(ValidationWarningCode code, String message) {
        return new ValidationWarning(code, message, Map.of());
    }

    @Override
    public Map<String, Object> context() {
        return Map.copyOf(context);
    }
}
