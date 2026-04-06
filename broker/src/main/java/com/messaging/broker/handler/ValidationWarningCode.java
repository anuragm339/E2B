package com.messaging.broker.handler;

/**
 * Enumeration of validation warning codes.
 */
public enum ValidationWarningCode {
    // Offset warnings
    OFFSET_CLAMPED_TO_STORAGE_HEAD,
    OFFSET_CLAMPED_TO_ZERO,

    // Topic warnings
    TOPIC_AUTO_CREATED,

    // Subscription warnings
    SUBSCRIPTION_ALREADY_EXISTS,

    // Data message warnings
    PAYLOAD_TRUNCATED,

    // General warnings
    AUTO_CORRECTED
}
