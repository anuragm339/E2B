package com.messaging.broker.handler;

/**
 * Enumeration of validation error codes.
 */
public enum ValidationErrorCode {
    // Offset validation errors
    NEGATIVE_OFFSET,
    OFFSET_EXCEEDS_STORAGE,
    STORAGE_ERROR,

    // Topic validation errors
    INVALID_TOPIC_NAME,
    TOPIC_NOT_FOUND,
    EMPTY_TOPIC_NAME,

    // Group validation errors
    INVALID_GROUP_NAME,
    EMPTY_GROUP_NAME,

    // Subscribe validation errors
    DUPLICATE_SUBSCRIPTION,
    INVALID_SUBSCRIPTION,
    EMPTY_TOPICS_LIST,

    // Data message validation errors
    PAYLOAD_TOO_LARGE,
    EMPTY_PAYLOAD,
    INVALID_PARTITION,

    // Client validation errors
    INVALID_CLIENT_ID,
    CLIENT_NOT_FOUND,

    // General errors
    UNKNOWN_ERROR
}
