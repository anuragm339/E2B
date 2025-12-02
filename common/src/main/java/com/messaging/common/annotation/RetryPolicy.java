package com.messaging.common.annotation;

/**
 * Retry policy for consumer error handling
 */
public enum RetryPolicy {
    /**
     * Exponential backoff then fixed interval (infinite retries)
     */
    EXPONENTIAL_THEN_FIXED,

    /**
     * Skip message on error and continue with next
     */
    SKIP_ON_ERROR,

    /**
     * Pause consumer on error (requires manual intervention)
     */
    PAUSE_ON_ERROR
}
