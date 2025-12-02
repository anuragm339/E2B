package com.messaging.common.annotation;

import com.messaging.common.api.ErrorHandler;
import com.messaging.common.api.NoOpErrorHandler;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * Annotation to mark a class as a message consumer
 */
@Target(ElementType.TYPE)
@Retention(RetentionPolicy.RUNTIME)
public @interface Consumer {

    /**
     * Topic to subscribe to
     */
    String topic();

    /**
     * Consumer group ID
     */
    String group();

    /**
     * Retry policy for error handling
     */
    RetryPolicy retryPolicy() default RetryPolicy.EXPONENTIAL_THEN_FIXED;

    /**
     * Maximum number of exponential retries before switching to fixed interval
     */
    int maxExponentialRetries() default 10;

    /**
     * Initial retry delay in milliseconds
     */
    long initialRetryDelayMs() default 1000;

    /**
     * Maximum retry delay in milliseconds (cap for exponential)
     */
    long maxRetryDelayMs() default 30000;

    /**
     * Fixed retry interval in milliseconds (after exponential retries exhausted)
     */
    long fixedRetryIntervalMs() default 30000;

    /**
     * Error handler class
     */
    Class<? extends ErrorHandler> errorHandler() default NoOpErrorHandler.class;
}
