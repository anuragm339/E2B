package com.messaging.broker.consumer;

/**
 * Policy for calculating retry delays in adaptive delivery.
 */
public interface DeliveryRetryPolicy {
    /**
     * Calculate next delivery delay based on current state.
     *
     * @param currentDelayMs Current delay in milliseconds
     * @param dataFound Whether data was found in last attempt
     * @return Next delay in milliseconds
     */
    long calculateNextDelay(long currentDelayMs, boolean dataFound);

    /**
     * Get initial delay for new delivery task.
     *
     * @return Initial delay in milliseconds
     */
    long getInitialDelay();
}
