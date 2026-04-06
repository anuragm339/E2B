package com.messaging.broker.model;

import java.util.Objects;

/**
 * Value object representing a batch acknowledgment payload.
 *
 * Sent by consumers to acknowledge receipt and processing of a message batch.
 * Format: {"offset": <long>, "success": <boolean>}
 */
public record AckPayload(long offset, boolean success, String errorMessage) {

    public AckPayload {
        // errorMessage can be null for successful acks
    }

    /**
     * Create a successful acknowledgment.
     *
     * @param offset Offset of the acknowledged batch
     * @return AckPayload instance representing success
     */
    public static AckPayload success(long offset) {
        return new AckPayload(offset, true, null);
    }

    /**
     * Create a failed acknowledgment with error message.
     *
     * @param offset Offset of the failed batch
     * @param errorMessage Error description
     * @return AckPayload instance representing failure
     */
    public static AckPayload failure(long offset, String errorMessage) {
        Objects.requireNonNull(errorMessage, "errorMessage cannot be null for failures");
        return new AckPayload(offset, false, errorMessage);
    }

    /**
     * Create an acknowledgment with success flag.
     *
     * @param offset Offset of the batch
     * @param success Whether processing succeeded
     * @return AckPayload instance
     */
    public static AckPayload of(long offset, boolean success) {
        return new AckPayload(offset, success, null);
    }

    /**
     * Check if this is a successful acknowledgment.
     *
     * @return True if success, false if failure
     */
    public boolean isSuccess() {
        return success;
    }

    /**
     * Check if this is a failed acknowledgment.
     *
     * @return True if failure, false if success
     */
    public boolean isFailure() {
        return !success;
    }

    /**
     * Check if an error message is present.
     *
     * @return True if errorMessage is not null
     */
    public boolean hasErrorMessage() {
        return errorMessage != null;
    }
}
