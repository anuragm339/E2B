package com.messaging.common.exception;

/**
 * Exception for data refresh workflow errors
 */
public class DataRefreshException extends MessagingException {

    private String refreshId;
    private String topic;
    private String refreshType;

    public DataRefreshException(ErrorCode errorCode, String message) {
        super(errorCode, message);
    }

    public DataRefreshException(ErrorCode errorCode, String message, Throwable cause) {
        super(errorCode, message, cause);
    }

    public DataRefreshException withRefreshId(String refreshId) {
        this.refreshId = refreshId;
        withContext("refreshId", refreshId);
        return this;
    }

    public DataRefreshException withTopic(String topic) {
        this.topic = topic;
        withContext("topic", topic);
        return this;
    }

    public DataRefreshException withRefreshType(String refreshType) {
        this.refreshType = refreshType;
        withContext("refreshType", refreshType);
        return this;
    }

    public static DataRefreshException alreadyInProgress(String topic, String existingRefreshId) {
        DataRefreshException ex = new DataRefreshException(
                ErrorCode.DATA_REFRESH_ALREADY_IN_PROGRESS,
                String.format("Data refresh already in progress for topic=%s refreshId=%s", topic, existingRefreshId))
                .withTopic(topic);
        ex.withContext("existingRefreshId", existingRefreshId);
        return ex;
    }

    public static DataRefreshException timeout(String refreshId, String topic, long timeoutSeconds) {
        DataRefreshException ex = new DataRefreshException(
                ErrorCode.DATA_REFRESH_TIMEOUT,
                String.format("Data refresh timed out: refreshId=%s topic=%s timeout=%ds", refreshId, topic, timeoutSeconds))
                .withRefreshId(refreshId)
                .withTopic(topic);
        ex.withContext("timeoutSeconds", timeoutSeconds);
        return ex;
    }

    public static DataRefreshException consumerNotReady(String refreshId, String topic, String consumerGroup, long elapsedSeconds) {
        DataRefreshException ex = new DataRefreshException(
                ErrorCode.DATA_REFRESH_CONSUMER_NOT_READY,
                String.format("Consumer not ready: refreshId=%s topic=%s group=%s elapsed=%ds", refreshId, topic, consumerGroup, elapsedSeconds))
                .withRefreshId(refreshId)
                .withTopic(topic);
        ex.withContext("consumerGroup", consumerGroup);
        ex.withContext("elapsedSeconds", elapsedSeconds);
        return ex;
    }

    public static DataRefreshException replayFailed(String refreshId, String topic, long startOffset, long endOffset, Throwable cause) {
        DataRefreshException ex = new DataRefreshException(
                ErrorCode.DATA_REFRESH_REPLAY_FAILED,
                String.format("Message replay failed: refreshId=%s topic=%s offsets=[%d, %d]", refreshId, topic, startOffset, endOffset),
                cause)
                .withRefreshId(refreshId)
                .withTopic(topic);
        ex.withContext("startOffset", startOffset);
        ex.withContext("endOffset", endOffset);
        return ex;
    }
}
