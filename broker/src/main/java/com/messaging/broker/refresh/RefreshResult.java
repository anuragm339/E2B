package com.messaging.broker.refresh;

public class RefreshResult {
    private final boolean success;
    private final String topic;
    private final DataRefreshState state;
    private final int consumerCount;
    private final String error;

    private RefreshResult(boolean success, String topic, DataRefreshState state, int consumerCount, String error) {
        this.success = success;
        this.topic = topic;
        this.state = state;
        this.consumerCount = consumerCount;
        this.error = error;
    }

    public static RefreshResult success(String topic, DataRefreshState state, int consumerCount) {
        return new RefreshResult(true, topic, state, consumerCount, null);
    }

    public static RefreshResult error(String error) {
        return new RefreshResult(false, null, null, 0, error);
    }

    // Getters
    public boolean isSuccess() { return success; }
    public String getTopic() { return topic; }
    public DataRefreshState getState() { return state; }
    public int getConsumerCount() { return consumerCount; }
    public String getError() { return error; }
}
