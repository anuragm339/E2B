package com.messaging.broker.refresh;

public enum DataRefreshState {
    IDLE,
    RESET_SENT,
    WAITING_FOR_ACKS,
    REPLAYING,
    READY_SENT,
    COMPLETED,
    ABORTED
}
