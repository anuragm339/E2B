package com.messaging.broker.consumer;

public enum RefreshState {
    IDLE,
    RESET_SENT,
    WAITING_FOR_ACKS,
    REPLAYING,
    READY_SENT,
    COMPLETED,
    ABORTED
}
