package com.messaging.broker.monitoring;

import java.util.UUID;

/**
 * Utility for generating correlation identifiers used in broker logs.
 */
public final class TraceIds {

    private TraceIds() {
    }

    public static String newTraceId() {
        return UUID.randomUUID().toString();
    }
}
