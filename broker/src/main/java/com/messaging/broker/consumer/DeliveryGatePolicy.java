package com.messaging.broker.consumer;

import com.messaging.broker.consumer.RemoteConsumer;

/**
 * Policy for gating delivery attempts.
 *
 * Gates can prevent delivery based on various conditions:
 * - Data availability (watermark check)
 * - System state (data refresh in progress)
 * - Flow control (in-flight limits)
 */
public interface DeliveryGatePolicy {
    /**
     * Check if delivery should proceed for this consumer.
     *
     * @param consumer Consumer to check
     * @return Result indicating whether delivery is allowed
     */
    GateResult shouldDeliver(RemoteConsumer consumer);

    /**
     * Result of gate check.
     */
    interface GateResult {
        boolean isAllowed();
        String getReason();

        static GateResult allow() {
            return new GateResult() {
                public boolean isAllowed() { return true; }
                public String getReason() { return null; }
            };
        }

        static GateResult deny(String reason) {
            return new GateResult() {
                public boolean isAllowed() { return false; }
                public String getReason() { return reason; }
            };
        }
    }
}
