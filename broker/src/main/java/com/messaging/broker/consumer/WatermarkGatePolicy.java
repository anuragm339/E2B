package com.messaging.broker.consumer;

import com.messaging.broker.consumer.DeliveryGatePolicy;
import com.messaging.broker.consumer.RemoteConsumer;
import com.messaging.common.api.StorageEngine;
import jakarta.inject.Inject;
import jakarta.inject.Singleton;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Gate policy that checks if new data is available based on storage watermarks.
 *
 * Prevents unnecessary delivery attempts when consumer is already at latest offset.
 */
@Singleton
public class WatermarkGatePolicy implements DeliveryGatePolicy {
    private static final Logger log = LoggerFactory.getLogger(WatermarkGatePolicy.class);

    private final StorageEngine storage;

    @Inject
    public WatermarkGatePolicy(StorageEngine storage) {
        this.storage = storage;
    }

    @Override
    public GateResult shouldDeliver(RemoteConsumer consumer) {
        try {
            long consumerOffset = consumer.getCurrentOffset();
            long storageOffset = storage.getCurrentOffset(consumer.getTopic(), 0);

            if (storageOffset <= consumerOffset) {
                log.trace("WatermarkGate DENY: {}:{} - no new data (storage={}, consumer={})",
                        consumer.getClientId(), consumer.getTopic(), storageOffset, consumerOffset);
                return GateResult.deny("No new data available");
            }

            log.trace("WatermarkGate ALLOW: {}:{} - new data available (storage={}, consumer={})",
                    consumer.getClientId(), consumer.getTopic(), storageOffset, consumerOffset);
            return GateResult.allow();

        } catch (Exception e) {
            log.error("Error checking watermark for {}:{}", consumer.getClientId(), consumer.getTopic(), e);
            return GateResult.deny("Watermark check failed: " + e.getMessage());
        }
    }
}
