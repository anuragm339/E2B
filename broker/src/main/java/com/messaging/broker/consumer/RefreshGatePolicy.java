package com.messaging.broker.consumer;

import com.messaging.broker.consumer.DeliveryGatePolicy;
import com.messaging.broker.consumer.RemoteConsumer;
import com.messaging.broker.consumer.RefreshContext;
import com.messaging.broker.consumer.RefreshCoordinator;
import com.messaging.broker.consumer.RefreshState;
import jakarta.inject.Inject;
import jakarta.inject.Singleton;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Gate policy that blocks delivery during data refresh RESET_SENT state.
 *
 * During RESET wait, consumers should be paused and not receiving messages.
 * Continuing delivery creates polling storm with accumulated ScheduledFutures.
 */
@Singleton
public class RefreshGatePolicy implements DeliveryGatePolicy {
    private static final Logger log = LoggerFactory.getLogger(RefreshGatePolicy.class);

    private volatile RefreshCoordinator dataRefreshCoordinator;

    @Inject
    public RefreshGatePolicy() {
        // RefreshCoordinator will be set via setter to avoid circular dependency
    }

    /**
     * Set RefreshCoordinator reference.
     */
    public void setDataRefreshCoordinator(RefreshCoordinator dataRefreshCoordinator) {
        this.dataRefreshCoordinator = dataRefreshCoordinator;
        log.info("RefreshGatePolicy wired to RefreshCoordinator");
    }

    @Override
    public GateResult shouldDeliver(RemoteConsumer consumer) {
        if (dataRefreshCoordinator == null) {
            // Not yet wired - allow delivery
            return GateResult.allow();
        }

        RefreshContext refreshContext = dataRefreshCoordinator.getRefreshStatus(consumer.getTopic());

        if (refreshContext != null && refreshContext.getState() == RefreshState.RESET_SENT) {
            log.trace("RefreshGate DENY: {}:{} - topic in RESET_SENT state (waiting for consumer ACK)",
                    consumer.getClientId(), consumer.getTopic());
            return GateResult.deny("Topic in data refresh RESET_SENT state");
        }

        return GateResult.allow();
    }
}
