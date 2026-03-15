package com.messaging.broker.monitoring;

import com.messaging.broker.consumer.RefreshContext;
import com.messaging.broker.consumer.RefreshCoordinator;
import com.messaging.broker.consumer.RefreshState;
import io.micronaut.context.annotation.Requires;
import io.micronaut.core.async.publisher.Publishers;
import io.micronaut.health.HealthStatus;
import io.micronaut.management.health.indicator.HealthIndicator;
import io.micronaut.management.health.indicator.HealthResult;
import jakarta.inject.Singleton;
import org.reactivestreams.Publisher;

import java.util.LinkedHashMap;
import java.util.Map;

/**
 * Health indicator for refresh operations.
 *
 * Reports DOWN while a refresh is in progress and UP when no refresh is active.
 */
@Singleton
@Requires(beans = RefreshCoordinator.class)
public class RefreshHealthIndicator implements HealthIndicator {

    private final RefreshCoordinator dataRefreshCoordinator;

    public RefreshHealthIndicator(RefreshCoordinator dataRefreshCoordinator) {
        this.dataRefreshCoordinator = dataRefreshCoordinator;
    }

    @Override
    public Publisher<HealthResult> getResult() {
        RefreshContext context = dataRefreshCoordinator.getCurrentRefreshContext();

        if (context != null && isActiveRefresh(context.getState())) {
            // Refresh in progress - report DOWN
            Map<String, Object> details = new LinkedHashMap<>();
            details.put("reason", "Refresh in progress");
            details.put("topic", context.getTopic());
            details.put("state", context.getState().toString());
            details.put("startTime", context.getStartTime().toString());

            // Add ACK progress
            int expectedCount = context.getExpectedConsumers().size();
            int resetAckCount = context.getReceivedResetAcks().size();
            int readyAckCount = context.getReceivedReadyAcks().size();

            details.put("expectedConsumers", expectedCount);
            details.put("resetAcks", resetAckCount + "/" + expectedCount);
            details.put("readyAcks", readyAckCount + "/" + expectedCount);

            // Add timing info if available
            if (context.getResetSentTime() != null) {
                details.put("resetSentTime", context.getResetSentTime().toString());
            }
            if (context.getReadySentTime() != null) {
                details.put("readySentTime", context.getReadySentTime().toString());
            }

            return Publishers.just(
                HealthResult.builder("dataRefresh")
                    .status(HealthStatus.DOWN)
                    .details(details)
                    .build()
            );
        }

        // No active refresh - report UP
        return Publishers.just(
            HealthResult.builder("dataRefresh")
                .status(HealthStatus.UP)
                .build()
        );
    }

    /**
     * Check if the refresh state is considered active (should report DOWN)
     */
    private boolean isActiveRefresh(RefreshState state) {
        return state != RefreshState.IDLE &&
               state != RefreshState.COMPLETED &&
               state != RefreshState.ABORTED;
    }
}
