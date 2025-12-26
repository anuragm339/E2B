package com.messaging.broker.health;

import com.messaging.broker.refresh.DataRefreshManager;
import com.messaging.broker.refresh.DataRefreshContext;
import com.messaging.broker.refresh.DataRefreshState;
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
 * Health indicator for DataRefresh operations
 *
 * Reports DOWN status when refresh is in progress with detailed state information
 * Reports UP status when no refresh is active
 */
@Singleton
@Requires(beans = DataRefreshManager.class)
public class DataRefreshHealthIndicator implements HealthIndicator {

    private final DataRefreshManager dataRefreshManager;

    public DataRefreshHealthIndicator(DataRefreshManager dataRefreshManager) {
        this.dataRefreshManager = dataRefreshManager;
    }

    @Override
    public Publisher<HealthResult> getResult() {
        DataRefreshContext context = dataRefreshManager.getCurrentRefreshContext();

        if (context != null && isActiveRefresh(context.getState())) {
            // Refresh in progress - report DOWN
            Map<String, Object> details = new LinkedHashMap<>();
            details.put("reason", "Data refresh in progress");
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
    private boolean isActiveRefresh(DataRefreshState state) {
        return state != DataRefreshState.IDLE &&
               state != DataRefreshState.COMPLETED &&
               state != DataRefreshState.ABORTED;
    }
}
