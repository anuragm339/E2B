package com.messaging.broker.monitoring;

import com.messaging.broker.consumer.RefreshContext;
import com.messaging.broker.consumer.RefreshCoordinator;
import com.messaging.broker.consumer.RefreshState;
import com.messaging.broker.snapshot.BootstrapProgressTracker;
import io.micronaut.context.env.Environment;
import io.micronaut.context.annotation.Requires;
import io.micronaut.core.async.publisher.Publishers;
import io.micronaut.core.type.Argument;
import io.micronaut.health.HealthStatus;
import io.micronaut.management.health.indicator.HealthIndicator;
import io.micronaut.management.health.indicator.HealthResult;
import jakarta.inject.Singleton;
import org.reactivestreams.Publisher;

import java.util.List;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * Health indicator for refresh operations.
 *
 * Reports DOWN while a refresh is in progress and UP when no refresh is active. When
 * {@code broker.refresh.health-critical-topics} is set, only those topics block health.
 */
@Singleton
@Requires(beans = RefreshCoordinator.class)
public class RefreshHealthIndicator implements HealthIndicator {

    private final RefreshCoordinator dataRefreshCoordinator;
    private final BootstrapProgressTracker bootstrapProgress;
    private final Set<String> healthCriticalTopics;

    public RefreshHealthIndicator(RefreshCoordinator dataRefreshCoordinator,
                                  BootstrapProgressTracker bootstrapProgress,
                                  Environment environment) {
        this.dataRefreshCoordinator = dataRefreshCoordinator;
        this.bootstrapProgress = bootstrapProgress;
        this.healthCriticalTopics = environment
                .getProperty("broker.refresh.health-critical-topics", Argument.listOf(String.class))
                .orElse(List.of())
                .stream()
                .map(String::trim)
                .filter(topic -> !topic.isEmpty())
                .collect(Collectors.toUnmodifiableSet());
    }

    @Override
    public Publisher<HealthResult> getResult() {
        // Destructive wipe + re-source window: the node has no serveable data and there is no
        // per-topic RefreshContext yet, so report DOWN node-wide (independent of health-critical-topics).
        if (bootstrapProgress.isReSourcing()) {
            Map<String, Object> details = new LinkedHashMap<>();
            details.put("reason", "Bootstrap re-sourcing in progress");
            details.putAll(bootstrapProgress.snapshot());
            return Publishers.just(
                HealthResult.builder("dataRefresh")
                    .status(HealthStatus.DOWN)
                    .details(details)
                    .build()
            );
        }

        RefreshContext context = currentBlockingContext();

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

    private RefreshContext currentBlockingContext() {
        if (healthCriticalTopics.isEmpty()) {
            return dataRefreshCoordinator.getCurrentRefreshContext();
        }

        for (Map.Entry<String, RefreshContext> entry : dataRefreshCoordinator.getActiveRefreshesSnapshot().entrySet()) {
            RefreshContext context = entry.getValue();
            if (healthCriticalTopics.contains(entry.getKey()) && isActiveRefresh(context.getState())) {
                return context;
            }
        }
        return null;
    }
}
