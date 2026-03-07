package com.messaging.broker.legacy;

import io.micronaut.context.annotation.ConfigurationProperties;

import java.util.Collections;
import java.util.List;
import java.util.Map;

/**
 * Configuration for legacy client integration.
 * Maps legacy serviceName to list of topics for auto-subscription.
 */
@ConfigurationProperties("legacy-clients")
public class LegacyClientConfig {

    private boolean enabled = true;
    private Map<String, List<String>> serviceTopics = Collections.emptyMap();

    public boolean isEnabled() {
        return enabled;
    }

    public void setEnabled(boolean enabled) {
        this.enabled = enabled;
    }

    public Map<String, List<String>> getServiceTopics() {
        return serviceTopics;
    }

    public void setServiceTopics(Map<String, List<String>> serviceTopics) {
        this.serviceTopics = serviceTopics;
    }

    /**
     * Get topics for a given service name.
     * Returns empty list if service is not configured.
     */
    public List<String> getTopicsForService(String serviceName) {
        return serviceTopics.getOrDefault(serviceName, Collections.emptyList());
    }

    /**
     * Check if a service is configured for legacy client support.
     */
    public boolean isServiceConfigured(String serviceName) {
        return serviceTopics.containsKey(serviceName);
    }
}
