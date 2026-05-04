package com.example.consumer.config;

import io.micronaut.context.annotation.ConfigurationProperties;

/**
 * Configuration for legacy Event protocol support.
 * When enabled, consumer uses legacy Event protocol (ordinals 0-7)
 * instead of modern BrokerMessage protocol (codes 0x01-0x0C).
 */
@ConfigurationProperties("consumer.legacy")
public class LegacyConfig {
    private boolean enabled = false;
    private String serviceName = "unknown-service";

    public boolean isEnabled() {
        return enabled;
    }

    public void setEnabled(boolean enabled) {
        this.enabled = enabled;
    }

    public String getServiceName() {
        return serviceName;
    }

    public void setServiceName(String serviceName) {
        this.serviceName = serviceName;
    }
}
