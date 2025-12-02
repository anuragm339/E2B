package com.messaging.common.model;

/**
 * Health status for system components
 */
public enum HealthStatus {
    /**
     * Component is fully operational
     */
    HEALTHY,

    /**
     * Component is operational but degraded (slow, occasional errors)
     */
    DEGRADED,

    /**
     * Component is not operational
     */
    UNHEALTHY
}
