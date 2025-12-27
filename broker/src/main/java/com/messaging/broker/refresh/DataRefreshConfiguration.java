package com.messaging.broker.refresh;

import io.micronaut.context.annotation.ConfigurationProperties;
import java.util.List;
import java.util.Map;

@ConfigurationProperties("data-refresh")
public class DataRefreshConfiguration {
    private List<String> expectedConsumers;
    private Map<String, List<String>> serviceTopics;  // For future service-level refresh

    public List<String> getExpectedConsumers() {
        return expectedConsumers;
    }

    public void setExpectedConsumers(List<String> expectedConsumers) {
        this.expectedConsumers = expectedConsumers;
    }

    public Map<String, List<String>> getServiceTopics() {
        return serviceTopics;
    }

    public void setServiceTopics(Map<String, List<String>> serviceTopics) {
        this.serviceTopics = serviceTopics;
    }
}
