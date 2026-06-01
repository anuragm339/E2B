package com.example.consumer;

import io.micronaut.runtime.Micronaut;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Main consumer application - configurable via environment variables
 */
public class ConsumerApplication {
    private static final Logger log = LoggerFactory.getLogger(ConsumerApplication.class);

    public static void main(String[] args) {
        String consumerType = System.getenv().getOrDefault("CONSUMER_TYPE", "unknown");

        // Support both CONSUMER_TOPICS (comma-separated) and legacy CONSUMER_TOPIC
        String consumerTopics = System.getenv().getOrDefault("CONSUMER_TOPICS",
                                System.getenv().getOrDefault("CONSUMER_TOPIC", "unknown"));
        String consumerPort = System.getenv().getOrDefault("CONSUMER_PORT", "8080");

        log.info("event=consumer.starting type={} topics={} port={}", consumerType, consumerTopics, consumerPort);

        Micronaut.run(ConsumerApplication.class, args);
    }
}
