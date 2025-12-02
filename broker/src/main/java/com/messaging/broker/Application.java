package com.messaging.broker;

import io.micronaut.runtime.Micronaut;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Main broker application
 */
public class Application {
    private static final Logger log = LoggerFactory.getLogger(Application.class);

    public static void main(String[] args) {
        log.info("Starting Messaging Broker...");
        Micronaut.run(Application.class, args);
    }
}
