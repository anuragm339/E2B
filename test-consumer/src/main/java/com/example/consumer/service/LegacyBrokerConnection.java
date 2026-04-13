package com.example.consumer.service;

import com.example.consumer.legacy.LegacyWireManager;
import com.example.consumer.legacy.events.*;
import com.messaging.common.model.BrokerMessage;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.net.Socket;

/**
 * Legacy Event protocol connection.
 * Sends RegisterEvent on connect, communicates using Event ordinals (0-7).
 */
public class LegacyBrokerConnection implements BrokerConnection {
    private static final Logger log = LoggerFactory.getLogger(LegacyBrokerConnection.class);
    private static final int PROTOCOL_VERSION = 2;

    private final String serviceName;
    private Socket socket;
    private LegacyWireManager wireManager;

    public LegacyBrokerConnection(String serviceName) {
        this.serviceName = serviceName;
    }

    @Override
    public void connect(String host, int port) throws IOException {
        log.info("🔌 Connecting to broker using LEGACY Event protocol: {}:{}", host, port);
        socket = new Socket(host, port);
        wireManager = new LegacyWireManager(
                new DataInputStream(socket.getInputStream()),
                new DataOutputStream(socket.getOutputStream())
        );

        // Send RegisterEvent
        RegisterEvent registerEvent = new RegisterEvent(PROTOCOL_VERSION, serviceName);
        wireManager.send(registerEvent);
        log.info("📤 Sent RegisterEvent: version={}, service={}", PROTOCOL_VERSION, serviceName);
    }

    @Override
    public void sendAck() throws IOException {
        wireManager.send(AckEvent.INSTANCE);
        log.debug("✅ Sent ACK (legacy)");
    }

    @Override
    public Event nextEvent() throws IOException {
        return wireManager.nextEvent();
    }

    @Override
    public BrokerMessage nextMessage() throws IOException {
        throw new UnsupportedOperationException("Legacy connection does not support BrokerMessage");
    }

    @Override
    public void close() throws IOException {
        if (socket != null && !socket.isClosed()) {
            socket.close();
            log.info("🔌 Legacy connection closed");
        }
    }

    @Override
    public boolean isLegacy() {
        return true;
    }
}
