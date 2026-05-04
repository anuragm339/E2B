package com.example.consumer.legacy;

import com.example.consumer.legacy.events.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.net.Socket;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Test client using legacy Event protocol.
 * Validates broker's legacy client integration.
 */
public class LegacyTestClient {
    private static final int PROTOCOL_VERSION = 2;
    private static final Logger log = LoggerFactory.getLogger(LegacyTestClient.class);

    private final String brokerHost;
    private final int brokerPort;
    private final String serviceName;
    private Socket socket;
    private LegacyWireManager wireManager;
    private volatile boolean running = false;
    private Thread eventLoopThread;

    // Received message tracking for validation
    private final List<DataMessage> receivedMessages = Collections.synchronizedList(new ArrayList<>());
    private final AtomicInteger resetCount = new AtomicInteger(0);
    private final AtomicInteger readyCount = new AtomicInteger(0);
    private final AtomicInteger batchCount = new AtomicInteger(0);
    private final AtomicInteger messageCount = new AtomicInteger(0);

    public LegacyTestClient(String brokerHost, int brokerPort, String serviceName) {
        this.brokerHost = brokerHost;
        this.brokerPort = brokerPort;
        this.serviceName = serviceName;
    }

    public void connect() throws IOException {
        log.info("Connecting to broker: {}:{} as service: {}", brokerHost, brokerPort, serviceName);

        socket = new Socket(brokerHost, brokerPort);
        wireManager = new LegacyWireManager(
                new DataInputStream(socket.getInputStream()),
                new DataOutputStream(socket.getOutputStream())
        );

        // Send RegisterEvent
        RegisterEvent registerEvent = new RegisterEvent(PROTOCOL_VERSION, serviceName);
        wireManager.send(registerEvent);
        log.info("Sent RegisterEvent: version={}, service={}", PROTOCOL_VERSION, serviceName);

        // Start event loop in background thread
        running = true;
        eventLoopThread = new Thread(this::runEventLoop, "LegacyClient-EventLoop");
        eventLoopThread.start();
    }

    private void runEventLoop() {
        log.info("Event loop started");
        while (running) {
            try {
                Event event = wireManager.nextEvent();
                log.debug("Received event: {}", event.getType());

                boolean shouldAck = handleEvent(event);

                if (shouldAck) {
                    wireManager.send(AckEvent.INSTANCE);
                    log.debug("Sent ACK for event: {}", event.getType());
                }

            } catch (IOException e) {
                if (running) {
                    log.error("Error in event loop", e);
                }
                running = false;
            }
        }
        log.info("Event loop stopped");
    }

    private boolean handleEvent(Event event) {
        switch (event.getType()) {
            case RESET:
                return handleReset((ResetEvent) event);
            case READY:
                return handleReady((ReadyEvent) event);
            case MESSAGE:
                return handleMessage((DataMessageEvent) event);
            case BATCH:
                return handleBatch((BatchEvent) event);
            case EOF:
                running = false;
                return false;
            default:
                log.warn("Unexpected event type: {}", event.getType());
                return false;
        }
    }

    private boolean handleReset(ResetEvent event) {
        log.info("RESET received - clearing local data");
        receivedMessages.clear();
        resetCount.incrementAndGet();
        return true;  // Send ACK
    }

    private boolean handleReady(ReadyEvent event) {
        log.info("READY received - refresh complete");
        readyCount.incrementAndGet();
        return true;  // Send ACK
    }

    private boolean handleMessage(DataMessageEvent event) {
        DataMessage msg = event.getMessage();
        log.debug("MESSAGE received: type={}, key={}", msg.getType(), msg.getKey());
        receivedMessages.add(msg);
        messageCount.incrementAndGet();
        return true;  // Send ACK
    }

    private boolean handleBatch(BatchEvent event) {
        int size = event.count();
        log.info("BATCH received: size={}", size);
        batchCount.incrementAndGet();

        for (Event e : event.getEvents()) {
            if (e.getType() == EventType.MESSAGE) {
                DataMessage msg = ((DataMessageEvent) e).getMessage();
                receivedMessages.add(msg);
                messageCount.incrementAndGet();
            } else if (e.getType() == EventType.DELETE) {
                log.debug("DELETE message in batch: {}", ((DeleteMessageEvent) e).getMessage().getKey());
            }
        }

        return true;  // Send ACK
    }

    // Validation methods
    public int getReceivedMessageCount() {
        return receivedMessages.size();
    }

    public List<DataMessage> getReceivedMessages() {
        synchronized (receivedMessages) {
            return new ArrayList<>(receivedMessages);
        }
    }

    public int getResetCount() {
        return resetCount.get();
    }

    public int getReadyCount() {
        return readyCount.get();
    }

    public int getBatchCount() {
        return batchCount.get();
    }

    public int getMessageCount() {
        return messageCount.get();
    }

    public boolean isRunning() {
        return running;
    }

    public void disconnect() {
        log.info("Disconnecting legacy client...");
        running = false;
        try {
            if (socket != null && !socket.isClosed()) {
                socket.close();
            }
            if (eventLoopThread != null) {
                eventLoopThread.join(5000);
            }
        } catch (IOException | InterruptedException e) {
            log.error("Error closing socket", e);
        }
        log.info("Legacy client disconnected");
    }

    // Main method for standalone testing
    public static void main(String[] args) {
        String brokerHost = System.getenv().getOrDefault("BROKER_HOST", "localhost");
        int brokerPort = Integer.parseInt(System.getenv().getOrDefault("BROKER_PORT", "9092"));
        String serviceName = System.getenv().getOrDefault("SERVICE_NAME", "price-quote-service");

        LegacyTestClient client = new LegacyTestClient(brokerHost, brokerPort, serviceName);

        try {
            client.connect();

            // Keep running until Ctrl+C
            Runtime.getRuntime().addShutdownHook(new Thread(() -> {
                log.info("Shutdown hook triggered");
                client.disconnect();
            }));

            log.info("Legacy test client running. Press Ctrl+C to exit.");
            log.info("Messages received: {}", client.getReceivedMessageCount());
            log.info("RESET count: {}", client.getResetCount());
            log.info("READY count: {}", client.getReadyCount());

            // Wait for event loop thread
            while (client.isRunning()) {
                Thread.sleep(1000);
                // Log stats every 10 seconds
                if (System.currentTimeMillis() % 10000 < 1000) {
                    log.info("Stats - Messages: {}, Batches: {}, RESET: {}, READY: {}",
                            client.getMessageCount(),
                            client.getBatchCount(),
                            client.getResetCount(),
                            client.getReadyCount());
                }
            }

        } catch (IOException | InterruptedException e) {
            log.error("Error in legacy test client", e);
        } finally {
            client.disconnect();
        }
    }
}
