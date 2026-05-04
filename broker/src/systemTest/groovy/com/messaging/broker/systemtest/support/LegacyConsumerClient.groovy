package com.messaging.broker.systemtest.support

import com.messaging.network.legacy.events.AckEvent
import com.messaging.network.legacy.events.Event
import com.messaging.network.legacy.events.RegisterEvent

import java.util.concurrent.ConcurrentLinkedQueue
import java.util.concurrent.atomic.AtomicBoolean

/**
 * Minimal in-process legacy consumer client for system tests.
 *
 * Sends a RegisterEvent with the given serviceName (legacy wire protocol), then reads
 * inbound Events on a background thread so test assertions can poll {@code received}.
 *
 * Usage:
 *   def client = LegacyConsumerClient.connect('127.0.0.1', port, 'price-quote-service')
 *   pollingConditions.eventually { assert client.received.any { it instanceof ReadyEvent } }
 *   client.sendAck()
 *   ...
 *   client.close()
 */
class LegacyConsumerClient implements AutoCloseable {

    final Socket socket
    final DataInputStream input
    final DataOutputStream output
    final String serviceName
    final ConcurrentLinkedQueue<Event> received
    final ConcurrentLinkedQueue<Throwable> errors
    final Thread readerThread
    final AtomicBoolean running

    private LegacyConsumerClient(
        Socket socket,
        DataInputStream input,
        DataOutputStream output,
        String serviceName,
        ConcurrentLinkedQueue<Event> received,
        ConcurrentLinkedQueue<Throwable> errors,
        Thread readerThread,
        AtomicBoolean running
    ) {
        this.socket = socket
        this.input = input
        this.output = output
        this.serviceName = serviceName
        this.received = received
        this.errors = errors
        this.readerThread = readerThread
        this.running = running
    }

    static LegacyConsumerClient connect(String host, int port, String serviceName) {
        def socket = new Socket(host, port)
        socket.soTimeout = 200
        def input  = new DataInputStream(new BufferedInputStream(socket.getInputStream()))
        def output = new DataOutputStream(new BufferedOutputStream(socket.getOutputStream()))
        def received = new ConcurrentLinkedQueue<Event>()
        def errors   = new ConcurrentLinkedQueue<Throwable>()
        def running  = new AtomicBoolean(true)

        Thread reader = Thread.startDaemon("legacy-sys-test-reader-${serviceName}") {
            while (running.get()) {
                try {
                    Event event = Event.from(input)
                    if (event != null) received.add(event)
                } catch (java.net.SocketTimeoutException ignored) {
                } catch (Exception e) {
                    if (running.get()) {
                        errors.add(e)
                        running.set(false)
                    }
                }
            }
        }

        def client = new LegacyConsumerClient(socket, input, output, serviceName, received, errors, reader, running)
        client.sendEvent(new RegisterEvent(1, serviceName))
        client
    }

    void sendAck() {
        sendEvent(AckEvent.INSTANCE)
    }

    synchronized void sendEvent(Event event) {
        output.writeByte(event.getType().ordinal())
        event.toWire(output)
        output.flush()
    }

    boolean isConnected() {
        socket != null && socket.isConnected() && !socket.isClosed()
    }

    /** Clear all received events — call between test phases to avoid stale state. */
    void clearReceived() {
        received.clear()
    }

    @Override
    void close() {
        running.set(false)
        try { socket?.close() } catch (Exception ignored) {}
        try { readerThread?.join(1000) } catch (InterruptedException e) { Thread.currentThread().interrupt() }
    }
}
