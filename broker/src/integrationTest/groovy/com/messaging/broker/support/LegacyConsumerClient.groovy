package com.messaging.broker.support

import com.messaging.network.legacy.events.AckEvent
import com.messaging.network.legacy.events.Event
import com.messaging.network.legacy.events.RegisterEvent

import java.io.BufferedInputStream
import java.io.BufferedOutputStream
import java.io.DataInputStream
import java.io.DataOutputStream
import java.net.Socket
import java.net.SocketTimeoutException
import java.util.concurrent.ConcurrentLinkedQueue
import java.util.concurrent.atomic.AtomicBoolean

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
        def input = new DataInputStream(new BufferedInputStream(socket.getInputStream()))
        def output = new DataOutputStream(new BufferedOutputStream(socket.getOutputStream()))
        def received = new ConcurrentLinkedQueue<Event>()
        def errors = new ConcurrentLinkedQueue<Throwable>()
        def running = new AtomicBoolean(true)

        Thread reader = Thread.startDaemon("legacy-consumer-reader-${serviceName}") {
            while (running.get()) {
                try {
                    Event event = Event.from(input)
                    if (event != null) {
                        received.add(event)
                    }
                } catch (SocketTimeoutException ignored) {
                } catch (Exception e) {
                    if (running.get()) {
                        errors.add(e)
                        running.set(false)
                    }
                }
            }
        }

        def harness = new LegacyConsumerClient(socket, input, output, serviceName, received, errors, reader, running)
        harness.sendEvent(new RegisterEvent(1, serviceName))
        harness
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

    @Override
    void close() {
        running.set(false)
        try {
            socket?.close()
        } catch (Exception ignored) {
        }
        try {
            readerThread?.join(1000)
        } catch (InterruptedException ignored) {
            Thread.currentThread().interrupt()
        }
    }
}
