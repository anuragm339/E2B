package com.messaging.broker.systemtest.support

import com.example.consumer.GenericConsumerHandler
import com.messaging.common.annotation.Consumer
import com.messaging.common.api.MessageHandler
import com.messaging.common.model.ConsumerRecord
import io.micronaut.context.annotation.Replaces
import io.micronaut.context.annotation.Requires
import jakarta.inject.Singleton

import java.util.concurrent.CopyOnWriteArrayList
import java.util.concurrent.CountDownLatch
import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.AtomicInteger

/**
 * Test-only replacement for GenericConsumerHandler.
 *
 * @Replaces(GenericConsumerHandler) prevents both beans from registering so there
 * is exactly one @Consumer bean in the test context.  CountDownLatch-based waiting
 * lets journey specs block until the expected number of records arrive.
 */
@Singleton
@Replaces(GenericConsumerHandler)
@Requires(property = "messaging.broker.host")   // only activate in consumer contexts, not the broker
@Consumer(
    topic = '${consumer.topics:prices-v1}',
    group  = '${consumer.group:system-test-group}'
)
class TestRecordCollector implements MessageHandler {

    private final CopyOnWriteArrayList<ConsumerRecord> received = []
    private volatile CountDownLatch latch = new CountDownLatch(0)
    private final AtomicInteger resetCount = new AtomicInteger()
    private final AtomicInteger readyCount = new AtomicInteger()

    /**
     * Block until at least {@code n} records are collected or the timeout fires.
     * Returns a snapshot of all records collected so far.
     */
    List<ConsumerRecord> waitForRecords(int n, int timeoutSecs = 15) {
        latch = new CountDownLatch(n)
        latch.await(timeoutSecs, TimeUnit.SECONDS)
        received.toList()
    }

    @Override
    void handleBatch(List<ConsumerRecord> records) throws Exception {
        received.addAll(records)
        records.each { latch.countDown() }
    }

    @Override
    void onReset(String topic) throws Exception {
        resetCount.incrementAndGet()
    }

    @Override
    void onReady(String topic) throws Exception {
        readyCount.incrementAndGet()
    }

    int getResetCount() { resetCount.get() }
    int getReadyCount() { readyCount.get() }
    List<ConsumerRecord> getAll() { received.toList() }

    void reset() {
        received.clear()
        resetCount.set(0)
        readyCount.set(0)
    }
}
