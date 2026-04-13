package com.messaging.broker.consumer

import com.messaging.broker.model.DeliveryKey
import com.messaging.broker.monitoring.BrokerMetrics
import com.messaging.broker.monitoring.ConsumerEventLogger
import com.messaging.broker.monitoring.DataRefreshMetrics
import com.messaging.common.api.BatchReadableStorage
import com.messaging.common.api.NetworkServer
import com.messaging.common.api.StorageEngine
import com.messaging.common.model.DeliveryBatch
import io.micrometer.core.instrument.Timer
import spock.lang.Specification

import java.nio.channels.WritableByteChannel
import java.util.concurrent.CompletableFuture
import java.util.concurrent.CopyOnWriteArrayList
import java.util.concurrent.CountDownLatch
import java.util.concurrent.ExecutorService
import java.util.concurrent.Executors
import java.util.concurrent.ScheduledExecutorService
import java.util.concurrent.atomic.AtomicBoolean

class BatchDeliveryServiceSpec extends Specification {

    NetworkServer server = Mock()
    StorageEngine storage = Mock()
    BatchReadableStorage batchStorage = Mock()
    ConsumerStateService stateService = Mock()
    ConsumerReadinessService readinessService = Mock()
    ConsumerOffsetTracker offsetTracker = Mock()
    BrokerMetrics metrics = Mock()
    DataRefreshMetrics dataRefreshMetrics = Mock()
    ConsumerRegistrationService registrationService = Mock()
    ScheduledExecutorService scheduler = Executors.newSingleThreadScheduledExecutor()
    ExecutorService storageExecutor = Executors.newSingleThreadExecutor()
    ConsumerEventLogger consumerLogger = Mock()

    BatchDeliveryService service = new BatchDeliveryService(
            server, storage, batchStorage, stateService, readinessService, offsetTracker,
            metrics, dataRefreshMetrics, registrationService, scheduler, storageExecutor,
            100, 1, 1, consumerLogger
    )

    def cleanup() {
        scheduler.shutdownNow()
        storageExecutor.shutdownNow()
    }

    def "deliverBatch blocks when consumer is not ready"() {
        given:
        def consumer = new RemoteConsumer("client-1", "prices-v1", "group-a")
        readinessService.isModernConsumerTopicReady("client-1", "prices-v1", "group-a") >> false

        expect:
        service.deliverBatch(consumer, 1024).reason() == "not-ready"
    }

    def "deliverBatch blocks when another delivery is already in flight"() {
        given:
        def consumer = new RemoteConsumer("client-1", "prices-v1", "group-a")
        readinessService.isModernConsumerTopicReady("client-1", "prices-v1", "group-a") >> true
        stateService.markInFlight(DeliveryKey.of("group-a", "prices-v1")) >> new AtomicBoolean(true)

        expect:
        service.deliverBatch(consumer, 1024).reason() == "in-flight"
    }

    def "deliverBatch blocks when pending ack exists"() {
        given:
        def consumer = new RemoteConsumer("client-2", "prices-v1", "group-a")
        readinessService.isModernConsumerTopicReady("client-2", "prices-v1", "group-a") >> true
        stateService.markInFlight(DeliveryKey.of("group-a", "prices-v1")) >> new AtomicBoolean(false)
        stateService.getPendingOffset(DeliveryKey.of("group-a", "prices-v1")) >>> [99L, 99L]

        expect:
        service.deliverBatch(consumer, 1024).reason() == "pending-ack"
    }

    def "deliverBatch unregisters consumer after max consecutive failures"() {
        given:
        def consumer = new RemoteConsumer("client-3", "prices-v1", "group-a")
        10.times { consumer.recordFailure() }
        readinessService.isModernConsumerTopicReady("client-3", "prices-v1", "group-a") >> true
        stateService.markInFlight(DeliveryKey.of("group-a", "prices-v1")) >> new AtomicBoolean(false)
        stateService.getPendingOffset(DeliveryKey.of("group-a", "prices-v1")) >> null

        when:
        def result = service.deliverBatch(consumer, 1024)

        then:
        result.reason() == "max-failures-exceeded"
        1 * registrationService.unregisterConsumer("client-3")
    }

    def "deliverBatch blocks during backoff window after failures"() {
        given:
        def consumer = new RemoteConsumer("client-4", "prices-v1", "group-a")
        consumer.recordFailure()
        readinessService.isModernConsumerTopicReady("client-4", "prices-v1", "group-a") >> true
        stateService.markInFlight(DeliveryKey.of("group-a", "prices-v1")) >> new AtomicBoolean(false)
        stateService.getPendingOffset(DeliveryKey.of("group-a", "prices-v1")) >> null

        expect:
        service.deliverBatch(consumer, 1024).reason() == "backoff"
    }

    def "deliverBatch returns no data for empty batch and closes payload"() {
        given:
        def consumer = new RemoteConsumer("client-1", "prices-v1", "group-a")
        readinessService.isModernConsumerTopicReady("client-1", "prices-v1", "group-a") >> true
        stateService.markInFlight(DeliveryKey.of("group-a", "prices-v1")) >> new AtomicBoolean(false)
        stateService.getPendingOffset(DeliveryKey.of("group-a", "prices-v1")) >> null
        metrics.startStorageReadTimer() >> Mock(Timer.Sample)
        def emptyBatch = new TrackingBatch("prices-v1", new byte[0], 0, 0)
        batchStorage.getBatch("prices-v1", 0, 0L, 1024) >> emptyBatch

        when:
        def result = service.deliverBatch(consumer, 1024)

        then:
        result.reason() == "no-data"
        emptyBatch.closed
        1 * metrics.stopStorageReadTimer(_ as Timer.Sample)
        1 * metrics.recordStorageRead()
    }

    def "deliverBatch reverts offset and records transient send failure"() {
        given:
        def consumer = new RemoteConsumer("client-1", "prices-v1", "group-a")
        readinessService.isModernConsumerTopicReady("client-1", "prices-v1", "group-a") >> true
        stateService.markInFlight(DeliveryKey.of("group-a", "prices-v1")) >> new AtomicBoolean(false)
        stateService.getPendingOffset(DeliveryKey.of("group-a", "prices-v1")) >> null
        metrics.startStorageReadTimer() >> Mock(Timer.Sample)
        def dataBatch = new TrackingBatch("prices-v1", 'abc'.bytes, 1, 0)
        batchStorage.getBatch("prices-v1", 0, 0L, 1024) >> dataBatch
        metrics.startConsumerDeliveryTimer() >> Mock(Timer.Sample)
        server.sendBatch("client-1", "group-a", dataBatch) >> CompletableFuture.failedFuture(new RuntimeException("boom"))

        when:
        def failed = service.deliverBatch(consumer, 1024)

        then:
        failed.reason() == "java.lang.RuntimeException: boom"
        consumer.currentOffset == 0L
        consumer.consecutiveFailures > 0
        1 * metrics.stopStorageReadTimer(_ as Timer.Sample)
        1 * metrics.recordStorageRead()
        1 * stateService.clearFromOffset(DeliveryKey.of("group-a", "prices-v1"))
        1 * metrics.recordConsumerTransferFailed("client-1", "prices-v1", "group-a", 1, 3)
        1 * metrics.recordConsumerFailure("client-1", "prices-v1", "group-a")
    }

    def "deliverBatch succeeds records lag and refresh metrics during replay"() {
        given:
        def consumer = new RemoteConsumer("client-1", "prices-v1", "group-a")
        consumer.recordFailure()
        consumer.lastFailureTime = System.currentTimeMillis() - consumer.getBackoffDelay() - 10
        def deliveryKey = DeliveryKey.of("group-a", "prices-v1")
        def batch = new TrackingBatch("prices-v1", 'abc'.bytes, 2, 4)
        def refreshCoordinator = Mock(RefreshCoordinator)
        def refreshContext = new RefreshContext("prices-v1", [] as Set)
        refreshContext.setState(RefreshState.REPLAYING)
        refreshContext.setRefreshId("refresh-1")
        service.setDataRefreshCoordinator(refreshCoordinator)

        readinessService.isModernConsumerTopicReady("client-1", "prices-v1", "group-a") >> true
        stateService.markInFlight(deliveryKey) >> new AtomicBoolean(false)
        stateService.getPendingOffset(deliveryKey) >> null
        metrics.startStorageReadTimer() >> Mock(Timer.Sample)
        metrics.startConsumerDeliveryTimer() >> Mock(Timer.Sample)
        batchStorage.getBatch("prices-v1", 0, 0L, 1024) >> batch
        server.sendBatch("client-1", "group-a", batch) >> CompletableFuture.completedFuture(null)
        refreshCoordinator.getRefreshStatus("prices-v1") >> refreshContext
        storage.getCurrentOffset("prices-v1", 0) >> 10L

        when:
        def result = service.deliverBatch(consumer, 1024)

        then:
        result.delivered()
        consumer.currentOffset == 5L
        consumer.consecutiveFailures == 0
        1 * metrics.recordConsumerRetry("client-1", "prices-v1", "group-a")
        1 * metrics.recordBatchSize(2)
        1 * stateService.setPendingOffset(deliveryKey, 5L)
        1 * stateService.setFromOffset(deliveryKey, 0L)
        1 * stateService.recordTraceId(deliveryKey, _ as String)
        1 * metrics.startPendingAck("prices-v1", "group-a")
        1 * stateService.recordBatchSendTime(deliveryKey, _ as Long)
        1 * stateService.scheduleTimeout(deliveryKey, _)
        1 * metrics.stopConsumerDeliveryTimer(_ as Timer.Sample, "client-1", "prices-v1", "group-a")
        1 * dataRefreshMetrics.recordDataTransferred("prices-v1", "group-a", 3L, 2, "refresh-1", "LOCAL")
        1 * metrics.recordConsumerBatchSent("client-1", "prices-v1", "group-a", 2, 3L)
        1 * metrics.updateConsumerLag("client-1", "prices-v1", "group-a", 5L)
    }

    def "deliverBatch clears pending state and unregisters on permanent failure"() {
        given:
        def consumer = new RemoteConsumer("client-1", "prices-v1", "group-a")
        9.times { consumer.recordFailure() }
        consumer.lastFailureTime = System.currentTimeMillis() - consumer.getBackoffDelay() - 10
        def deliveryKey = DeliveryKey.of("group-a", "prices-v1")
        def batch = new TrackingBatch("prices-v1", 'abc'.bytes, 1, 0)

        readinessService.isModernConsumerTopicReady("client-1", "prices-v1", "group-a") >> true
        stateService.markInFlight(deliveryKey) >> new AtomicBoolean(false)
        stateService.getPendingOffset(deliveryKey) >> null
        metrics.startStorageReadTimer() >> Mock(Timer.Sample)
        metrics.startConsumerDeliveryTimer() >> Mock(Timer.Sample)
        batchStorage.getBatch("prices-v1", 0, 0L, 1024) >> batch
        server.sendBatch("client-1", "group-a", batch) >> CompletableFuture.failedFuture(new RuntimeException("boom"))

        when:
        def result = service.deliverBatch(consumer, 1024)

        then:
        result.reason() == "java.lang.RuntimeException: boom"
        consumer.currentOffset == 0L
        consumer.consecutiveFailures == 10
        1 * metrics.recordConsumerFailure("client-1", "prices-v1", "group-a")
        1 * stateService.clearFromOffset(deliveryKey)
        1 * stateService.clearPendingOffset(deliveryKey)
        1 * stateService.recordBatchSendTime(deliveryKey, 0L)
        1 * stateService.clearTraceId(deliveryKey)
        1 * registrationService.unregisterConsumer("client-1")
        1 * metrics.recordConsumerTransferFailed("client-1", "prices-v1", "group-a", 1, 3)
    }

    def "delivery helper methods clear state and block failed consumers"() {
        given:
        def consumer = new RemoteConsumer("client-1", "prices-v1", "group-a")
        10.times { consumer.recordFailure() }
        def healthyConsumer = new RemoteConsumer("client-3", "prices-v1", "group-a")
        def overFailedConsumer = new RemoteConsumer("client-4", "prices-v1", "group-a")
        10.times { overFailedConsumer.recordFailure() }
        readinessService.isModernConsumerTopicReady("client-3", "prices-v1", "group-a") >> true
        readinessService.isModernConsumerTopicReady("client-4", "prices-v1", "group-a") >> true
        stateService.isInFlight(_) >> false
        stateService.getPendingOffset(_) >> null

        when:
        def canDeliver = service.canDeliver(healthyConsumer)
        def blockedForFailures = service.canDeliver(overFailedConsumer)
        service.resetDeliveryState(consumer)
        service.handleDeliverySuccess(consumer)
        service.handleDeliveryFailure(consumer, new RuntimeException("x"))

        then:
        canDeliver
        !blockedForFailures
        1 * stateService.removeDeliveryState(DeliveryKey.of("group-a", "prices-v1"))
        1 * stateService.clearInFlight(DeliveryKey.of("group-a", "prices-v1"))
        1 * metrics.recordConsumerFailure("client-1", "prices-v1", "group-a")
    }

    def "deliverBatch skips readiness check for legacy consumer"() {
        given:
        def consumer = new RemoteConsumer("client-legacy", "prices-v1", "group-a", true)
        stateService.markInFlight(DeliveryKey.of("group-a", "prices-v1")) >> new AtomicBoolean(false)
        stateService.getPendingOffset(DeliveryKey.of("group-a", "prices-v1")) >> null
        metrics.startStorageReadTimer() >> Mock(Timer.Sample)
        def emptyBatch = new TrackingBatch("prices-v1", new byte[0], 0, 0)
        batchStorage.getBatch("prices-v1", 0, 0L, 1024) >> emptyBatch

        when:
        def result = service.deliverBatch(consumer, 1024)

        then:
        result.reason() == "no-data"
        0 * readinessService.isModernConsumerTopicReady(_, _, _)
    }

    def "canDeliver skips readiness check for legacy consumer"() {
        given:
        def consumer = new RemoteConsumer("client-legacy", "prices-v1", "group-a", true)
        stateService.isInFlight(DeliveryKey.of("group-a", "prices-v1")) >> false
        stateService.getPendingOffset(DeliveryKey.of("group-a", "prices-v1")) >> null

        when:
        def result = service.canDeliver(consumer)

        then:
        result
        0 * readinessService.isModernConsumerTopicReady(_, _, _)
    }

    def "canDeliver returns false when in-flight"() {
        given:
        def consumer = new RemoteConsumer("client-1", "prices-v1", "group-a")
        readinessService.isModernConsumerTopicReady("client-1", "prices-v1", "group-a") >> true
        stateService.isInFlight(DeliveryKey.of("group-a", "prices-v1")) >> true

        expect:
        !service.canDeliver(consumer)
    }

    def "canDeliver returns false when pending offset exists"() {
        given:
        def consumer = new RemoteConsumer("client-1", "prices-v1", "group-a")
        readinessService.isModernConsumerTopicReady("client-1", "prices-v1", "group-a") >> true
        stateService.isInFlight(DeliveryKey.of("group-a", "prices-v1")) >> false
        stateService.getPendingOffset(DeliveryKey.of("group-a", "prices-v1")) >> 42L

        expect:
        !service.canDeliver(consumer)
    }

    def "canDeliver returns false during active backoff window"() {
        given:
        def consumer = new RemoteConsumer("client-1", "prices-v1", "group-a")
        consumer.recordFailure()
        consumer.lastFailureTime = System.currentTimeMillis()  // failure just occurred
        readinessService.isModernConsumerTopicReady("client-1", "prices-v1", "group-a") >> true
        stateService.isInFlight(DeliveryKey.of("group-a", "prices-v1")) >> false
        stateService.getPendingOffset(DeliveryKey.of("group-a", "prices-v1")) >> null

        expect:
        !service.canDeliver(consumer)
    }

    def "deliverBatch records no refresh metrics when coordinator is null"() {
        given:
        def consumer = new RemoteConsumer("client-1", "prices-v1", "group-a")
        def deliveryKey = DeliveryKey.of("group-a", "prices-v1")
        def batch = new TrackingBatch("prices-v1", 'abc'.bytes, 1, 3)
        // dataRefreshCoordinator is null — setDataRefreshCoordinator never called

        readinessService.isModernConsumerTopicReady("client-1", "prices-v1", "group-a") >> true
        stateService.markInFlight(deliveryKey) >> new AtomicBoolean(false)
        stateService.getPendingOffset(deliveryKey) >> null
        metrics.startStorageReadTimer() >> Mock(Timer.Sample)
        metrics.startConsumerDeliveryTimer() >> Mock(Timer.Sample)
        batchStorage.getBatch("prices-v1", 0, 0L, 1024) >> batch
        server.sendBatch("client-1", "group-a", batch) >> CompletableFuture.completedFuture(null)
        storage.getCurrentOffset("prices-v1", 0) >> 10L

        when:
        def result = service.deliverBatch(consumer, 1024)

        then:
        result.delivered()
        0 * dataRefreshMetrics.recordDataTransferred(_, _, _, _, _, _)
    }

    def "deliverBatch records no refresh metrics when no refresh context exists for topic"() {
        given:
        def consumer = new RemoteConsumer("client-1", "prices-v1", "group-a")
        def deliveryKey = DeliveryKey.of("group-a", "prices-v1")
        def batch = new TrackingBatch("prices-v1", 'abc'.bytes, 1, 3)
        def refreshCoordinator = Mock(RefreshCoordinator)
        service.setDataRefreshCoordinator(refreshCoordinator)

        readinessService.isModernConsumerTopicReady("client-1", "prices-v1", "group-a") >> true
        stateService.markInFlight(deliveryKey) >> new AtomicBoolean(false)
        stateService.getPendingOffset(deliveryKey) >> null
        metrics.startStorageReadTimer() >> Mock(Timer.Sample)
        metrics.startConsumerDeliveryTimer() >> Mock(Timer.Sample)
        batchStorage.getBatch("prices-v1", 0, 0L, 1024) >> batch
        server.sendBatch("client-1", "group-a", batch) >> CompletableFuture.completedFuture(null)
        storage.getCurrentOffset("prices-v1", 0) >> 10L
        refreshCoordinator.getRefreshStatus("prices-v1") >> null

        when:
        def result = service.deliverBatch(consumer, 1024)

        then:
        result.delivered()
        0 * dataRefreshMetrics.recordDataTransferred(_, _, _, _, _, _)
    }

    def "deliverBatch records no refresh metrics when refresh state is not REPLAYING"() {
        given:
        def consumer = new RemoteConsumer("client-1", "prices-v1", "group-a")
        def deliveryKey = DeliveryKey.of("group-a", "prices-v1")
        def batch = new TrackingBatch("prices-v1", 'abc'.bytes, 1, 3)
        def refreshCoordinator = Mock(RefreshCoordinator)
        def refreshContext = new RefreshContext("prices-v1", [] as Set)
        refreshContext.setState(RefreshState.COMPLETED)
        service.setDataRefreshCoordinator(refreshCoordinator)

        readinessService.isModernConsumerTopicReady("client-1", "prices-v1", "group-a") >> true
        stateService.markInFlight(deliveryKey) >> new AtomicBoolean(false)
        stateService.getPendingOffset(deliveryKey) >> null
        metrics.startStorageReadTimer() >> Mock(Timer.Sample)
        metrics.startConsumerDeliveryTimer() >> Mock(Timer.Sample)
        batchStorage.getBatch("prices-v1", 0, 0L, 1024) >> batch
        server.sendBatch("client-1", "group-a", batch) >> CompletableFuture.completedFuture(null)
        storage.getCurrentOffset("prices-v1", 0) >> 10L
        refreshCoordinator.getRefreshStatus("prices-v1") >> refreshContext

        when:
        def result = service.deliverBatch(consumer, 1024)

        then:
        result.delivered()
        0 * dataRefreshMetrics.recordDataTransferred(_, _, _, _, _, _)
    }

    def "deliverBatch returns success even when lag metric update throws"() {
        given:
        def consumer = new RemoteConsumer("client-1", "prices-v1", "group-a")
        def deliveryKey = DeliveryKey.of("group-a", "prices-v1")
        def batch = new TrackingBatch("prices-v1", 'abc'.bytes, 1, 3)

        readinessService.isModernConsumerTopicReady("client-1", "prices-v1", "group-a") >> true
        stateService.markInFlight(deliveryKey) >> new AtomicBoolean(false)
        stateService.getPendingOffset(deliveryKey) >> null
        metrics.startStorageReadTimer() >> Mock(Timer.Sample)
        metrics.startConsumerDeliveryTimer() >> Mock(Timer.Sample)
        batchStorage.getBatch("prices-v1", 0, 0L, 1024) >> batch
        server.sendBatch("client-1", "group-a", batch) >> CompletableFuture.completedFuture(null)
        storage.getCurrentOffset("prices-v1", 0) >> { throw new RuntimeException("storage-unavailable") }

        when:
        def result = service.deliverBatch(consumer, 1024)

        then:
        result.delivered()
    }

    // ── Concurrency: Fix 2 ───────────────────────────────────────────────────

    def "removePendingOffset is exclusive — only one concurrent caller receives the offset"() {
        // Verifies the atomic-remove gate used by both the ACK handler and the timeout handler.
        // Without ConcurrentHashMap.remove() as the gate, two threads could both see a non-null
        // pending offset and both attempt to commit or revert, causing double-advance / double-revert.
        given:
        def store = new InMemoryInFlightDeliveryStore()
        def key = DeliveryKey.of("group-a", "prices-v1")
        store.setPendingOffset(key, 42L)

        def startLatch = new CountDownLatch(1)
        def winners = new CopyOnWriteArrayList<Long>()

        def threads = (1..20).collect {
            Thread.start {
                startLatch.await()
                def claimed = store.removePendingOffset(key)
                if (claimed != null) winners.add(claimed)
            }
        }

        when:
        startLatch.countDown()
        threads*.join()

        then:
        winners.size() == 1
        winners[0] == 42L
        store.getPendingOffset(key) == null
    }

    def "deliverBatch does not record transfer metrics when batch read fails before assignment"() {
        given:
        def consumer = new RemoteConsumer("client-1", "prices-v1", "group-a")
        def deliveryKey = DeliveryKey.of("group-a", "prices-v1")

        readinessService.isModernConsumerTopicReady("client-1", "prices-v1", "group-a") >> true
        stateService.markInFlight(deliveryKey) >> new AtomicBoolean(false)
        stateService.getPendingOffset(deliveryKey) >> null
        metrics.startStorageReadTimer() >> Mock(Timer.Sample)
        batchStorage.getBatch("prices-v1", 0, 0L, 1024) >> { throw new RuntimeException("disk-read-error") }

        when:
        def result = service.deliverBatch(consumer, 1024)

        then:
        !result.delivered()
        0 * metrics.recordConsumerTransferFailed(_, _, _, _, _)
    }

    private static final class TrackingBatch implements DeliveryBatch {
        final String topic
        final byte[] data
        final int recordCount
        final long firstOffset
        final long lastOffset
        boolean closed = false

        TrackingBatch(String topic, byte[] data, int recordCount, long lastOffset) {
            this.topic = topic
            this.data = data
            this.recordCount = recordCount
            this.firstOffset = lastOffset > 0 ? lastOffset : 0L
            this.lastOffset = lastOffset
        }

        @Override
        String getTopic() { topic }

        @Override
        int getRecordCount() { recordCount }

        @Override
        long getTotalBytes() { data.length }

        @Override
        long getFirstOffset() { firstOffset }

        @Override
        long getLastOffset() { lastOffset }

        @Override
        long transferTo(WritableByteChannel target, long position) { -1L }

        @Override
        void close() { closed = true }
    }
}
