package com.messaging.broker.consumer

import com.messaging.common.annotation.Consumer
import com.messaging.common.annotation.RetryPolicy
import com.messaging.common.api.ErrorHandler
import com.messaging.common.api.MessageHandler
import com.messaging.common.api.NoOpErrorHandler
import com.messaging.common.model.EventType
import com.messaging.common.api.StorageEngine
import com.messaging.common.model.MessageRecord
import spock.lang.Specification

import java.time.Instant
class ConsumerDeliveryManagerSpec extends Specification {

    def "startConsumerDelivery uses earliest offset when persisted offset is behind"() {
        given:
        def storage = Mock(StorageEngine)
        def processor = Mock(ConsumerAnnotationProcessor)
        def offsetTracker = Mock(ConsumerOffsetTracker)
        def manager = new ConsumerDeliveryManager(storage, processor, offsetTracker)

        and:
        def annotation = stubConsumer("topic", "group")
        def handler = Mock(MessageHandler)
        def errorHandler = new NoOpErrorHandler()
        def context = new ConsumerContext("consumer-1", annotation, handler, errorHandler)
        offsetTracker.getOffset("consumer-1") >> 5L
        storage.getEarliestOffset("topic", 0) >> 10L

        when:
        manager.startConsumerDelivery(context)

        then:
        context.getCurrentOffset() == 10L

        cleanup:
        manager.stopConsumerDelivery("consumer-1")
        manager.shutdown()
    }

    def "resetConsumerOffset updates context and tracker"() {
        given:
        def storage = Mock(StorageEngine)
        def processor = Mock(ConsumerAnnotationProcessor)
        def offsetTracker = Mock(ConsumerOffsetTracker)
        def manager = new ConsumerDeliveryManager(storage, processor, offsetTracker)

        and:
        def annotation = stubConsumer("topic", "group")
        def handler = Mock(MessageHandler)
        def errorHandler = new NoOpErrorHandler()
        def context = new ConsumerContext("consumer-1", annotation, handler, errorHandler)
        processor.getConsumer("consumer-1") >> context

        when:
        manager.resetConsumerOffset("consumer-1", 42L)

        then:
        context.getCurrentOffset() == 42L
        1 * offsetTracker.updateOffset("consumer-1", 42L)

        cleanup:
        manager.shutdown()
    }

    def "pause and resume consumer toggles context state"() {
        given:
        def storage = Mock(StorageEngine)
        def processor = Mock(ConsumerAnnotationProcessor)
        def offsetTracker = Mock(ConsumerOffsetTracker)
        def manager = new ConsumerDeliveryManager(storage, processor, offsetTracker)

        and:
        def annotation = stubConsumer("topic", "group")
        def handler = Mock(MessageHandler)
        def errorHandler = new NoOpErrorHandler()
        def context = new ConsumerContext("consumer-1", annotation, handler, errorHandler)
        processor.getConsumer("consumer-1") >> context

        when:
        manager.pauseConsumer("consumer-1")

        then:
        context.isPaused()

        when:
        manager.resumeConsumer("consumer-1")

        then:
        !context.isPaused()

        cleanup:
        manager.shutdown()
    }

    def "delivery loop advances offset after successful batch delivery"() {
        given:
        def storage = Mock(StorageEngine)
        def processor = Mock(ConsumerAnnotationProcessor)
        def offsetTracker = Mock(ConsumerOffsetTracker)
        def manager = new ConsumerDeliveryManager(storage, processor, offsetTracker)

        and:
        def annotation = stubConsumer("topic", "group")
        def handler = Mock(MessageHandler) {
            1 * handleBatch(_ as List)
        }
        def context = new ConsumerContext("consumer-1", annotation, handler, new NoOpErrorHandler())
        context.setCurrentOffset(0L)
        storage.getCurrentOffset("topic", 0) >> 2L
        storage.read("topic", 0, 0L, 100) >> [
                message(0L, "k1", "abc"),
                message(1L, "k2", "defgh")
        ]

        when:
        invokeDeliverMessages(manager, context)

        then:
        context.currentOffset == 2L
        context.consecutiveFailures == 0
        context.currentBatchSize > 100
        1 * offsetTracker.updateOffset("consumer-1", 2L)

        cleanup:
        manager.shutdown()
    }

    def "skip on error policy advances past the failed batch"() {
        given:
        def storage = Mock(StorageEngine)
        def processor = Mock(ConsumerAnnotationProcessor)
        def offsetTracker = Mock(ConsumerOffsetTracker)
        def manager = new ConsumerDeliveryManager(storage, processor, offsetTracker)
        def errorHandler = Mock(ErrorHandler) {
            1 * onError(_, _ as Exception)
        }

        and:
        def annotation = stubConsumer("topic", "group", RetryPolicy.SKIP_ON_ERROR)
        def handler = Mock(MessageHandler) {
            1 * handleBatch(_ as List) >> { throw new RuntimeException("boom") }
        }
        def context = new ConsumerContext("consumer-1", annotation, handler, errorHandler)
        context.setCurrentOffset(0L)
        storage.getCurrentOffset("topic", 0) >> 2L
        storage.read("topic", 0, 0L, 100) >> [message(0L, "k1", "abc"), message(1L, "k2", "def")]

        when:
        invokeDeliverMessages(manager, context)

        then:
        context.currentOffset == 2L
        context.consecutiveFailures == 0
        !context.paused
        context.currentBatchSize == 50
        1 * offsetTracker.updateOffset("consumer-1", 2L)

        cleanup:
        manager.shutdown()
    }

    def "pause on error policy pauses the consumer after a failed batch"() {
        given:
        def storage = Mock(StorageEngine)
        def processor = Mock(ConsumerAnnotationProcessor)
        def offsetTracker = Mock(ConsumerOffsetTracker)
        def manager = new ConsumerDeliveryManager(storage, processor, offsetTracker)
        def errorHandler = Mock(ErrorHandler) {
            1 * onError(_, _ as Exception)
        }

        and:
        def annotation = stubConsumer("topic", "group", RetryPolicy.PAUSE_ON_ERROR)
        def handler = Mock(MessageHandler) {
            1 * handleBatch(_ as List) >> { throw new RuntimeException("boom") }
        }
        def context = new ConsumerContext("consumer-1", annotation, handler, errorHandler)
        context.setCurrentOffset(0L)
        storage.getCurrentOffset("topic", 0) >> 2L
        storage.read("topic", 0, 0L, 100) >> [message(0L, "k1", "abc"), message(1L, "k2", "def")]

        when:
        invokeDeliverMessages(manager, context)

        then:
        context.paused
        context.consecutiveFailures == 1
        context.currentOffset == 0L
        context.currentBatchSize == 50

        cleanup:
        manager.shutdown()
    }

    def "exponential retry policy keeps offset and failure state for retry"() {
        given:
        def storage = Mock(StorageEngine)
        def processor = Mock(ConsumerAnnotationProcessor)
        def offsetTracker = Mock(ConsumerOffsetTracker)
        def manager = new ConsumerDeliveryManager(storage, processor, offsetTracker)
        def errorHandler = Mock(ErrorHandler) {
            1 * onError(_, _ as Exception)
        }

        and:
        def annotation = stubConsumer("topic", "group", RetryPolicy.EXPONENTIAL_THEN_FIXED)
        def handler = Mock(MessageHandler) {
            1 * handleBatch(_ as List) >> { throw new RuntimeException("boom") }
        }
        def context = new ConsumerContext("consumer-1", annotation, handler, errorHandler)
        context.setCurrentOffset(0L)
        storage.getCurrentOffset("topic", 0) >> 2L
        storage.read("topic", 0, 0L, 100) >> [message(0L, "k1", "abc"), message(1L, "k2", "def")]

        when:
        invokeDeliverMessages(manager, context)

        then:
        !context.paused
        context.consecutiveFailures == 1
        context.currentOffset == 0L
        context.currentBatchSize == 50
        context.lastFailureTime > 0L
        0 * offsetTracker.updateOffset("consumer-1", _)

        cleanup:
        manager.shutdown()
    }

    def "startDelivery starts all discovered consumers and stats can be queried"() {
        given:
        def storage = Stub(StorageEngine) {
            getEarliestOffset("topic", 0) >> 0L
            getCurrentOffset("topic", 0) >> 0L
        }
        def processor = Mock(ConsumerAnnotationProcessor)
        def offsetTracker = Stub(ConsumerOffsetTracker) {
            getOffset("consumer-1") >> 0L
        }
        def manager = new ConsumerDeliveryManager(storage, processor, offsetTracker)
        def context = new ConsumerContext("consumer-1", stubConsumer("topic", "group"), Mock(MessageHandler), new NoOpErrorHandler())
        processor.getAllConsumers() >> [context]
        processor.getConsumer("consumer-1") >> context

        when:
        manager.startDelivery()
        def stats = manager.getConsumerStats("consumer-1")

        then:
        stats != null
        stats.consumerId == "consumer-1"
        stats.topic == "topic"
        stats.group == "group"
        manager.getConsumerStats("missing") == null

        cleanup:
        manager.stopConsumerDelivery("consumer-1")
        manager.shutdown()
    }

    private static Consumer stubConsumer(String topic, String group, RetryPolicy retryPolicy = RetryPolicy.EXPONENTIAL_THEN_FIXED) {
        return [
            topic: { topic },
            group: { group },
            retryPolicy: { retryPolicy },
            maxExponentialRetries: { 10 },
            initialRetryDelayMs: { 1000L },
            maxRetryDelayMs: { 30000L },
            fixedRetryIntervalMs: { 30000L },
            errorHandler: { NoOpErrorHandler.class }
        ] as Consumer
    }

    private static MessageRecord message(long offset, String key, String data) {
        new MessageRecord(offset, "topic", 0, key, EventType.MESSAGE, data, Instant.now())
    }

    private static void invokeDeliverMessages(ConsumerDeliveryManager manager, ConsumerContext context) {
        def method = ConsumerDeliveryManager.getDeclaredMethod("deliverMessages", ConsumerContext)
        method.accessible = true
        method.invoke(manager, context)
    }
}
