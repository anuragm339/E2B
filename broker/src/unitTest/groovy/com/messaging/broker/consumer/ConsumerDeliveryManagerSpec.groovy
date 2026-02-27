package com.messaging.broker.consumer

import com.messaging.common.annotation.Consumer
import com.messaging.common.annotation.RetryPolicy
import com.messaging.common.api.ErrorHandler
import com.messaging.common.api.MessageHandler
import com.messaging.common.api.NoOpErrorHandler
import com.messaging.common.api.StorageEngine
import spock.lang.Specification

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

    private static Consumer stubConsumer(String topic, String group) {
        return [
            topic: { topic },
            group: { group },
            retryPolicy: { RetryPolicy.EXPONENTIAL_THEN_FIXED },
            maxExponentialRetries: { 10 },
            initialRetryDelayMs: { 1000L },
            maxRetryDelayMs: { 30000L },
            fixedRetryIntervalMs: { 30000L },
            errorHandler: { NoOpErrorHandler.class }
        ] as Consumer
    }
}
