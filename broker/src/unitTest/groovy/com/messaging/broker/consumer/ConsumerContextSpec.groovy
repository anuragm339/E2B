package com.messaging.broker.consumer

import com.messaging.common.annotation.Consumer
import com.messaging.common.annotation.RetryPolicy
import com.messaging.common.api.MessageHandler
import com.messaging.common.api.NoOpErrorHandler
import spock.lang.Specification

class ConsumerContextSpec extends Specification {

    def "calculates retry delay for each retry policy"() {
        expect:
        contextFor(RetryPolicy.EXPONENTIAL_THEN_FIXED, Stub(MessageHandler)).with {
            incrementFailures()
            calculateRetryDelay() == 1000L
        }

        contextFor(RetryPolicy.EXPONENTIAL_THEN_FIXED, Stub(MessageHandler)).with {
            6.times { incrementFailures() }
            calculateRetryDelay() == 30_000L
        }

        contextFor(RetryPolicy.SKIP_ON_ERROR, Stub(MessageHandler)).with {
            incrementFailures()
            calculateRetryDelay() == 0L
        }

        contextFor(RetryPolicy.PAUSE_ON_ERROR, Stub(MessageHandler)).with {
            incrementFailures()
            calculateRetryDelay() == Long.MAX_VALUE
        }
    }

    def "tracks failures pause state offsets and adaptive batch size"() {
        given:
        def context = contextFor(RetryPolicy.EXPONENTIAL_THEN_FIXED, Mock(MessageHandler))

        when:
        context.setCurrentOffset(55L)
        context.incrementFailures()
        context.pause()
        context.updateAverageMessageSize(8_192, 4)
        def grownBatchSize = context.currentBatchSize
        context.reduceBatchSizeForRetry()
        context.resume()
        context.resetFailures()

        then:
        context.consumerId == "consumer-1"
        context.topic == "prices-v1"
        context.group == "group-a"
        context.currentOffset == 55L
        grownBatchSize > 0
        context.currentBatchSize == Math.max(1, grownBatchSize.intdiv(2))
        !context.paused
        context.consecutiveFailures == 0
        context.lastFailureTime == 0L
        context.toString().contains("prices-v1")
    }

    private static ConsumerContext contextFor(RetryPolicy policy, MessageHandler handler) {
        def annotation = [
                topic: { "prices-v1" },
                group: { "group-a" },
                retryPolicy: { policy },
                maxExponentialRetries: { 5 },
                initialRetryDelayMs: { 1000L },
                maxRetryDelayMs: { 30_000L },
                fixedRetryIntervalMs: { 30_000L },
                errorHandler: { NoOpErrorHandler.class }
        ] as Consumer

        new ConsumerContext("consumer-1", annotation, handler, new NoOpErrorHandler())
    }
}
