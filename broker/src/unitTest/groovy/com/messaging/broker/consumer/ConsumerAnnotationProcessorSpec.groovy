package com.messaging.broker.consumer

import com.messaging.common.annotation.Consumer
import com.messaging.common.annotation.RetryPolicy
import com.messaging.common.api.ErrorHandler
import com.messaging.common.api.MessageHandler
import com.messaging.common.api.NoOpErrorHandler
import com.messaging.common.model.ConsumerRecord
import io.micronaut.context.ApplicationContext
import spock.lang.Specification

class ConsumerAnnotationProcessorSpec extends Specification {

    def "scans annotated handlers and registers consumers"() {
        given:
        def context = Mock(ApplicationContext)
        def processor = new ConsumerAnnotationProcessor(context)
        def errorHandler = Mock(ErrorHandler)
        def handler = new AnnotatedHandler()
        def nonAnnotated = new PlainHandler()

        context.getBeansOfType(MessageHandler) >> [handler, nonAnnotated]
        context.createBean(CustomErrorHandler) >> errorHandler

        when:
        processor.onApplicationEvent(null)

        then:
        processor.allConsumers.size() == 1
        def registered = processor.allConsumers.first()
        registered.topic == "prices-v1"
        registered.group == "group-a"
        registered.retryPolicy == RetryPolicy.SKIP_ON_ERROR
        registered.errorHandler.is(errorHandler)
        processor.getConsumersByTopic("prices-v1")*.consumerId == [registered.consumerId]
        processor.getConsumer(registered.consumerId).is(registered)

        when:
        processor.unregisterConsumer(registered.consumerId)

        then:
        processor.allConsumers.isEmpty()
    }

    def "falls back to no op error handler when custom handler creation fails"() {
        given:
        def context = Mock(ApplicationContext)
        def processor = new ConsumerAnnotationProcessor(context)

        context.getBeansOfType(MessageHandler) >> [new AnnotatedHandler()]
        context.createBean(CustomErrorHandler) >> { throw new IllegalStateException("boom") }
        context.getBean(NoOpErrorHandler) >> new NoOpErrorHandler()

        when:
        processor.onApplicationEvent(null)

        then:
        processor.allConsumers.size() == 1
        processor.allConsumers.first().errorHandler instanceof NoOpErrorHandler
    }

    @Consumer(
            topic = "prices-v1",
            group = "group-a",
            retryPolicy = RetryPolicy.SKIP_ON_ERROR,
            errorHandler = CustomErrorHandler.class
    )
    static class AnnotatedHandler implements MessageHandler {
        @Override
        void handleBatch(List<ConsumerRecord> records) throws Exception {
        }
    }

    static class PlainHandler implements MessageHandler {
        @Override
        void handleBatch(List<ConsumerRecord> records) throws Exception {
        }
    }

    static class CustomErrorHandler implements ErrorHandler {
        @Override
        void onError(ConsumerRecord record, Exception error) {
        }
    }
}
