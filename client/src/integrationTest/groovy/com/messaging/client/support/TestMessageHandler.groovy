package com.messaging.client.support

import com.messaging.common.annotation.Consumer
import com.messaging.common.api.MessageHandler
import com.messaging.common.model.ConsumerRecord
import jakarta.inject.Singleton

import java.util.concurrent.CopyOnWriteArrayList

/**
 * Test-only MessageHandler bean.
 *
 * @Consumer makes ClientConsumerManager discover it and subscribe 'test-topic' / 'test-group'.
 * @Singleton registers it in the Micronaut context so @Requires(beans = MessageHandler) is satisfied.
 * Records every handleBatch / onReset / onReady call for spec assertions.
 */
@Singleton
@Consumer(topic = 'test-topic', group = 'test-group')
class TestMessageHandler implements MessageHandler {

    final List<List<ConsumerRecord>> batches     = new CopyOnWriteArrayList<>()
    final List<String>               resetTopics = new CopyOnWriteArrayList<>()
    final List<String>               readyTopics = new CopyOnWriteArrayList<>()

    @Override
    void handleBatch(List<ConsumerRecord> records) {
        batches.add(new ArrayList<>(records))
    }

    @Override
    void onReset(String topic) {
        resetTopics.add(topic)
    }

    @Override
    void onReady(String topic) {
        readyTopics.add(topic)
    }

    void clear() {
        batches.clear()
        resetTopics.clear()
        readyTopics.clear()
    }
}
