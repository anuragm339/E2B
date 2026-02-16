package com.messaging.common.exception;

/**
 * Exception for consumer-related errors
 */
public class ConsumerException extends MessagingException {

    private String consumerId;
    private String consumerGroup;
    private String topic;

    public ConsumerException(ErrorCode errorCode, String message) {
        super(errorCode, message);
    }

    public ConsumerException(ErrorCode errorCode, String message, Throwable cause) {
        super(errorCode, message, cause);
    }

    public ConsumerException withConsumerId(String consumerId) {
        this.consumerId = consumerId;
        withContext("consumerId", consumerId);
        return this;
    }

    public ConsumerException withConsumerGroup(String consumerGroup) {
        this.consumerGroup = consumerGroup;
        withContext("consumerGroup", consumerGroup);
        return this;
    }

    public ConsumerException withTopic(String topic) {
        this.topic = topic;
        withContext("topic", topic);
        return this;
    }

    public static ConsumerException notRegistered(String group, String topic) {
        return new ConsumerException(
                ErrorCode.CONSUMER_NOT_REGISTERED,
                String.format("Consumer not registered: group=%s topic=%s", group, topic))
                .withConsumerGroup(group)
                .withTopic(topic);
    }

    public static ConsumerException offsetCommitFailed(String group, String topic, long offset, Throwable cause) {
        ConsumerException ex = new ConsumerException(
                ErrorCode.CONSUMER_OFFSET_COMMIT_FAILED,
                String.format("Offset commit failed: group=%s topic=%s offset=%d", group, topic, offset),
                cause)
                .withConsumerGroup(group)
                .withTopic(topic);
        ex.withContext("offset", offset);
        return ex;
    }

    public static ConsumerException batchDeliveryFailed(String consumerId, String topic, int batchSize, Throwable cause) {
        ConsumerException ex = new ConsumerException(
                ErrorCode.CONSUMER_BATCH_DELIVERY_FAILED,
                String.format("Batch delivery failed: consumerId=%s topic=%s batchSize=%d", consumerId, topic, batchSize),
                cause)
                .withConsumerId(consumerId)
                .withTopic(topic);
        ex.withContext("batchSize", batchSize);
        return ex;
    }
}
