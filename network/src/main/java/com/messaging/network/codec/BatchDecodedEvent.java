package com.messaging.network.codec;

import com.messaging.common.model.ConsumerRecord;
import java.util.List;

/**
 * Event emitted by ZeroCopyBatchDecoder after successfully decoding a batch.
 * Contains the decoded records and the topic name for BATCH_ACK.
 */
public record BatchDecodedEvent(List<ConsumerRecord> records, String topic) {
}
