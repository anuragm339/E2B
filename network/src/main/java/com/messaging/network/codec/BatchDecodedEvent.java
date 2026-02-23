package com.messaging.network.codec;

import com.messaging.common.model.ConsumerRecord;
import java.util.List;

/**
 * Event emitted by ZeroCopyBatchDecoder after successfully decoding a batch.
 * Contains the decoded records, topic name, and group for BATCH_ACK.
 *
 * MULTI-GROUP FIX: Group field added to support multiple groups per topic on one connection.
 */
public record BatchDecodedEvent(List<ConsumerRecord> records, String topic, String group) {
}
