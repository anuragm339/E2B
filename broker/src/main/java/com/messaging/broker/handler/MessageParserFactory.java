package com.messaging.broker.handler;

import com.messaging.broker.handler.AckPayloadParser;
import com.messaging.broker.handler.CommitOffsetRequestParser;
import com.messaging.broker.handler.DataMessageRequestParser;
import com.messaging.broker.handler.SubscribeRequestParser;
import com.messaging.broker.handler.JsonAckPayloadParser;
import com.messaging.broker.handler.JsonCommitOffsetRequestParser;
import com.messaging.broker.handler.JsonDataMessageRequestParser;
import com.messaging.broker.handler.JsonSubscribeRequestParser;
import com.messaging.broker.handler.LegacySubscribeRequestParser;
import jakarta.inject.Inject;
import jakarta.inject.Singleton;

import java.nio.charset.StandardCharsets;

/**
 * Factory for creating message parsers.
 *
 * Provides access to all protocol parsers and handles protocol detection.
 */
@Singleton
public class MessageParserFactory {

    private final JsonSubscribeRequestParser modernSubscribeParser;
    private final LegacySubscribeRequestParser legacySubscribeParser;
    private final JsonCommitOffsetRequestParser commitOffsetParser;
    private final JsonDataMessageRequestParser dataMessageParser;
    private final JsonAckPayloadParser ackPayloadParser;

    @Inject
    public MessageParserFactory(
            JsonSubscribeRequestParser modernSubscribeParser,
            LegacySubscribeRequestParser legacySubscribeParser,
            JsonCommitOffsetRequestParser commitOffsetParser,
            JsonDataMessageRequestParser dataMessageParser,
            JsonAckPayloadParser ackPayloadParser) {
        this.modernSubscribeParser = modernSubscribeParser;
        this.legacySubscribeParser = legacySubscribeParser;
        this.commitOffsetParser = commitOffsetParser;
        this.dataMessageParser = dataMessageParser;
        this.ackPayloadParser = ackPayloadParser;
    }

    /**
     * Get SUBSCRIBE parser with automatic protocol detection.
     *
     * Detects whether payload is modern or legacy protocol format.
     *
     * @param payload Message payload
     * @return Appropriate SUBSCRIBE parser
     */
    public SubscribeRequestParser getSubscribeParser(byte[] payload) {
        // Auto-detect based on payload format
        try {
            String json = new String(payload, StandardCharsets.UTF_8);
            if (json.contains("\"isLegacy\":true") || json.contains("\"isLegacy\": true")) {
                return legacySubscribeParser;
            }
            return modernSubscribeParser;
        } catch (Exception e) {
            return modernSubscribeParser;  // Default to modern
        }
    }

    /**
     * Get modern protocol SUBSCRIBE parser.
     *
     * @return Modern SUBSCRIBE parser
     */
    public SubscribeRequestParser getModernSubscribeParser() {
        return modernSubscribeParser;
    }

    /**
     * Get legacy protocol SUBSCRIBE parser.
     *
     * @return Legacy SUBSCRIBE parser
     */
    public SubscribeRequestParser getLegacySubscribeParser() {
        return legacySubscribeParser;
    }

    /**
     * Get COMMIT_OFFSET parser.
     *
     * @return COMMIT_OFFSET parser
     */
    public CommitOffsetRequestParser getCommitOffsetParser() {
        return commitOffsetParser;
    }

    /**
     * Get DATA message parser.
     *
     * @return DATA parser
     */
    public DataMessageRequestParser getDataMessageParser() {
        return dataMessageParser;
    }

    /**
     * Get BATCH_ACK parser.
     *
     * @return ACK payload parser
     */
    public AckPayloadParser getAckPayloadParser() {
        return ackPayloadParser;
    }
}
