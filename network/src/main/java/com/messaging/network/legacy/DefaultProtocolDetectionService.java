package com.messaging.network.legacy;

import com.messaging.common.model.BrokerMessage;
import com.messaging.network.legacy.ProtocolDetectionService;
import com.messaging.network.legacy.events.EventType;
import jakarta.inject.Singleton;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Default implementation of ProtocolDetectionService (Phase 11).
 *
 * Detection strategy:
 * - Legacy: First byte is EventType ordinal (0 = REGISTER)
 * - Modern: First byte is MessageType code (0x01-0x0C)
 *
 * Legacy EventType ordinals:
 *   0 = REGISTER
 *   1 = MESSAGE
 *   2 = RESET
 *   3 = READY
 *   4 = ACK
 *   5 = EOF
 *   6 = DELETE
 *   7 = BATCH
 *
 * Modern MessageType codes:
 *   0x01 = DATA
 *   0x02 = ACK
 *   0x03 = SUBSCRIBE
 *   0x04 = COMMIT_OFFSET
 *   0x05 = RESET
 *   0x06 = READY
 *   0x07 = DISCONNECT
 *   0x08 = HEARTBEAT
 *   0x09 = BATCH_HEADER
 *   0x0A = BATCH_ACK
 *   0x0B = RESET_ACK
 *   0x0C = READY_ACK
 */
@Singleton
public class DefaultProtocolDetectionService implements ProtocolDetectionService {
    private static final Logger log = LoggerFactory.getLogger(DefaultProtocolDetectionService.class);

    @Override
    public ProtocolType detectProtocol(byte firstByte) {
        // Legacy protocol: first byte is EventType ordinal (0-7)
        // We expect REGISTER (0) as first message from legacy client
        if (firstByte >= 0 && firstByte <= 7) {
            // Could be legacy REGISTER or ambiguous with modern codes
            // Check if it's a valid EventType
            try {
                EventType.get(firstByte);
                // Valid EventType - likely legacy if it's REGISTER (0)
                if (firstByte == EventType.REGISTER.ordinal()) {
                    return ProtocolType.LEGACY;
                }
                // For other ordinals (1-7), check if they make sense as first message
                // Legacy clients should send REGISTER first, so 1-7 as first byte
                // is more likely to be modern protocol
            } catch (IllegalArgumentException e) {
                // Invalid EventType - fall through to modern check
            }
        }

        // Modern protocol: first byte is MessageType code
        // Check if it matches known MessageType codes
        for (BrokerMessage.MessageType type : BrokerMessage.MessageType.values()) {
            if (type.getCode() == firstByte) {
                return ProtocolType.MODERN;
            }
        }

        // Default to modern if ambiguous
        log.warn("Ambiguous protocol detection (firstByte=0x{}), defaulting to MODERN",
                String.format("%02X", firstByte & 0xFF));
        return ProtocolType.MODERN;
    }
}
