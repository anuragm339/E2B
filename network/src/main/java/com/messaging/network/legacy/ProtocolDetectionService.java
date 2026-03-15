package com.messaging.network.legacy;

/**
 * Service for detecting network protocol type (Phase 11).
 *
 * Responsibilities:
 * - Detect legacy vs modern protocol from first byte
 * - Provide protocol metadata for logging/metrics
 */
public interface ProtocolDetectionService {
    /**
     * Detect protocol type from first byte of message.
     *
     * @param firstByte First byte of incoming message
     * @return Detected protocol type
     */
    ProtocolType detectProtocol(byte firstByte);

    /**
     * Protocol type enum.
     */
    enum ProtocolType {
        LEGACY,  // Legacy Event protocol (EventType ordinals 0-7)
        MODERN   // Modern BrokerMessage protocol (MessageType codes 0x01-0x0C)
    }
}
