package com.messaging.common.model;

import com.messaging.common.exception.ErrorCode;
import com.messaging.common.exception.NetworkException;

/**
 * Message exchanged between broker components via network
 */
public class BrokerMessage {
    private MessageType type;
    private long messageId;
    private byte[] payload;

    public BrokerMessage() {
    }

    public BrokerMessage(MessageType type, long messageId, byte[] payload) {
        this.type = type;
        this.messageId = messageId;
        this.payload = payload;
    }

    public MessageType getType() {
        return type;
    }

    public void setType(MessageType type) {
        this.type = type;
    }

    public long getMessageId() {
        return messageId;
    }

    public void setMessageId(long messageId) {
        this.messageId = messageId;
    }

    public byte[] getPayload() {
        return payload;
    }

    public void setPayload(byte[] payload) {
        this.payload = payload;
    }

    /**
     * Message types for broker protocol
     */
    public enum MessageType {
        DATA((byte) 0x01),
        ACK((byte) 0x02),
        SUBSCRIBE((byte) 0x03),
        COMMIT_OFFSET((byte) 0x04),
        RESET((byte) 0x05),       // Data refresh: clear all local data
        READY((byte) 0x06),       // Data refresh complete: all data sent
        DISCONNECT((byte) 0x07),
        HEARTBEAT((byte) 0x08),
        BATCH_HEADER((byte) 0x09), // Zero-copy batch header (recordCount, totalBytes, lastOffset, topic)
        BATCH_ACK((byte) 0x0A),    // Consumer acknowledgment of batch receipt
        RESET_ACK((byte) 0x0B),    // Consumer acknowledgment of RESET message
        READY_ACK((byte) 0x0C);    // Consumer acknowledgment of READY message

        private final byte code;

        MessageType(byte code) {
            this.code = code;
        }

        public byte getCode() {
            return code;
        }

        public static MessageType fromCode(byte code) {
            for (MessageType type : values()) {
                if (type.code == code) {
                    return type;
                }
            }
            NetworkException ex = new NetworkException(ErrorCode.NETWORK_DECODING_ERROR,
                "Unknown message type code: " + code);
            ex.withContext("messageTypeCode", code);
            // Wrap in RuntimeException since enum methods can't declare checked exceptions
            throw new RuntimeException("Invalid message type code: " + code, ex);
        }
    }
}
