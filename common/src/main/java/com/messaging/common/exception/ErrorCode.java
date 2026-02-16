package com.messaging.common.exception;

/**
 * Comprehensive error codes for the messaging system.
 * Organized by category with structured codes for monitoring and alerting.
 */
public enum ErrorCode {

    // ==================== STORAGE ERRORS (1xxx) ====================
    STORAGE_IO_ERROR(1001, "STORAGE", "I/O error during storage operation", true),
    STORAGE_CORRUPTION(1002, "STORAGE", "Storage data corruption detected", false),
    STORAGE_FULL(1003, "STORAGE", "Storage capacity exceeded", false),
    STORAGE_SEGMENT_NOT_FOUND(1004, "STORAGE", "Segment file not found", false),
    STORAGE_INDEX_CORRUPTION(1005, "STORAGE", "Index file corruption detected", false),
    STORAGE_CRC_MISMATCH(1006, "STORAGE", "CRC32 validation failed", false),
    STORAGE_OFFSET_OUT_OF_RANGE(1007, "STORAGE", "Requested offset out of range", false),
    STORAGE_WRITE_FAILED(1008, "STORAGE", "Failed to write to storage", true),
    STORAGE_READ_FAILED(1009, "STORAGE", "Failed to read from storage", true),
    STORAGE_RECOVERY_FAILED(1010, "STORAGE", "Segment recovery failed", false),
    STORAGE_METADATA_ERROR(1011, "STORAGE", "Metadata store operation failed", true),
    STORAGE_SEGMENT_SEAL_FAILED(1012, "STORAGE", "Failed to seal active segment", true),
    STORAGE_FILE_CHANNEL_ERROR(1013, "STORAGE", "FileChannel operation failed", true),
    STORAGE_MMAP_ERROR(1014, "STORAGE", "Memory-mapped file operation failed", true),

    // ==================== NETWORK ERRORS (2xxx) ====================
    NETWORK_CONNECTION_FAILED(2001, "NETWORK", "Failed to establish connection", true),
    NETWORK_CONNECTION_LOST(2002, "NETWORK", "Connection lost unexpectedly", true),
    NETWORK_SEND_FAILED(2003, "NETWORK", "Failed to send message", true),
    NETWORK_RECEIVE_FAILED(2004, "NETWORK", "Failed to receive message", true),
    NETWORK_TIMEOUT(2005, "NETWORK", "Network operation timed out", true),
    NETWORK_ENCODING_ERROR(2006, "NETWORK", "Message encoding failed", false),
    NETWORK_DECODING_ERROR(2007, "NETWORK", "Message decoding failed", false),
    NETWORK_INVALID_MESSAGE_FORMAT(2008, "NETWORK", "Invalid message format received", false),
    NETWORK_CHANNEL_CLOSED(2009, "NETWORK", "Network channel closed", false),
    NETWORK_BIND_FAILED(2010, "NETWORK", "Failed to bind to port", false),
    NETWORK_PROTOCOL_VIOLATION(2011, "NETWORK", "Network protocol violation detected", false),
    NETWORK_HANDSHAKE_FAILED(2012, "NETWORK", "Connection handshake failed", true),

    // ==================== CONSUMER ERRORS (3xxx) ====================
    CONSUMER_NOT_REGISTERED(3001, "CONSUMER", "Consumer not registered", false),
    CONSUMER_ALREADY_REGISTERED(3002, "CONSUMER", "Consumer already registered", false),
    CONSUMER_DISCONNECTED(3003, "CONSUMER", "Consumer disconnected", true),
    CONSUMER_OFFSET_INVALID(3004, "CONSUMER", "Invalid consumer offset", false),
    CONSUMER_OFFSET_COMMIT_FAILED(3005, "CONSUMER", "Offset commit failed", true),
    CONSUMER_SUBSCRIPTION_FAILED(3006, "CONSUMER", "Consumer subscription failed", true),
    CONSUMER_BATCH_DELIVERY_FAILED(3007, "CONSUMER", "Batch delivery to consumer failed", true),
    CONSUMER_LAG_EXCEEDED(3008, "CONSUMER", "Consumer lag exceeded threshold", false),
    CONSUMER_REBALANCE_FAILED(3009, "CONSUMER", "Consumer rebalance failed", true),
    CONSUMER_HEARTBEAT_TIMEOUT(3010, "CONSUMER", "Consumer heartbeat timeout", false),
    CONSUMER_READY_TIMEOUT(3011, "CONSUMER", "Consumer READY acknowledgment timeout", false),
    CONSUMER_RESET_FAILED(3012, "CONSUMER", "Consumer data reset failed", true),

    // ==================== DATA REFRESH ERRORS (4xxx) ====================
    DATA_REFRESH_ALREADY_IN_PROGRESS(4001, "DATA_REFRESH", "Data refresh already in progress", false),
    DATA_REFRESH_TIMEOUT(4002, "DATA_REFRESH", "Data refresh operation timed out", false),
    DATA_REFRESH_CONSUMER_NOT_READY(4003, "DATA_REFRESH", "Consumer not ready for data refresh", false),
    DATA_REFRESH_STATE_INVALID(4004, "DATA_REFRESH", "Invalid data refresh state transition", false),
    DATA_REFRESH_REPLAY_FAILED(4005, "DATA_REFRESH", "Message replay failed during data refresh", true),
    DATA_REFRESH_METRICS_ERROR(4006, "DATA_REFRESH", "Data refresh metrics recording failed", true),
    DATA_REFRESH_COMPLETION_FAILED(4007, "DATA_REFRESH", "Data refresh completion failed", true),

    // ==================== BROKER ERRORS (5xxx) ====================
    BROKER_NOT_INITIALIZED(5001, "BROKER", "Broker not initialized", false),
    BROKER_ALREADY_RUNNING(5002, "BROKER", "Broker already running", false),
    BROKER_SHUTDOWN_FAILED(5003, "BROKER", "Broker shutdown failed", false),
    BROKER_MESSAGE_PROCESSING_FAILED(5004, "BROKER", "Message processing failed", true),
    BROKER_TOPIC_NOT_FOUND(5005, "BROKER", "Topic not found", false),
    BROKER_PARTITION_NOT_FOUND(5006, "BROKER", "Partition not found", false),
    BROKER_INVALID_CONFIGURATION(5007, "BROKER", "Invalid broker configuration", false),
    BROKER_RESOURCE_EXHAUSTED(5008, "BROKER", "Broker resources exhausted", false),

    // ==================== REGISTRY ERRORS (6xxx) ====================
    REGISTRY_CONNECTION_FAILED(6001, "REGISTRY", "Failed to connect to cloud registry", true),
    REGISTRY_TOPOLOGY_FETCH_FAILED(6002, "REGISTRY", "Failed to fetch topology from registry", true),
    REGISTRY_REGISTRATION_FAILED(6003, "REGISTRY", "Broker registration failed", true),
    REGISTRY_HEARTBEAT_FAILED(6004, "REGISTRY", "Registry heartbeat failed", true),
    REGISTRY_INVALID_RESPONSE(6005, "REGISTRY", "Invalid response from registry", true),

    // ==================== PIPE ERRORS (7xxx) ====================
    PIPE_CONNECTION_FAILED(7001, "PIPE", "Failed to connect to parent broker", true),
    PIPE_POLL_FAILED(7002, "PIPE", "Pipe polling failed", true),
    PIPE_MESSAGE_FORWARD_FAILED(7003, "PIPE", "Failed to forward message from pipe", true),
    PIPE_INVALID_MESSAGE(7004, "PIPE", "Invalid message received from pipe", false),

    // ==================== VALIDATION ERRORS (8xxx) ====================
    VALIDATION_INVALID_TOPIC(8001, "VALIDATION", "Invalid topic name", false),
    VALIDATION_INVALID_PARTITION(8002, "VALIDATION", "Invalid partition number", false),
    VALIDATION_INVALID_OFFSET(8003, "VALIDATION", "Invalid offset value", false),
    VALIDATION_INVALID_MESSAGE_KEY(8004, "VALIDATION", "Invalid message key", false),
    VALIDATION_INVALID_MESSAGE_DATA(8005, "VALIDATION", "Invalid message data", false),
    VALIDATION_MESSAGE_TOO_LARGE(8006, "VALIDATION", "Message size exceeds limit", false),
    VALIDATION_INVALID_CONSUMER_GROUP(8007, "VALIDATION", "Invalid consumer group name", false),

    // ==================== CONCURRENCY ERRORS (9xxx) ====================
    CONCURRENCY_LOCK_TIMEOUT(9001, "CONCURRENCY", "Failed to acquire lock within timeout", true),
    CONCURRENCY_DEADLOCK_DETECTED(9002, "CONCURRENCY", "Deadlock detected", false),
    CONCURRENCY_THREAD_INTERRUPTED(9003, "CONCURRENCY", "Thread interrupted", false),
    CONCURRENCY_RACE_CONDITION(9004, "CONCURRENCY", "Race condition detected", true),

    // ==================== UNKNOWN/GENERIC ERRORS (9999) ====================
    UNKNOWN_ERROR(9999, "UNKNOWN", "Unknown error occurred", true);

    private final int code;
    private final String category;
    private final String description;
    private final boolean retriable;

    ErrorCode(int code, String category, String description, boolean retriable) {
        this.code = code;
        this.category = category;
        this.description = description;
        this.retriable = retriable;
    }

    public int getCode() {
        return code;
    }

    public String getCategory() {
        return category;
    }

    public String getDescription() {
        return description;
    }

    public boolean isRetriable() {
        return retriable;
    }

    /**
     * Get ErrorCode by numeric code
     */
    public static ErrorCode fromCode(int code) {
        for (ErrorCode errorCode : values()) {
            if (errorCode.code == code) {
                return errorCode;
            }
        }
        return UNKNOWN_ERROR;
    }

    /**
     * Check if code belongs to a specific category
     */
    public boolean isCategory(String category) {
        return this.category.equalsIgnoreCase(category);
    }
}
