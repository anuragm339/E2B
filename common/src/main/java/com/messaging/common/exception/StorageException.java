package com.messaging.common.exception;

/**
 * Exception for storage layer errors (segments, indexes, I/O operations)
 */
public class StorageException extends MessagingException {

    private String topic;
    private int partition;
    private Long offset;
    private String segmentPath;

    public StorageException(ErrorCode errorCode, String message) {
        super(errorCode, message);
    }

    public StorageException(ErrorCode errorCode, String message, Throwable cause) {
        super(errorCode, message, cause);
    }

    public StorageException withTopic(String topic) {
        this.topic = topic;
        withContext("topic", topic);
        return this;
    }

    public StorageException withPartition(int partition) {
        this.partition = partition;
        withContext("partition", partition);
        return this;
    }

    public StorageException withOffset(long offset) {
        this.offset = offset;
        withContext("offset", offset);
        return this;
    }

    public StorageException withSegmentPath(String segmentPath) {
        this.segmentPath = segmentPath;
        withContext("segmentPath", segmentPath);
        return this;
    }

    public String getTopic() {
        return topic;
    }

    public int getPartition() {
        return partition;
    }

    public Long getOffset() {
        return offset;
    }

    public String getSegmentPath() {
        return segmentPath;
    }

    // Factory methods for common storage errors
    public static StorageException ioError(String message, Throwable cause) {
        return new StorageException(ErrorCode.STORAGE_IO_ERROR, message, cause);
    }

    public static StorageException corruption(String message) {
        return new StorageException(ErrorCode.STORAGE_CORRUPTION, message);
    }

    public static StorageException crcMismatch(String message, long offset) {
        return new StorageException(ErrorCode.STORAGE_CRC_MISMATCH, message)
                .withOffset(offset);
    }

    public static StorageException offsetOutOfRange(String topic, int partition, long offset, long minOffset, long maxOffset) {
        StorageException ex = new StorageException(
                ErrorCode.STORAGE_OFFSET_OUT_OF_RANGE,
                String.format("Offset %d out of range [%d, %d] for topic=%s partition=%d",
                        offset, minOffset, maxOffset, topic, partition))
                .withTopic(topic)
                .withPartition(partition)
                .withOffset(offset);
        ex.withContext("minOffset", minOffset);
        ex.withContext("maxOffset", maxOffset);
        return ex;
    }

    public static StorageException segmentNotFound(String segmentPath) {
        return new StorageException(
                ErrorCode.STORAGE_SEGMENT_NOT_FOUND,
                "Segment file not found: " + segmentPath)
                .withSegmentPath(segmentPath);
    }

    public static StorageException writeFailed(String topic, int partition, Throwable cause) {
        return new StorageException(
                ErrorCode.STORAGE_WRITE_FAILED,
                String.format("Failed to write to topic=%s partition=%d", topic, partition),
                cause)
                .withTopic(topic)
                .withPartition(partition);
    }

    public static StorageException readFailed(String topic, int partition, long offset, Throwable cause) {
        return new StorageException(
                ErrorCode.STORAGE_READ_FAILED,
                String.format("Failed to read from topic=%s partition=%d offset=%d", topic, partition, offset),
                cause)
                .withTopic(topic)
                .withPartition(partition)
                .withOffset(offset);
    }
}
