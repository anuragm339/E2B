package com.messaging.common.exception;

/**
 * Exception for network layer errors (connection, encoding, protocol)
 */
public class NetworkException extends MessagingException {

    private String remoteAddress;
    private Integer remotePort;

    public NetworkException(ErrorCode errorCode, String message) {
        super(errorCode, message);
    }

    public NetworkException(ErrorCode errorCode, String message, Throwable cause) {
        super(errorCode, message, cause);
    }

    public NetworkException withRemoteAddress(String address, int port) {
        this.remoteAddress = address;
        this.remotePort = port;
        withContext("remoteAddress", address);
        withContext("remotePort", port);
        return this;
    }

    public static NetworkException connectionFailed(String address, int port, Throwable cause) {
        return new NetworkException(
                ErrorCode.NETWORK_CONNECTION_FAILED,
                String.format("Failed to connect to %s:%d", address, port),
                cause)
                .withRemoteAddress(address, port);
    }

    public static NetworkException sendFailed(String address, int port, Throwable cause) {
        return new NetworkException(
                ErrorCode.NETWORK_SEND_FAILED,
                String.format("Failed to send message to %s:%d", address, port),
                cause)
                .withRemoteAddress(address, port);
    }

    public static NetworkException encodingError(String messageType, Throwable cause) {
        NetworkException ex = new NetworkException(
                ErrorCode.NETWORK_ENCODING_ERROR,
                "Failed to encode message: " + messageType,
                cause);
        ex.withContext("messageType", messageType);
        return ex;
    }

    public static NetworkException decodingError(String details, Throwable cause) {
        return new NetworkException(
                ErrorCode.NETWORK_DECODING_ERROR,
                "Failed to decode message: " + details,
                cause);
    }
}
