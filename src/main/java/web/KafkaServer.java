package web;

import core.models.*;
import enums.API_KEYS;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.SocketTimeoutException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.logging.Level;
import java.util.logging.Logger;

import static enums.API_KEYS.*;
import static enums.ERROR.UNKNOWN_TOPIC_OR_PARTITION;
import static enums.ERROR.UNSUPPORTED_VERSION;
import static web.ThreadSocketPoolExecutor.executeParallelTask;
import static web.util.RequestReaderUtil.*;

/**
 * Kafka server implementation that handles client connections and processes Kafka protocol requests.
 *
 * <p>This server supports the following Kafka API operations:</p>
 * <ul>
 *   <li>ApiVersions - Returns supported API versions to clients</li>
 *   <li>DescribeTopicPartitions - Provides topic partition metadata</li>
 * </ul>
 *
 * <p>The server uses a thread pool to handle multiple concurrent client connections
 * and implements the Kafka wire protocol for request/response communication.</p>
 *
 * <p>Usage example:</p>
 * <pre>{@code
 * KafkaServer server = new KafkaServer(9092);
 * server.start(); // Starts listening on port 9092
 * }</pre>
 *
 * @author Kafka Server Team
 * @version 1.0
 * @since 1.0
 */
public class KafkaServer {

    private static final Logger LOGGER = Logger.getLogger(KafkaServer.class.getName());

    // Server configuration constants
    private static final int MIN_API_VERSION = 0;
    private static final int MAX_API_VERSION = 4;
    private static final int SOCKET_TIMEOUT_MS = 10000; // 10 seconds
    private static final int TAG_BUFFER_SIZE = 1;
    private static final int CORRELATION_ID_SIZE = 8; // 8 bytes already read (api key + version + correlation id)

    // Server state
    private final int port;
    private ServerSocket serverSocket;

    /**
     * Constructs a new KafkaServer instance that will listen on the specified port.
     *
     * @param port the port number on which the server will listen for client connections
     * @throws IllegalArgumentException if port is not in valid range (1-65535)
     */
    public KafkaServer(int port) {
        if (port < 1 || port > 65535) {
            throw new IllegalArgumentException("Port must be between 1 and 65535, got: " + port);
        }
        this.port = port;
    }

    /**
     * Starts the Kafka server and begins accepting client connections.
     *
     * <p>This method initializes the server socket and enters the main server loop
     * where it will continuously accept and process client connections until
     * an unrecoverable error occurs.</p>
     *
     * <p>Note: This method blocks the calling thread.</p>
     *
     * @throws RuntimeException if the server fails to start
     */
    public synchronized void start() {
        try {
            initializeServer();
            runServerLoop();
        } catch (IOException e) {
            LOGGER.log(Level.SEVERE, "Failed to start Kafka server on port " + port, e);
            throw new RuntimeException("Server startup failed", e);
        }
    }

    /**
     * Initializes the server socket and binds it to the configured port.
     *
     * @throws IOException if the server socket cannot be created or bound to the port
     */
    private void initializeServer() throws IOException {
        this.serverSocket = new ServerSocket(port);
        serverSocket.setReuseAddress(true);
        LOGGER.info("Kafka server started successfully on port " + port);
    }

    /**
     * Main server loop that accepts client connections and dispatches them to appropriate handlers.
     *
     * <p>For each incoming connection, this method:</p>
     * <ol>
     *   <li>Reads the message size and API key from the request header</li>
     *   <li>Determines the request type based on the API key</li>
     *   <li>Dispatches the request to the appropriate handler in a separate thread</li>
     * </ol>
     */
    private void runServerLoop() {
        LOGGER.info("Entering main server loop, ready to accept client connections");

        while (true) {
            try {
                Socket clientSocket = acceptClientConnection();
                InputStream inputStream = clientSocket.getInputStream();

                // Parse request header to determine the API type
                int messageSize = readMsgRequest(inputStream);
                short apiKey = readApiKey(inputStream, false);

                dispatchRequest(clientSocket, messageSize, apiKey);

            } catch (Exception e) {
                LOGGER.log(Level.WARNING, "Error handling client request", e);
            }
        }
    }

    /**
     * Accepts a new client connection from the server socket.
     *
     * @return the accepted client socket
     * @throws IOException if the connection cannot be accepted
     */
    private Socket acceptClientConnection() throws IOException {
        Socket clientSocket = serverSocket.accept();
        String clientAddress = formatClientAddress(clientSocket);
        LOGGER.info("Client connected: " + clientAddress);
        return clientSocket;
    }

    /**
     * Dispatches a request to the appropriate handler based on the API key.
     *
     * @param clientSocket the client socket for the connection
     * @param messageSize the size of the message in bytes
     * @param apiKey the API key indicating the request type
     */
    private void dispatchRequest(Socket clientSocket, int messageSize, short apiKey) {
        String clientAddress = formatClientAddress(clientSocket);

        switch (API_KEYS.apiKeyFromInt(apiKey)) {
            case API_VERSIONS:
                LOGGER.info("Received ApiVersions request from client: " + clientAddress);
                executeParallelTask(client -> handleApiVersionsRequest(client, messageSize, apiKey), clientSocket);
                break;

            case DESCRIBE_TOPIC_PARTITION:
                LOGGER.info("Received DescribeTopicPartitions request from client: " + clientAddress);
                executeParallelTask(client -> handleDescribeTopicPartitionsRequest(client, messageSize, apiKey), clientSocket);
                break;

            default:
                LOGGER.warning("Unsupported API key " + apiKey + " from client: " + clientAddress);
                closeSocketSafely(clientSocket);
        }
    }

    /**
     * Handles ApiVersions requests from clients.
     *
     * <p>This method processes one or more ApiVersions requests from a single client connection,
     * returning the supported API versions and their capabilities.</p>
     *
     * @param clientSocket the client socket
     * @param messageSize the size of the first message
     * @param apiKey the API key for the first request
     */
    private void handleApiVersionsRequest(Socket clientSocket, int messageSize, short apiKey) {
        try (clientSocket) {
            clientSocket.setSoTimeout(SOCKET_TIMEOUT_MS);

            InputStream inputStream = clientSocket.getInputStream();
            OutputStream outputStream = clientSocket.getOutputStream();
            String clientAddress = formatClientAddress(clientSocket);

            int requestCount = 0;

            while (!clientSocket.isClosed() && clientSocket.isConnected()) {
                try {
                    requestCount++;
                    LOGGER.info("Processing ApiVersions request " + requestCount + " from client: " + clientAddress);

                    KafkaRequest request = parseApiVersionsRequest(inputStream, messageSize, apiKey);
                    ApiVersionResponseDTO response = createApiVersionsResponse(request);
                    sendResponse(outputStream, response);

                    LOGGER.info("ApiVersions request " + requestCount + " completed for correlation ID: " + request.correlationId());

                } catch (SocketTimeoutException e) {
                    handleClientTimeout(clientAddress, requestCount);
                    break;
                } catch (IOException e) {
                    handleClientDisconnection(clientAddress, requestCount, e);
                    break;
                }
            }
        } catch (Exception e) {
            LOGGER.log(Level.WARNING, "Error processing ApiVersions request", e);
        }
    }

    /**
     * Handles DescribeTopicPartitions requests from clients.
     *
     * <p>This method processes requests for topic partition metadata,
     * returning information about topic partitions, leaders, and replicas.</p>
     *
     * @param clientSocket the client socket
     * @param messageSize the size of the first message
     * @param apiKey the API key for the first request
     */
    private void handleDescribeTopicPartitionsRequest(Socket clientSocket, int messageSize, short apiKey) {
        try (clientSocket) {
            clientSocket.setSoTimeout(SOCKET_TIMEOUT_MS);

            InputStream inputStream = clientSocket.getInputStream();
            OutputStream outputStream = clientSocket.getOutputStream();
            String clientAddress = formatClientAddress(clientSocket);

            int requestCount = 0;

            while (!clientSocket.isClosed() && clientSocket.isConnected()) {
                try {
                    requestCount++;
                    LOGGER.info("Processing DescribeTopicPartitions request " + requestCount + " from client: " + clientAddress);

                    DescribeTopicRequest request = parseDescribeTopicPartitionsRequest(inputStream, messageSize, apiKey);
                    DescribeTopicDTO response = createDescribeTopicPartitionsResponse(request);
                    sendResponse(outputStream, response);

                    LOGGER.info("DescribeTopicPartitions request " + requestCount + " completed for correlation ID: " + request.correlationId());

                } catch (Exception e) {
                    handleClientDisconnection(clientAddress, requestCount, e);
                    break;
                }
            }
        } catch (Exception e) {
            LOGGER.log(Level.WARNING, "Error processing DescribeTopicPartitions request", e);
        }
    }

    /**
     * Parses an ApiVersions request from the input stream.
     *
     * @param inputStream the input stream to read from
     * @param messageSize the total message size
     * @param apiKey the API key (already read)
     * @return the parsed KafkaRequest object
     * @throws IOException if parsing fails
     */
    private KafkaRequest parseApiVersionsRequest(InputStream inputStream, int messageSize, short apiKey) throws IOException {
        short apiVersion = readApiVersion(inputStream);
        int correlationId = readCorrelationId(inputStream);

        // Skip any remaining request data
        int remainingBytes = messageSize - CORRELATION_ID_SIZE;
        if (remainingBytes > 0) {
            long skippedBytes = inputStream.skip(remainingBytes);
            LOGGER.fine("Skipped " + skippedBytes + " bytes of remaining request data");
        }

        return new KafkaRequest(messageSize, apiKey, apiVersion, correlationId);
    }

    /**
     * Parses a DescribeTopicPartitions request from the input stream.
     *
     * @param inputStream the input stream to read from
     * @param messageSize the total message size
     * @param apiKey the API key (already read)
     * @return the parsed DescribeTopicRequest object
     * @throws IOException if parsing fails
     */
    private DescribeTopicRequest parseDescribeTopicPartitionsRequest(InputStream inputStream, int messageSize, short apiKey) throws IOException {
        LOGGER.fine("Parsing DescribeTopicPartitions request");

        // Parse request header
        short apiVersion = readApiVersion(inputStream);
        LOGGER.fine("API version: " + apiVersion);

        int correlationId = readCorrelationId(inputStream);
        LOGGER.fine("Correlation ID: " + correlationId);

        // Parse client information
        short clientIdLength = readClientIdLenght(inputStream);
        LOGGER.fine("Client ID length: " + clientIdLength);

        byte[] clientIdContents = readExactly(inputStream, clientIdLength);
        String clientId = new String(clientIdContents, StandardCharsets.UTF_8);
        LOGGER.fine("Client ID: '" + clientId + "'");

        // Skip tag buffer
        skipBytes(inputStream, TAG_BUFFER_SIZE);

        // Parse topics array
        List<TopicInfo> topics = parseTopicsArray(inputStream);

        // Parse request footer
        int responsePartitionLimit = readDescribeTopicPartitionLimit(inputStream);
        LOGGER.fine("Partition limit: " + responsePartitionLimit);

        byte[] cursor = readCompactBytes(inputStream);
        LOGGER.fine("Cursor: " + (cursor != null ? cursor.length + " bytes" : "null"));

        // Skip final tag buffer
        skipBytes(inputStream, TAG_BUFFER_SIZE);

        LOGGER.info("Successfully parsed DescribeTopicPartitions request with " + topics.size() + " topics");

        return new DescribeTopicRequest(messageSize, apiKey, apiVersion, correlationId,
                clientIdLength, clientIdContents, topics.size(), topics, responsePartitionLimit);
    }

    /**
     * Parses the topics array from the request stream.
     *
     * @param inputStream the input stream to read from
     * @return list of TopicInfo objects
     * @throws IOException if parsing fails
     */
    private List<TopicInfo> parseTopicsArray(InputStream inputStream) throws IOException {
        int arrayLength = readVarint(inputStream);
        LOGGER.fine("Topics array length: " + arrayLength);

        if (arrayLength == 0) {
            throw new IOException("Invalid topics array length: 0");
        }

        int actualArrayLength = arrayLength - 1; // Compact array format subtracts 1
        List<TopicInfo> topics = new ArrayList<>(actualArrayLength);

        for (int i = 0; i < actualArrayLength; i++) {
            TopicInfo topic = parseSingleTopic(inputStream, i);
            topics.add(topic);
        }

        return topics;
    }

    /**
     * Parses a single topic from the topics array.
     *
     * @param inputStream the input stream to read from
     * @param topicIndex the index of this topic (for error reporting)
     * @return TopicInfo object containing topic data
     * @throws IOException if parsing fails or topic name is invalid
     */
    private TopicInfo parseSingleTopic(InputStream inputStream, int topicIndex) throws IOException {
        String topicName = readCompactString(inputStream);
        LOGGER.fine("Topic " + topicIndex + " name: '" + topicName + "'");

        // Skip topic tag buffer
        skipBytes(inputStream, TAG_BUFFER_SIZE);

        if (topicName == null || topicName.isEmpty()) {
            throw new IOException("Invalid topic name at index " + topicIndex);
        }

        byte[] topicNameBytes = topicName.getBytes(StandardCharsets.UTF_8);
        return new TopicInfo((byte) topicNameBytes.length, topicNameBytes);
    }

    /**
     * Creates a response for ApiVersions requests.
     *
     * @param request the original request
     * @return ApiVersionResponseDTO containing supported versions
     */
    private ApiVersionResponseDTO createApiVersionsResponse(KafkaRequest request) {
        short errorCode = validateApiVersion(request.apiVersion());
        LOGGER.fine("Creating ApiVersions response with error code: " + errorCode);
        return new ApiVersionResponseDTO(request.correlationId(), errorCode);
    }

    /**
     * Creates a response for DescribeTopicPartitions requests.
     *
     * <p>Currently returns UNKNOWN_TOPIC_OR_PARTITION error for all topics
     * as topic management is not yet implemented.</p>
     *
     * @param request the original request
     * @return DescribeTopicDTO containing topic information
     */
    private DescribeTopicDTO createDescribeTopicPartitionsResponse(DescribeTopicRequest request) {
        // TODO: Implement logic to verify if topic exists and return appropriate error code
        final byte[] TOPIC_ID = "00000000-0000-0000-0000-000000000000".getBytes();

        DescribeTopicDTO response = new DescribeTopicDTO(request.correlationId());

        for (TopicInfo topic : request.topics()) {
            TopicResponseDTO topicResponse = new TopicResponseDTO(
                    TOPIC_ID,
                    UNKNOWN_TOPIC_OR_PARTITION.getCode(),
                    topic.topicNameLength(),
                    topic.topicName(),
                    (byte) 0
            );
            response.addTopicResponse(topicResponse);
        }

        LOGGER.fine("Created DescribeTopicPartitions response for " + request.topics().size() + " topics");
        return response;
    }

    /**
     * Sends a response to the client over the output stream.
     *
     * @param outputStream the output stream to write to
     * @param response the response object to send
     * @throws IOException if sending fails
     */
    private void sendResponse(OutputStream outputStream, IBufferByteDTO response) throws IOException {
        byte[] responseBytes = response.toByteBuffer().array();

        outputStream.write(responseBytes);
        outputStream.flush();

        LOGGER.fine("Sent response: " + responseBytes.length + " bytes");

        if (LOGGER.isLoggable(Level.FINEST)) {
            logResponseHex(responseBytes);
        }
    }

    /**
     * Logs the hexadecimal representation of response bytes for debugging.
     *
     * @param responseBytes the response byte array to log
     */
    private void logResponseHex(byte[] responseBytes) {
        StringBuilder hexLog = new StringBuilder("Response bytes (first 16): ");
        int bytesToLog = Math.min(16, responseBytes.length);

        for (int i = 0; i < bytesToLog; i++) {
            hexLog.append(String.format("%02x ", responseBytes[i]));
        }

        LOGGER.finest(hexLog.toString());
    }

    // ===== UTILITY METHODS =====

    /**
     * Validates if the given API version is supported by this server.
     *
     * @param apiVersion the API version to validate
     * @return 0 if valid, UNSUPPORTED_VERSION error code if invalid
     */
    private short validateApiVersion(short apiVersion) {
        boolean isValid = apiVersion >= MIN_API_VERSION && apiVersion <= MAX_API_VERSION;
        return isValid ? (short) 0 : UNSUPPORTED_VERSION.getCode();
    }

    /**
     * Checks if an API version is within the supported range.
     *
     * @param apiVersion the API version to check
     * @return true if the version is supported, false otherwise
     */
    private boolean isValidApiVersion(short apiVersion) {
        return apiVersion >= MIN_API_VERSION && apiVersion <= MAX_API_VERSION;
    }

    /**
     * Safely reads exactly the specified number of bytes from the input stream.
     *
     * <p>This method ensures that all requested bytes are read, handling cases
     * where the underlying stream returns fewer bytes than requested.</p>
     *
     * @param inputStream the input stream to read from
     * @param length the exact number of bytes to read
     * @return byte array containing the read data
     * @throws IOException if EOF is encountered before all bytes are read
     * @throws IllegalArgumentException if length is negative
     */
    private byte[] readExactly(InputStream inputStream, int length) throws IOException {
        if (length < 0) {
            throw new IllegalArgumentException("Length cannot be negative: " + length);
        }

        byte[] buffer = new byte[length];
        int totalRead = 0;

        while (totalRead < length) {
            int bytesRead = inputStream.read(buffer, totalRead, length - totalRead);
            if (bytesRead == -1) {
                throw new IOException("EOF while reading " + length + " bytes (got " + totalRead + ")");
            }
            totalRead += bytesRead;
        }

        return buffer;
    }

    /**
     * Safely skips the specified number of bytes from the input stream.
     *
     * @param inputStream the input stream to skip from
     * @param bytesToSkip the number of bytes to skip
     * @throws IOException if skipping fails or EOF is encountered
     */
    private void skipBytes(InputStream inputStream, int bytesToSkip) throws IOException {
        long skippedBytes = inputStream.skip(bytesToSkip);
        if (skippedBytes != bytesToSkip) {
            throw new IOException("Could not skip " + bytesToSkip + " bytes, only skipped " + skippedBytes);
        }
    }

    /**
     * Formats a client socket address for logging purposes.
     *
     * @param socket the client socket
     * @return formatted string containing IP address and port
     */
    private String formatClientAddress(Socket socket) {
        return socket.getInetAddress() + ":" + socket.getPort();
    }

    /**
     * Handles client timeout scenarios gracefully.
     *
     * @param clientAddress the client address for logging
     * @param requestCount the number of requests processed before timeout
     */
    private void handleClientTimeout(String clientAddress, int requestCount) {
        if (requestCount > 0) {
            LOGGER.info("Client finished sending requests (processed " + requestCount + " requests): " + clientAddress);
        } else {
            LOGGER.info("No data received from client (timeout): " + clientAddress);
        }
    }

    /**
     * Handles client disconnection scenarios.
     *
     * @param clientAddress the client address for logging
     * @param requestCount the number of requests processed before disconnection
     * @param cause the exception that caused the disconnection
     */
    private void handleClientDisconnection(String clientAddress, int requestCount, Exception cause) {
        LOGGER.info("Client disconnected after " + requestCount + " requests: " + clientAddress + " - " + cause.getMessage());
    }

    /**
     * Safely closes a client socket, logging any errors that occur.
     *
     * @param clientSocket the socket to close
     */
    private void closeSocketSafely(Socket clientSocket) {
        try {
            if (clientSocket != null && !clientSocket.isClosed()) {
                clientSocket.close();
            }
        } catch (IOException e) {
            LOGGER.log(Level.WARNING, "Error closing client socket", e);
        }
    }

    // ===== LEGACY METHODS (kept for compatibility) =====

    /**
     * Legacy method for handling client connections.
     *
     * @deprecated This method name is inconsistent. Use {@link #handleApiVersionsRequest(Socket, int, short)} instead.
     */
    @Deprecated
    private void handleClientConnection(Socket clientSocket, int messageSize, short apiKey) {
        handleApiVersionsRequest(clientSocket, messageSize, apiKey);
    }

    /**
     * Legacy method for handling describe topic partition requests.
     *
     * @deprecated This method name is inconsistent. Use {@link #handleDescribeTopicPartitionsRequest(Socket, int, short)} instead.
     */
    @Deprecated
    private void handleDescribeTopicPartition(Socket clientSocket, int messageSize, short apiKey) {
        handleDescribeTopicPartitionsRequest(clientSocket, messageSize, apiKey);
    }

    /**
     * Legacy method for reading describe topic partition requests.
     *
     * @deprecated Use {@link #parseDescribeTopicPartitionsRequest(InputStream, int, short)} instead.
     */
    @Deprecated
    private DescribeTopicRequest readDescribeTopicPartitionRequest(InputStream inputStream, int messageSize, short apiKey) throws IOException {
        return parseDescribeTopicPartitionsRequest(inputStream, messageSize, apiKey);
    }

    /**
     * Legacy method for reading API version requests.
     *
     * @deprecated Use {@link #parseApiVersionsRequest(InputStream, int, short)} instead.
     */
    @Deprecated
    private KafkaRequest readApiVersionRequest(InputStream inputStream, int messageSize, short apiKey) throws IOException {
        return parseApiVersionsRequest(inputStream, messageSize, apiKey);
    }

    /**
     * Legacy method for creating API version responses.
     *
     * @deprecated Use {@link #createApiVersionsResponse(KafkaRequest)} instead.
     */
    @Deprecated
    private ApiVersionResponseDTO createApiVersionResponse(KafkaRequest request) {
        return createApiVersionsResponse(request);
    }

    /**
     * Legacy method for creating describe topic partition responses.
     *
     * @deprecated Use {@link #createDescribeTopicPartitionsResponse(DescribeTopicRequest)} instead.
     */
    @Deprecated
    private DescribeTopicDTO createDescribeTopicPartitionResponse(DescribeTopicRequest request) {
        return createDescribeTopicPartitionsResponse(request);
    }

    /**
     * Unused legacy method for reading API key from request.
     * This method is not used in the current implementation.
     *
     * @deprecated This method is not used and may be removed in future versions.
     */
    @Deprecated
    @SuppressWarnings("unused")
    private API_KEYS readApiKeyFromRequest(InputStream inputStream) throws IOException {
        LOGGER.warning("readApiKeyFromRequest is deprecated and should not be used");
        throw new UnsupportedOperationException("This method is deprecated and not supported");
    }
}