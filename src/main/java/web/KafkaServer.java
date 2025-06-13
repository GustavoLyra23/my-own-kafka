package web;

import core.models.*;
import enums.API_KEYS;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.SocketTimeoutException;
import java.util.ArrayList;
import java.util.List;
import java.util.logging.Level;
import java.util.logging.Logger;

import static core.models.DescribeTopicDTO.createDescribeTopicPartitionsResponse;
import static enums.ERROR.UNSUPPORTED_VERSION;
import static utils.ThreadSocketPoolExecutor.executeParallelTask;
import static utils.ByteNetReader.*;

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

        //noinspection InfiniteLoopStatement
        while (true) {
            try {
                Socket clientSocket = acceptClientConnection();
                InputStream inputStream = clientSocket.getInputStream();
                // Parse request header to determine the API type
                int messageSize = readMsgRequest(inputStream);
                short apiKey = readApiKey(inputStream);
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
     * @param messageSize  the size of the message in bytes
     * @param apiKey       the API key indicating the request type
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
     * @param messageSize  the size of the first message
     * @param apiKey       the API key for the first request
     */
    private void handleApiVersionsRequest(Socket clientSocket, int messageSize, short apiKey) {
        try (clientSocket) {
            clientSocket.setSoTimeout(SOCKET_TIMEOUT_MS);
            OutputStream outputStream = clientSocket.getOutputStream();
            var inputStream = clientSocket.getInputStream();
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
     * @param messageSize  the size of the first message
     * @param apiKey       the API key for the first request
     */
    private void handleDescribeTopicPartitionsRequest(Socket clientSocket, int messageSize, short apiKey) {
        try (clientSocket) {
            clientSocket.setSoTimeout(SOCKET_TIMEOUT_MS);
            OutputStream outputStream = clientSocket.getOutputStream();
            var inputStream = clientSocket.getInputStream();
            String clientAddress = formatClientAddress(clientSocket);
            int requestCount = 0;

            while (!clientSocket.isClosed() && clientSocket.isConnected()) {
                try {
                    requestCount++;
                    LOGGER.info("Processing DescribeTopicPartitions request " + requestCount + " from client: " + clientAddress);

                    //Dealing with the request parsing....
                    var request = parseDescribeTopicPartitionsRequest(inputStream, messageSize, apiKey);
                    var response = createDescribeTopicPartitionsResponse(request);
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
     * @param apiKey      the API key (already read)
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
     * IMPORTANT: This assumes that messageSize and apiKey have already been read from the stream.
     *
     * @param inputStream the input stream to read from
     * @param messageSize the total message size (excluding the 4-byte size field)
     * @param apiKey      the API key (already read)
     * @return the parsed DescribeTopicRequest object
     * @throws IOException if parsing fails
     */
    private DescribeTopicRequest parseDescribeTopicPartitionsRequest(InputStream inputStream, int messageSize, short apiKey) throws IOException {
        try {
            short apiVersion = readInt16(inputStream);
            int correlationId = readInt32(inputStream);

            // Client ID
            short clientIdLength = readInt16(inputStream);
            byte[] clientIdBytes = new byte[clientIdLength];
            inputStream.read(clientIdBytes);

            // Skip tag buffer
            inputStream.read();

            // Topics
            int topicsLength = inputStream.read(); // compact array length
            List<TopicInfo> topics = new ArrayList<>();

            for (int i = 0; i < topicsLength - 1; i++) { // -1 for compact format
                int topicNameLength = inputStream.read(); // compact string length
                byte[] topicNameBytes = new byte[topicNameLength - 1]; // -1 for compact format
                inputStream.read(topicNameBytes);
                inputStream.read(); // skip topic tag buffer

                topics.add(new TopicInfo((byte) topicNameBytes.length, topicNameBytes));
            }

            // Partition limit
            int partitionLimit = readInt32(inputStream);

            // Skip remaining bytes (cursor + final tag)
            while (inputStream.available() > 0) {
                inputStream.read();
            }

            return new DescribeTopicRequest(messageSize, apiKey, apiVersion, correlationId,
                    clientIdLength, clientIdBytes, topics.size(), topics, partitionLimit);

        } catch (Exception e) {
            throw new IOException("Parse failed: " + e.getMessage(), e);
        }
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
     * Sends a response to the client over the output stream.
     *
     * @param outputStream the output stream to write to
     * @param response     the response object to send
     * @throws IOException if sending fails
     */
    private void sendResponse(OutputStream outputStream, IBufferByteDTO response) throws IOException {
        byte[] responseBytes = response.toByteBuffer().array();
        outputStream.write(responseBytes);
        outputStream.flush();
        LOGGER.fine("Sent response: " + responseBytes.length + " bytes");
    }

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
     * @param requestCount  the number of requests processed before timeout
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
     * @param requestCount  the number of requests processed before disconnection
     * @param cause         the exception that caused the disconnection
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

}