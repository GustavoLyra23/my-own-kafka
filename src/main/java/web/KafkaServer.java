package web;

import core.models.*;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.SocketTimeoutException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.logging.Level;
import java.util.logging.Logger;

import static enums.API_KEYS.apiKeyFromInt;
import static enums.ERROR.UNKNOWN_TOPIC_OR_PARTITION;
import static enums.ERROR.UNSUPPORTED_VERSION;
import static web.ThreadSocketPoolExecutor.executeParallelTask;
import static web.util.RequestReaderUtil.*;

public class KafkaServer {
    private static final Logger LOGGER = Logger.getLogger(KafkaServer.class.getName());
    private static final int MIN_API_VERSION = 0;
    private static final int MAX_API_VERSION = 4;
    private final int port;
    private ServerSocket serverSocket;
    private static final int SOCKET_TIMEOUT = 10000; // 10 seconds...

    public KafkaServer(int port) {
        this.port = port;
    }

    public synchronized void start() {
        try {
            initializeServer();
            runServerLoop();
        } catch (IOException e) {
            LOGGER.log(Level.SEVERE, "Failed to start server", e);
        }
    }

    private void initializeServer() throws IOException {
        this.serverSocket = new ServerSocket(port);
        serverSocket.setReuseAddress(true);
        LOGGER.info("Kafka server started on port " + port);
    }

    //main server func...
    private void runServerLoop() {
        while (true) {
            try {
                Socket clientSocket = acceptClientConnection();
                var apiKey = readApiKey(clientSocket.getInputStream(), true);
                switch (apiKeyFromInt(apiKey)) {
                    case API_VERSIONS:
                        LOGGER.info("Received ApiVersions request from client: " + formatClientAddress(clientSocket));
                        executeParallelTask(this::handleClientConnection, clientSocket);
                        break;
                    case DESCRIBE_TOPIC_PARTITION:
                        LOGGER.info("Received DescribeTopicPartitions request from client: " + formatClientAddress(clientSocket));
                        executeParallelTask(this::handleDescribeTopicPartition, clientSocket);
                        break;
                    default:
                        LOGGER.warning("Unsupported API Key: " + apiKey + " from client: ");
                }
            } catch (Exception e) {
                LOGGER.log(Level.WARNING, "Error handling client request", e);
            }
        }
    }

    private Socket acceptClientConnection() throws IOException {
        Socket clientSocket = serverSocket.accept();
        LOGGER.info("Client connected: " + formatClientAddress(clientSocket));
        return clientSocket;
    }

    private String formatClientAddress(Socket socket) {
        return socket.getInetAddress() + ":" + socket.getPort();
    }

    private void handleClientConnection(Socket clientSocket) {
        try (clientSocket) {
            clientSocket.setSoTimeout(SOCKET_TIMEOUT);

            InputStream inputStream = clientSocket.getInputStream();
            OutputStream outputStream = clientSocket.getOutputStream();

            int requestCount = 0;

            while (!clientSocket.isClosed() && clientSocket.isConnected()) {
                try {
                    LOGGER.info("Waiting for request " + (requestCount + 1) + " from client: " + formatClientAddress(clientSocket));

                    KafkaRequest request = readApiVersionRequest(inputStream);
                    requestCount++;

                    LOGGER.info("Received request " + requestCount + " - API Key: " + request.apiKey() +
                            ", API Version: " + request.apiVersion() +
                            ", Correlation ID: " + request.correlationId());
                    ApiVersionResponseDTO response = createApiVersionResponse(request);
                    sendResponse(outputStream, response);
                    LOGGER.info("Request " + requestCount + " processed successfully for correlation ID: " + request.correlationId());
                } catch (SocketTimeoutException e) {
                    if (requestCount > 0) {
                        LOGGER.info("Client finished sending requests (processed " + requestCount + " requests): " + formatClientAddress(clientSocket));
                    } else {
                        LOGGER.info("No data received from client (timeout): " + formatClientAddress(clientSocket));
                    }
                    break;

                } catch (IOException e) {
                    LOGGER.info("Client disconnected after " + requestCount + " requests: " + formatClientAddress(clientSocket) + " - " + e.getMessage());
                    break;
                }
            }
        } catch (Exception e) {
            LOGGER.log(Level.WARNING, "Error processing client connection", e);
        }
    }

    private boolean isValidApiVersion(short apiVersion) {
        return apiVersion >= MIN_API_VERSION && apiVersion <= MAX_API_VERSION;
    }

    private DescribeTopicRequest readDescribeTopicPartitionRequest(InputStream inputStream) throws IOException {
        LOGGER.info("Reading DescribeTopicPartition request...");

        // Headers
        int msgSize = readMsgRequest(inputStream);
        LOGGER.info("✓ Message size: " + msgSize);

        var apiKey = readApiKey(inputStream, false);
        LOGGER.info("✓ API key: " + apiKey);

        var apiVersion = readApiVersion(inputStream);
        LOGGER.info("✓ API version: " + apiVersion);

        var correlationId = readCorrelationId(inputStream);
        LOGGER.info("✓ Correlation ID: " + correlationId);

        var clientIdLength = readClientIdLenght(inputStream);
        LOGGER.info("✓ Client ID length: " + clientIdLength);

        byte[] clientIdContents = readExactly(inputStream, clientIdLength);
        LOGGER.info("✓ Client ID: '" + new String(clientIdContents, StandardCharsets.UTF_8) + "'");

        // Skip tag buffer
        inputStream.skip(1);

        int arrayLength = readVarint(inputStream);
        LOGGER.info("✓ Topics array length: " + arrayLength);

        if (arrayLength == 0) {
            throw new IOException("Invalid topics array length");
        }
        int actualArrayLength = arrayLength - 1;

        var topics = new ArrayList<TopicInfo>();

        for (int i = 0; i < actualArrayLength; i++) {
            String topicName = readCompactString(inputStream);
            LOGGER.info("✓ Topic name: '" + topicName + "'");

            inputStream.skip(1); // Skip topic tag buffer

            if (topicName == null || topicName.isEmpty()) {
                throw new IOException("Invalid topic name at index " + i);
            }

            byte[] topicNameBytes = topicName.getBytes(StandardCharsets.UTF_8);
            topics.add(new TopicInfo((byte) topicNameBytes.length, topicNameBytes));
        }

        int responsePartitionLimit = readDescribeTopicPartitionLimit(inputStream);
        LOGGER.info("✓ Partition limit: " + responsePartitionLimit);

        byte[] cursor = readCompactBytes(inputStream);
        LOGGER.info("✓ Cursor: " + (cursor != null ? cursor.length + " bytes" : "null"));

        inputStream.skip(1); // Skip final tag buffer

        LOGGER.info("🎉 Request parsed successfully!");

        return new DescribeTopicRequest(msgSize, apiKey, apiVersion, correlationId,
                clientIdLength, clientIdContents,
                actualArrayLength, topics,
                responsePartitionLimit);
    }

    private byte[] readExactly(InputStream inputStream, int length) throws IOException {
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

    private KafkaRequest readApiVersionRequest(InputStream inputStream) throws IOException {
        int msgSize = readMsgRequest(inputStream);
        LOGGER.info("Read message size: " + msgSize);

        short apiKey = readApiKey(inputStream, false);
        short apiVersion = readApiVersion(inputStream);
        int correlationId = readCorrelationId(inputStream);

        int remainingBytes = msgSize - 8;
        if (remainingBytes > 0) {
            long skipped = inputStream.skip(remainingBytes);
            LOGGER.info("Skipped " + skipped + " bytes of remaining request data");
        }

        return new KafkaRequest(msgSize, apiKey, apiVersion, correlationId);
    }

    private ApiVersionResponseDTO createApiVersionResponse(KafkaRequest request) {
        short errorCode = validateApiVersion(request.apiVersion());
        LOGGER.info("Creating response with error code: " + errorCode);
        return new ApiVersionResponseDTO(request.correlationId(), errorCode);
    }

    private void sendResponse(OutputStream outputStream, IBufferByteDTO buffDTO) throws IOException {
        byte[] responseBytes = buffDTO.toByteBuffer().array();
        LOGGER.info("Sending response: " + responseBytes.length + " bytes");
        StringBuilder hexLog = new StringBuilder("Response bytes (first 16): ");
        for (int i = 0; i < Math.min(16, responseBytes.length); i++) {
            hexLog.append(String.format("%02x ", responseBytes[i]));
        }
        LOGGER.info(hexLog.toString());
        outputStream.write(responseBytes);
        outputStream.flush();

        LOGGER.info("Response sent and flushed successfully");
    }

    private short validateApiVersion(short apiVersion) {
        return isValidApiVersion(apiVersion) ? 0 : UNSUPPORTED_VERSION.getCode();
    }

    //TODO: refactor code...
    private void handleDescribeTopicPartition(Socket clientSocket) {
        try (clientSocket) {
            clientSocket.setSoTimeout(SOCKET_TIMEOUT);
            InputStream inputStream = clientSocket.getInputStream();
            OutputStream outputStream = clientSocket.getOutputStream();
            int requestCount = 0;
            while (!clientSocket.isClosed() && clientSocket.isConnected()) {
                try {
                    LOGGER.info("Waiting for request " + (requestCount + 1) + " from client: " + formatClientAddress(clientSocket));
                    var request = readDescribeTopicPartitionRequest(inputStream);
                    requestCount++;
                    LOGGER.info("Received request " + requestCount + " - API Key: " + request.apiKey() +
                            ", API Version: " + request.apiVersion() +
                            ", Correlation ID: " + request.correlationId());
                    var response = createDescribeTopicPartitionResponse(request);
                    sendResponse(outputStream, response);
                    LOGGER.info("Request " + requestCount + " processed successfully for correlation ID: " + request.correlationId());
                } catch (Exception e) {
                    LOGGER.info("Client disconnected after " + requestCount + " requests: " + formatClientAddress(clientSocket) + " - " + e.getMessage());
                    break;
                }
            }
        } catch (Exception e) {
            LOGGER.log(Level.WARNING, "Error processing client connection", e);
        }
    }

    private DescribeTopicDTO createDescribeTopicPartitionResponse(DescribeTopicRequest request) {
        //TODO: should add logic to verify if topic exists and return error code if not.
        final byte[] TOPIC_ID = "00000000-0000-0000-0000-000000000000".getBytes();
        var describeTopic = new DescribeTopicDTO(request.correlationId());
        for (TopicInfo topic : request.topics())
            describeTopic.addTopicResponse(new TopicResponseDTO(TOPIC_ID,
                    UNKNOWN_TOPIC_OR_PARTITION.getCode(), topic.topicNameLength(),
                    topic.topicName(), (byte) 0));
        return describeTopic;
    }
}