package web;

import core.models.*;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.SocketTimeoutException;
import java.util.ArrayList;
import java.util.logging.Level;
import java.util.logging.Logger;

import static enums.API_KEYS.*;
import static enums.ERROR.UNSUPPORTED_VERSION;
import static web.ThreadSocketPoolExecutor.executeTask;
import static web.util.RequestReaderUtil.*;

public class KafkaServer {
    private static final Logger LOGGER = Logger.getLogger(KafkaServer.class.getName());
    private static final int MIN_API_VERSION = 0;
    private static final int MAX_API_VERSION = 4;
    private final int port;
    private ServerSocket serverSocket;
    private static final int SOCKET_TIMEOUT = 1000;

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

    private void runServerLoop() {
        while (true) {
            try {
                Socket clientSocket = acceptClientConnection();
                var apiKey = readApiKey(clientSocket.getInputStream(), true);
                switch (apiKeyFromInt(apiKey)) {
                    case API_VERSIONS:
                        LOGGER.info("Received ApiVersions request from client: " + formatClientAddress(clientSocket));
                        executeTask(this::handleClientConnection, clientSocket);
                        break;
                    case DESCRIBE_TOPIC_PARTITION:
                        LOGGER.info("Received DescribeTopicPartitions request from client: " + formatClientAddress(clientSocket));
                        executeTask(this::handleDescribeTopicPartition, clientSocket);
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
        int msgSize = readMsgRequest(inputStream);
        var apiKey = readApiKey(inputStream, false);
        var apiVersion = readApiVersion(inputStream);
        var correlationId = readCorrelationId(inputStream);
        var clientIdLength = readClientIdLenght(inputStream);
        var clientIdContents = readClientIdContents(inputStream);
        //reading topics array..
        var readArrayLength = readTopicArrayLength(inputStream);
        var topics = new ArrayList<TopicInfo>();

        for (int i = 0; i < readArrayLength; i++) {
            var topicLength = readTopicNameLength(inputStream);
            var topicName = readTopicName(inputStream);
            topics.add(new TopicInfo(topicLength, topicName));
            //skipping tag buffer...
            inputStream.skip(1);
            LOGGER.info("Read topic name: " + new String(topicName));
        }

        var responsePartitionLimit = readDescribeTopicPartitionLimit(inputStream);
        LOGGER.info("Read message size: " + msgSize);
        return new DescribeTopicRequest(msgSize, apiKey, apiVersion, correlationId, clientIdLength, clientIdContents, readArrayLength, topics, responsePartitionLimit);

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
//                    LOGGER.info("Waiting for request " + (requestCount + 1) + " from client: " + formatClientAddress(clientSocket));
//                    KafkaRequest request = readApiVersionRequest(inputStream);
//                    requestCount++;
//                    LOGGER.info("Received request " + requestCount + " - API Key: " + request.apiKey() +
//                            ", API Version: " + request.apiVersion() +
//                            ", Correlation ID: " + request.correlationId());
//                    ApiVersionResponseDTO response = createResponse(request);
//                    sendResponse(outputStream, response);
//                    LOGGER.info("Request " + requestCount + " processed successfully for correlation ID: " + request.correlationId());
//                } catch (SocketTimeoutException e) {
//                    if (requestCount > 0) {
//                        LOGGER.info("Client finished sending requests (processed " + requestCount + " requests): " + formatClientAddress(clientSocket));
//                    } else {
//                        LOGGER.info("No data received from client (timeout): " + formatClientAddress(clientSocket));
//                    }
//                    break;
                    LOGGER.info("Waiting for request " + (requestCount + 1) + " from client: " + formatClientAddress(clientSocket));
                    var request = readDescribeTopicPartitionRequest(inputStream);
                    requestCount++;
                    LOGGER.info("Received request " + requestCount + " - API Key: " + request.apiKey() +
                            ", API Version: " + request.apiVersion() +
                            ", Correlation ID: " + request.correlationId());


                } catch (Exception e) {
                    LOGGER.info("Client disconnected after " + requestCount + " requests: " + formatClientAddress(clientSocket) + " - " + e.getMessage());
                    break;
                }
            }
        } catch (Exception e) {
            LOGGER.log(Level.WARNING, "Error processing client connection", e);
        }
    }
}