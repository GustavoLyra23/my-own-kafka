package web;

import core.models.KafkaRequest;
import core.models.ResponseDTO;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.logging.Level;
import java.util.logging.Logger;

import static enums.ERROR.UNSUPPORTED_VERSION;
import static web.ThreadSocketPoolExecutor.executeTask;
import static web.util.RequestReaderUtil.*;

public class KafkaServer {
    private static final Logger LOGGER = Logger.getLogger(KafkaServer.class.getName());
    private static final int MIN_API_VERSION = 0;
    private static final int MAX_API_VERSION = 4;
    private final int port;
    private ServerSocket serverSocket;

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
                executeTask(this::handleClientRequest, clientSocket);
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

    private void handleClientRequest(Socket clientSocket) {
        try (clientSocket) {
            KafkaRequest request = readRequest(clientSocket.getInputStream());
            ResponseDTO response = createResponse(request);
            sendResponse(clientSocket.getOutputStream(), response);
            LOGGER.info("Request processed successfully for correlation ID: " + request.correlationId());
        } catch (Exception e) {
            LOGGER.log(Level.WARNING, "Error processing client request", e);
        }
    }

    private boolean isValidApiVersion(short apiVersion) {
        return apiVersion >= MIN_API_VERSION && apiVersion <= MAX_API_VERSION;
    }

    private KafkaRequest readRequest(InputStream inputStream) throws IOException {
        int msgSize = readMsgRequest(inputStream);
        short apiKey = readApiKey(inputStream);
        short apiVersion = readApiVersion(inputStream);
        int correlationId = readCorrelationId(inputStream);
        int remainingBytes = msgSize - 8;
        if (remainingBytes > 0) inputStream.skip(remainingBytes);
        return new KafkaRequest(msgSize, apiKey, apiVersion, correlationId);
    }


    private ResponseDTO createResponse(KafkaRequest request) {
        short errorCode = validateApiVersion(request.apiVersion());
        return new ResponseDTO(request.correlationId(), errorCode);
    }

    private void sendResponse(OutputStream outputStream, ResponseDTO response) throws IOException {
        byte[] responseBytes = response.toByteBuffer().array();
        outputStream.write(responseBytes);
        outputStream.flush();
    }

    private short validateApiVersion(short apiVersion) {
        return isValidApiVersion(apiVersion) ? 0 : UNSUPPORTED_VERSION.getCode();
    }
}
