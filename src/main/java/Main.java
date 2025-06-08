import models.Header;
import models.ProtocolMsg;

import java.io.IOException;
import java.io.InputStream;
import java.net.Socket;
import java.nio.ByteBuffer;

import static enums.REQUEST_DATA.*;
import static web.Server.closeClientConnection;
import static web.Server.startTCPServer;
import static web.readers.RequestReaderUtil.*;

public class Main {
    public static void main(String[] args) {
        Socket clientSocket = null;
        try {
            int port = 9092;
            clientSocket = startTCPServer(port);
            System.out.println("Server started on port " + port);

            var inputStream = clientSocket.getInputStream();
            var out = clientSocket.getOutputStream();

            // Lê message size (4 bytes)
            int messageSize = readMsgRequest(inputStream);

            // Lê api key (2 bytes)
            short apiKey = readApiKey(inputStream);

            // Lê api version (2 bytes)
            short apiVersion = readApiVersion(inputStream);

            // Lê correlation id (4 bytes)
            int correlationId = readCorrelationId(inputStream);

            System.err.println("Message Size: " + messageSize);
            System.err.println("API Key: " + apiKey);
            System.err.println("API Version: " + apiVersion);
            System.err.println("Correlation ID: " + correlationId);

            ByteBuffer response = ByteBuffer.allocate(8);
            response.putInt(0);
            response.putInt(correlationId);
            out.write(response.array());
        } catch (Exception e) {
            System.out.println("Exception: " + e.getMessage());
        } finally {
            closeClientConnection(clientSocket);
        }
    }
}