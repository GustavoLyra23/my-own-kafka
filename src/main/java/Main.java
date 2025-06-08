import enums.ERROR;
import models.Body;
import models.Header;
import models.ProtocolMsg;

import java.io.IOException;
import java.io.InputStream;
import java.net.Socket;
import java.nio.ByteBuffer;

import static enums.ERROR.UNSUPPORTED_VERSION;
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
            // Lê api key (2 bytes)
            short apiKey = readApiKey(inputStream);
            // Lê api version (2 bytes)
            short apiVersion = readApiVersion(inputStream);
            short errorCode = 0;
            if (apiVersion < 0 || apiVersion > 4) errorCode
                    = UNSUPPORTED_VERSION.getCode();
            var protocolMsg = ProtocolMsg.builder()
                    .messageSize(readMsgRequest(inputStream))
                    .header(new Header(readCorrelationId(inputStream)))
                    .body(new Body(apiKey, apiVersion))
                    .build();
            ByteBuffer response = ByteBuffer.allocate(8);
            response.putInt(0);
            response.putInt(protocolMsg.getHeader().correlationId());
            response.putShort(errorCode);
            out.write(response.array());
        } catch (Exception e) {
            System.out.println("Exception: " + e.getMessage());
        } finally {
            closeClientConnection(clientSocket);
        }
    }
}