
import models.Body;
import models.Header;
import models.ProtocolMsg;

import java.net.Socket;
import java.nio.ByteBuffer;

import static enums.ERROR.UNSUPPORTED_VERSION;
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
            int msgSize = readMsgRequest(inputStream);
            short apiKey = readApiKey(inputStream);
            short apiVersion = readApiVersion(inputStream);
            int correlationId = readCorrelationId(inputStream);

            short errorCode = 0;
            if (apiVersion < 0 || apiVersion > 4) {
                errorCode = UNSUPPORTED_VERSION.getCode();
            }

            // APIVersions response structure:
            // Message Length (4 bytes)
            // Correlation ID (4 bytes)
            // Error Code (2 bytes)
            // Array Length (4 bytes) - number of API versions
            // For each API version:
            //   - API Key (2 bytes)
            //   - Min Version (2 bytes)
            //   - Max Version (2 bytes)

            // Response body size calculation
            int responseBodySize = 4 + 2 + 4 + 6; // correlation_id + error_code + array_length + one_api_entry

            ByteBuffer response = ByteBuffer.allocate(4 + responseBodySize);

            // Write message length
            response.putInt(responseBodySize);

            // Write correlation ID
            response.putInt(correlationId);

            // Write error code
            response.putShort(errorCode);

            // Write array length (1 entry)
            response.putInt(1);

            // Write API version entry for API_VERSIONS (key 18)
            response.putShort((short) 18); // API key for API_VERSIONS
            response.putShort((short) 0);  // min version
            response.putShort((short) 4);  // max version

            out.write(response.array());
            out.flush();

        } catch (Exception e) {
            System.out.println("Exception: " + e.getMessage());
        } finally {
            closeClientConnection(clientSocket);
        }
    }
}