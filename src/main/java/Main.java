
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

            // APIVersions v3+ response format with tagged fields:
            // Message Length (4 bytes)
            // Correlation ID (4 bytes)
            // Error Code (2 bytes)
            // API Versions Array Length as compact array (variable bytes)
            // For each API version:
            //   - API Key (2 bytes)
            //   - Min Version (2 bytes)
            //   - Max Version (2 bytes)
            //   - Tagged Fields (1 byte = 0 for empty)
            // Throttle Time (4 bytes)
            // Tagged Fields (1 byte = 0 for empty)

            // Calculate response body size
            int responseBodySize = 4 + 2 + 1 + 6 + 1 + 4 + 1; // correlation_id + error_code + compact_array_length + api_version_entry + tagged_fields + throttle_time + tagged_fields

            ByteBuffer response = ByteBuffer.allocate(4 + responseBodySize);

            // Write message length
            response.putInt(responseBodySize);

            // Write correlation ID
            response.putInt(correlationId);

            // Write error code
            response.putShort(errorCode);

            // Write API versions array length (compact format: length + 1)
            response.put((byte) 2); // 1 entry + 1 = 2

            // Write API version entry for API_VERSIONS (key 18)
            response.putShort((short) 18); // API key for API_VERSIONS
            response.putShort((short) 0);  // min version
            response.putShort((short) 4);  // max version

            // Tagged fields for this API version entry (empty)
            response.put((byte) 0);

            // Throttle time (required in v3+)
            response.putInt(0);

            // Tagged fields for response body (empty)
            response.put((byte) 0);

            out.write(response.array());
            out.flush();

        } catch (Exception e) {
            System.out.println("Exception: " + e.getMessage());
        } finally {
            closeClientConnection(clientSocket);
        }
    }
}