import models.Header;
import models.ProtocolMsg;

import java.io.IOException;
import java.io.InputStream;
import java.io.PrintWriter;
import java.net.ServerSocket;
import java.net.Socket;
import java.nio.ByteBuffer;

import static enums.REQUEST_DATA.MSG_SIZE;
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

//            readApiKey(inputStream);
//            readApiVersion(inputStream);
            out.write(readMsgRequest(inputStream).byteValue() + readCorrelationId(inputStream).byteValue());


            //            var inputStream = clientSocket.getInputStream();
//            byte[] messageSizeBytes = new byte[4];
//            inputStream.read(messageSizeBytes);
//            int messageSize = ByteBuffer.wrap(messageSizeBytes).getInt();
//            byte[] correlationIdBytes = new byte[4];
//            inputStream.read(correlationIdBytes);
//            int correlationId = ByteBuffer.wrap(correlationIdBytes).getInt();
//            System.err.println("Message Size: " + messageSize);
//            System.err.println("Correlation ID: " + new byte[]{0, 0, 0, 0, 0,});
//            String response = "messageSize: " + messageSize + System.lineSeparator();
//            clientSocket.getOutputStream().write(new byte[]{0, 0, 0, 0, 0, 0, 0, 7});
        } catch (Exception e) {
            System.out.println("Exception: " + e.getMessage());
        } finally {
            closeClientConnection(clientSocket);
        }
    }
}
