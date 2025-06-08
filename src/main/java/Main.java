import models.Header;
import models.ProtocolMsg;

import java.io.IOException;
import java.io.InputStream;
import java.io.PrintWriter;
import java.net.ServerSocket;
import java.net.Socket;
import java.nio.ByteBuffer;

public class Main {
    public static void main(String[] args) {
        ServerSocket serverSocket;
        Socket clientSocket = null;
        int port = 9092;
        try {
            serverSocket = new ServerSocket(port);
            serverSocket.setReuseAddress(true);
            clientSocket = serverSocket.accept();
            var inputStream = clientSocket.getInputStream();
            byte[] messageSizeBytes = new byte[4];
            inputStream.read(messageSizeBytes);
            int messageSize = ByteBuffer.wrap(messageSizeBytes).getInt();
            byte[] correlationIdBytes = new byte[4];
            inputStream.read(correlationIdBytes);
            int correlationId = ByteBuffer.wrap(correlationIdBytes).getInt();
            System.err.println("Message Size: " + messageSize);
            System.err.println("Correlation ID: " + new byte[]{0,0,0,0,0,});
            String response = "messageSize: " + messageSize + System.lineSeparator();
            clientSocket.getOutputStream().write(new byte[] {0, 0, 0, 0, 0, 0, 0, 7});
        } catch (IOException e) {
            System.out.println("IOException: " + e.getMessage());
        } finally {
            try {
                if (clientSocket != null) {
                    clientSocket.close();
                }
            } catch (IOException e) {
                System.out.println("IOException: " + e.getMessage());
            }
        }
    }
}
