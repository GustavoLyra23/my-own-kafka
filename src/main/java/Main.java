import models.Header;
import models.ProtocolMsg;

import java.io.IOException;
import java.io.PrintWriter;
import java.net.ServerSocket;
import java.net.Socket;

public class Main {
    public static void main(String[] args) {
        ServerSocket serverSocket;
        Socket clientSocket = null;
        int port = 9092;
        try {
            serverSocket = new ServerSocket(port);
            serverSocket.setReuseAddress(true);
            clientSocket = serverSocket.accept();
            PrintWriter out = new PrintWriter(clientSocket.getOutputStream(),
                    true);
            var protocolMsg = new ProtocolMsg(0, new Header(7));
            var stringRes = "messageSize: " + protocolMsg.getMessageSize() + System.lineSeparator();
            stringRes += "correlation_id: " + protocolMsg.getHeader().correlationId();
            out.println(stringRes);
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
