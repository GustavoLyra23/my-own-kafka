package web;

import java.io.IOException;
import java.net.ServerSocket;
import java.net.Socket;

public class Server {

    public static Socket startTCPServer(int port) throws IOException {
        try (var serverSocket = new ServerSocket(port)) {
            serverSocket.setReuseAddress(true);
            return serverSocket.accept();
        } catch (IOException e) {
            System.err.println("Error starting server on port " + port + ": " + e.getMessage());
            throw e;
        }
    }

    public static void closeClientConnection(Socket clientSocket) {
        if (clientSocket != null && !clientSocket.isClosed()) {
            try {
                clientSocket.close();
            } catch (IOException e) {
                System.err.println("Error closing client connection: " + e.getMessage());
            }
        }
    }
}
