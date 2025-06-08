package core.models;

import java.net.Socket;

@FunctionalInterface
public interface SocketExecutor {
    void processRequest(Socket client);
}
