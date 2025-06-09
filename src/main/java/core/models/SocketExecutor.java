package core.models;

import java.net.Socket;

/**
 * Functional interface for processing requests from a Socket.
 */
@FunctionalInterface
public interface SocketExecutor {
    void processRequest(Socket client);
}
