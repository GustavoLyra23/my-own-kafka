package utils;

import core.models.SocketExecutor;

import java.net.Socket;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

/**
 * Utility class for managing a thread pool executor that handles socket requests in parallel.
 * This class uses virtual threads to efficiently manage concurrent socket processing.
 */
public class ThreadSocketPoolExecutor {

    /**
     * Java 21 Thread Pool
     */
    private static final ExecutorService threadPoolExecutor = Executors.newVirtualThreadPerTaskExecutor();

    /**
     * Submits a socket request to be processed in parallel using the provided SocketExecutor.
     * Uses a virtual thread for each request to optimize resource usage.
     *
     * @param socketExecutor The executor that processes the socket request.
     * @param clientSocket   The client socket to be processed.
     */
    public static void executeParallelTask(SocketExecutor socketExecutor, Socket clientSocket) {
        threadPoolExecutor.execute(() -> socketExecutor.processRequest(clientSocket));
    }
}
