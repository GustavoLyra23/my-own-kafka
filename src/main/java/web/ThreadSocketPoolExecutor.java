package web;

import core.models.SocketExecutor;

import java.net.Socket;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class ThreadSocketPoolExecutor {

    private static final ExecutorService threadPoolExecutor = Executors.newVirtualThreadPerTaskExecutor();

    public static void executeParallelTask(SocketExecutor socketExecutor, Socket clientSocket) {
        threadPoolExecutor.execute(() -> socketExecutor.processRequest(clientSocket));
    }
}
