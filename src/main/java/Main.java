import web.KafkaServer;

import java.io.IOException;
import java.util.logging.Logger;

public class Main {
    private static final int DEFAULT_PORT = 9092;
    private static final Logger LOGGER = Logger.getLogger(Main.class.getName());

    public static void main(String[] args) throws IOException {
        LOGGER.info("Starting main application");
        var kafkaServer = new KafkaServer(DEFAULT_PORT);
        kafkaServer.start();
    }
}