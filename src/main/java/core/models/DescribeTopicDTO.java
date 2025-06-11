package core.models;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.logging.Logger;

/**
 * DescribeTopicDTO com estrutura correta incluindo tag buffers
 */
public class DescribeTopicDTO implements IBufferByteDTO {

    private static final Logger LOGGER = Logger.getLogger(DescribeTopicDTO.class.getName());
    private final int correlationId;
    private final List<TopicResponseDTO> topicList = new ArrayList<>();

    public DescribeTopicDTO(int correlationId) {
        this.correlationId = correlationId;
    }

    public void addTopicResponse(TopicResponseDTO topicResponse) {
        topicList.add(topicResponse);
    }

    @Override
    public ByteBuffer toByteBuffer() {
        LOGGER.info("Creating DescribeTopicPartitions response with proper structure");

        ByteBuffer buffer = ByteBuffer.allocate(512);

        // Message size placeholder
        int sizePos = buffer.position();
        buffer.putInt(0);
        int bodyStart = buffer.position();

        // === RESPONSE HEADER ===
        // Correlation ID
        buffer.putInt(correlationId);
        LOGGER.info("Added correlation ID: " + correlationId);

        // Tag buffer after correlation ID (para versão com tagged fields)
        buffer.put((byte) 0); // empty tag buffer
        LOGGER.info("Added response header tag buffer");

        // === RESPONSE BODY ===
        // Throttle time ms
        buffer.putInt(0);
        LOGGER.info("Added throttle time: 0");

        // Topics array
        if (topicList.isEmpty()) {
            buffer.put((byte) 0); // empty compact array
        } else {
            buffer.put((byte) (topicList.size() + 1)); // compact array length

            for (TopicResponseDTO topic : topicList) {
                String topicName = new String(topic.getTopicName());
                LOGGER.info("Adding topic: " + topicName + " with error: " + topic.getErrorCode());

                // Error code
                buffer.putShort(topic.getErrorCode());

                // Topic name (compact string)
                byte[] nameBytes = topic.getTopicName();
                buffer.put((byte) (nameBytes.length + 1));
                buffer.put(nameBytes);

                // Topic ID (16 bytes)
                buffer.put(new byte[16]);

                // Is internal
                buffer.put((byte) 0);

                // Partitions (empty compact array)
                buffer.put((byte) 1);

                // Topic authorized operations
                buffer.putInt(0);

                // Topic tag buffer
                buffer.put((byte) 0);
            }
        }

        // Next cursor (null)
        buffer.put((byte) 0);

        // Final tag buffer
        buffer.put((byte) 0);

        // Set message size
        int bodySize = buffer.position() - bodyStart;
        buffer.putInt(sizePos, bodySize);

        LOGGER.info("Response size: " + bodySize);

        buffer.flip();

        // Debug log
        byte[] bytes = new byte[buffer.remaining()];
        buffer.get(bytes);
        buffer.rewind();

        StringBuilder hex = new StringBuilder("Response hex:\n");
        for (int i = 0; i < Math.min(40, bytes.length); i++) {
            if (i % 16 == 0) hex.append(String.format("%04x | ", i));
            hex.append(String.format("%02x ", bytes[i] & 0xFF));
            if ((i + 1) % 16 == 0) hex.append("\n");
        }
        LOGGER.info(hex.toString());

        return buffer;
    }
}