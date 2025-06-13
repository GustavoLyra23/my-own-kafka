package core.models;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.logging.Logger;

import static enums.ERROR.UNKNOWN_TOPIC_OR_PARTITION;

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

    public static DescribeTopicDTO createDescribeTopicPartitionsResponse(DescribeTopicRequest request) {
        LOGGER.info("Creating DescribeTopicPartitions Response...");
        LOGGER.info("Request correlation ID: " + request.correlationId());
        LOGGER.info("Number of topics in request: " + request.topics().size());

        final byte[] TOPIC_ID = new byte[16];

        DescribeTopicDTO response = new DescribeTopicDTO(request.correlationId());

        for (int i = 0; i < request.topics().size(); i++) {
            TopicInfo topic = request.topics().get(i);
            String topicName = new String(topic.topicName());
            LOGGER.info("Processing topic " + (i + 1) + ": '" + topicName + "'");

            TopicResponseDTO topicResponse = new TopicResponseDTO(
                    TOPIC_ID,
                    UNKNOWN_TOPIC_OR_PARTITION.getCode(),
                    topic.topicNameLength(),
                    topic.topicName(),
                    (byte) 0
            );
            response.addTopicResponse(topicResponse);
            LOGGER.info("Added topic response for: " + topicName + " with error code: " + UNKNOWN_TOPIC_OR_PARTITION.getCode());
        }
        LOGGER.info("Created response with " + request.topics().size() + " topics");
        return response;
    }

    @Override
    public ByteBuffer toByteBuffer() {
        LOGGER.info("TESTING: Using -1 for null next_cursor");

        int bodySize = 4 + 1 + 4 + 1; // correlation + HEADER_TAG + throttle + topics_length

        for (TopicResponseDTO topic : topicList) {
            bodySize += 2; // error_code
            bodySize += 1 + topic.getTopicName().length; // compact string
            bodySize += 16; // topic_id
            bodySize += 1; // is_internal
            bodySize += 1; // partitions
            bodySize += 4; // authorized_operations
            bodySize += 1; // topic tag buffer
        }

        // Next cursor: -1 para null (pode ser o formato correto)
        bodySize += 1; // next_cursor como varint -1
        bodySize += 1; // final tag buffer

        int totalSize = 4 + bodySize;

        LOGGER.info("Using -1 for cursor: " + totalSize + " bytes");

        ByteBuffer buffer = ByteBuffer.allocate(totalSize);

        // Message size
        buffer.putInt(bodySize);

        // Correlation ID
        buffer.putInt(correlationId);

        // Response header tag buffer
        buffer.put((byte) 0);

        // Throttle time ms
        buffer.putInt(0);

        // Topics array
        buffer.put((byte) (topicList.size() + 1));

        for (TopicResponseDTO topic : topicList) {
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

            // Authorized operations
            buffer.putInt(0);

            // Topic tag buffer
            buffer.put((byte) 0);
        }

        // Next cursor: usar -1 como varint para representar null
        // Em varint, -1 é representado como 0xFF
        buffer.put((byte) 0xFF);

        // Final tag buffer
        buffer.put((byte) 0);

        buffer.flip();

        byte[] bytes = new byte[buffer.remaining()];
        buffer.get(bytes);
        buffer.rewind();

        StringBuilder hex = new StringBuilder("CURSOR -1 test:\n");
        for (int i = 0; i < bytes.length; i++) {
            if (i % 16 == 0) hex.append(String.format("%04x | ", i));
            hex.append(String.format("%02x ", bytes[i] & 0xFF));
            if ((i + 1) % 16 == 0) hex.append("\n");
        }
        LOGGER.info(hex.toString());

        LOGGER.info("✅ TESTING: Used 0xFF (-1) for null cursor");

        return buffer;
    }
}