package core.models;

import enums.TOPIC_OPERATIONS;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.logging.Logger;

/**
 * Data Transfer Object (DTO) for describing a topic in a messaging system.
 * This class encapsulates the topic's ID, name, and authorized operations.
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

    private int calculateBodySize() {
        int size = 4 + 1 + 4 + 1 + (topicList.size() * 29) + 1 + 1;
        LOGGER.fine("Calculated body size: " + size);
        return size;
    }

    public void setData() {

    }

    @Override
    public ByteBuffer toByteBuffer() {
        int bodySize = calculateBodySize();
        LOGGER.fine("Creating DescribeTopicDTO buffer with body size: " + bodySize);
        var buff = ByteBuffer.allocate(4 + bodySize);
        buff.putInt(bodySize);
        buff.putInt(correlationId);
        buff.put((byte) 0);
        buff.put(new byte[4]);
        buff.put((byte) (topicList.size() + 1));
        for (TopicResponseDTO topic : topicList) {
            buff.putShort(topic.getErrorCode());
            buff.put(topic.getTopicLength());
            buff.put(topic.getTopicName());
            buff.put(topic.getTopicId());
            buff.put(topic.getIsInternal());
            buff.put((byte) 1);
            buff.put(new byte[4]);
            buff.put((byte) 0);
        }
        buff.put((byte) 0);
        buff.put((byte) 0);
        return buff;
    }
}
