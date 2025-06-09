package core.models;

import enums.TOPIC_OPERATIONS;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;

/**
 * Data Transfer Object (DTO) for describing a topic in a messaging system.
 * This class encapsulates the topic's ID, name, and authorized operations.
 */
public class DescribeTopicDTO extends AbstractResponseDTO implements IBufferByteDTO {

    private final String topicId = "00000000-0000-0000-0000-000000000000";
    private final String topicName;
    private List<TOPIC_OPERATIONS> topicAuthorizedOperations = new ArrayList<>();

    protected DescribeTopicDTO(int correlationId, short errorCode, String topicName) {
        super(correlationId, errorCode);
        this.topicName = topicName;
    }

    public String getTopicId() {
        return topicId;
    }

    public void addTopicOperation(TOPIC_OPERATIONS operation) {
        if (!topicAuthorizedOperations.contains(operation)) topicAuthorizedOperations.add(operation);
    }

    public String getTopicName() {
        return topicName;
    }

    @Override
    public ByteBuffer toByteBuffer() {
        return null;
    }
}
