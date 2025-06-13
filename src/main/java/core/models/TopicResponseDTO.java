package core.models;

import enums.TOPIC_OPERATIONS;

import java.util.ArrayList;
import java.util.List;
import java.util.logging.Logger;

/**
 * Represents a response for a topic in Kafka.
 * This class contains details about the topic, including its ID, name, error code, and whether it is internal.
 */
public class TopicResponseDTO {
    private static final Logger LOGGER = Logger.getLogger(TopicResponseDTO.class.getName());

    private final byte[] topicId;
    private final List<TOPIC_OPERATIONS> topicAuthorizedOperations = new ArrayList<>();
    private final short errorCode;
    private final byte topicLength;
    private final byte[] topicName;
    private final byte isInternal;

    public TopicResponseDTO(byte[] topicId, short errorCode, byte topicLength, byte[] topicName, byte isInternal) {
        LOGGER.info("Creating topic response....");
        this.topicId = topicId;
        this.errorCode = errorCode;
        this.topicLength = topicLength;
        this.topicName = topicName;
        this.isInternal = isInternal;
    }

    public byte[] getTopicId() {
        return topicId;
    }

    public List<TOPIC_OPERATIONS> getTopicAuthorizedOperations() {
        return topicAuthorizedOperations;
    }

    /**
     * Adds a topic operation to the list of authorized operations for this topic.
     *
     * @param operation the operation to be added
     */
    public void addTopicAuthorizedOperation(TOPIC_OPERATIONS operation) {
        topicAuthorizedOperations.add(operation);
    }

    public short getErrorCode() {
        return errorCode;
    }

    public byte getTopicLength() {
        return topicLength;
    }

    public byte[] getTopicName() {
        return topicName;
    }

    public byte getIsInternal() {
        return isInternal;
    }
}