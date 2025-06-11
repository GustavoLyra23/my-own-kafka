package core.models;

import enums.TOPIC_OPERATIONS;
import java.util.ArrayList;
import java.util.List;
import java.util.logging.Logger;

public class TopicResponseDTO {
    private static final Logger LOGGER = Logger.getLogger(TopicResponseDTO.class.getName());

    private final byte[] topicId;
    private final List<TOPIC_OPERATIONS> topicAuthorizedOperations = new ArrayList<>();
    private final short errorCode;
    private final byte topicLength;
    private final byte[] topicName;
    private final byte isInternal;

    public TopicResponseDTO(byte[] topicId, short errorCode, byte topicLength, byte[] topicName, byte isInternal) {
        this.topicId = topicId;
        this.errorCode = errorCode;
        this.topicLength = topicLength;
        this.topicName = topicName;
        this.isInternal = isInternal;

        // Log para debug
        LOGGER.fine("Created TopicResponseDTO:");
        LOGGER.fine("  Topic name: " + (topicName != null ? new String(topicName) : "null"));
        LOGGER.fine("  Topic length: " + topicLength);
        LOGGER.fine("  Error code: " + errorCode);
        LOGGER.fine("  Is internal: " + isInternal);
        LOGGER.fine("  Topic ID length: " + (topicId != null ? topicId.length : 0));
    }

    public byte[] getTopicId() {
        return topicId;
    }

    public List<TOPIC_OPERATIONS> getTopicAuthorizedOperations() {
        return topicAuthorizedOperations;
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