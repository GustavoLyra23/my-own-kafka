package core.models;

import enums.TOPIC_OPERATIONS;

import java.util.ArrayList;
import java.util.List;

public class TopicResponseDTO {
    private final byte[] topicId;
    //TODO: need to create an logic for topic operations...
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
