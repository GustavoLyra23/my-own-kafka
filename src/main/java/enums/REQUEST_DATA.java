package enums;

/**
 * Enum representing the size in bytes of various components in a Kafka-like request.
 */
public enum REQUEST_DATA {

    MSG_SIZE(4),
    REQUEST_API_KEY_SIZE(2),
    REQUEST_API_VERSION_SIZE(2),
    CORRELATION_ID_SIZE(4),
    CLIENTID_LENGTH(2),
    CLIENTID_CONTENTS(9),
    TOPIC_NAME_LENGTH(1),
    TOPIC_NAME_SIZE(3),
    RESPONSE_PARTITION_LIMIT_SIZE(4),
    TOPIC_ARRAY_LENGTH(1);
    /**
     * represents the number of bytes for each request component.
     */
    private final int size;

    REQUEST_DATA(int size) {
        this.size = size;
    }

    public int getSize() {
        return size;
    }
}
