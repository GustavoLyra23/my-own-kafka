package enums;

/**
 * Enum representing various topic operations in a Kafka-like system.
 * Each operation corresponds to a specific action that can be performed on topics.
 */
public enum TOPIC_OPERATIONS {
    READ,
    WRITE,
    CREATE,
    DELETE,
    ALTER,
    DESCRIBE,
    DESCRIBE_CONFIGS,
    ALTER_CONFIGS
}
