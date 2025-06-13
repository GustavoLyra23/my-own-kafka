package core.models;

/**
 * Represents information about a Kafka topic.
 *
 * @param topicNameLength The length of the topic name.
 * @param topicName       The name of the topic as a byte array.
 */
public record TopicInfo(byte topicNameLength, byte[] topicName) {
}
