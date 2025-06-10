package core.models;

import java.util.List;


/**
 * Represents a request to describe topics in Kafka.
 * This request includes details about the topics to be described.
 */
public record DescribeTopicRequest(
        int msgSize,
        short apiKey,
        short apiVersion,
        int correlationId,
        short clientIdLength,
        byte[] clientIdContents,
        int topicArrayLength,
        List<TopicInfo> topics,
        int responsePartitionLimit
) {
}
