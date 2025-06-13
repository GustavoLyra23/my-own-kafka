
package core.models;

/**
 * Immutable record representing a Kafka protocol request header.
 * This record encapsulates the essential components of a Kafka request that are
 * present in all API calls, providing a structured way to handle request metadata.
 *
 * <p>The KafkaRequest contains the fundamental fields required for Kafka protocol
 * communication, including message size, API identification, versioning information,
 * and correlation tracking. These fields are used by the Kafka protocol to route
 * requests, ensure compatibility, and match responses to their corresponding requests.</p>
 *
 * <p>As a record, this class is immutable and automatically provides implementations
 * for equals(), hashCode(), toString(), and accessor methods for all components.</p>
 *
 * @author Your Name
 * @version 1.0
 * @since Java 17
 *
 * @param msgSize the total size of the request message in bytes, excluding the message size field itself
 * @param apiKey the API key identifying which Kafka API is being called (e.g., ApiVersions, DescribeTopicPartitions)
 * @param apiVersion the version of the API being used, determining the request/response format
 * @param correlationId a unique identifier used to correlate this request with its response in asynchronous communication
 */
public record KafkaRequest(int msgSize, short apiKey, short apiVersion, int correlationId) {
}