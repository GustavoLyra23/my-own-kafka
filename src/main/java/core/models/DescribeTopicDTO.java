package core.models;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.logging.Logger;

import static enums.ERROR.UNKNOWN_TOPIC_OR_PARTITION;

/**
 * Data Transfer Object (DTO) representing a response to describe topics in Kafka protocol.
 * This class encapsulates the response data for DescribeTopicPartitions API requests,
 * containing details about topics including their names, error codes, partition information,
 * and other metadata.
 *
 * <p>The DescribeTopicPartitions API is used to retrieve metadata about specific topics,
 * including partition details, leadership information, and access permissions. This DTO
 * structures the response according to Kafka's binary protocol specification.</p>
 *
 * <p>This class implements {@link IBufferByteDTO} to provide binary serialization
 * capabilities for network transmission.</p>
 *
 * @author Your Name
 * @version 1.0
 * @see IBufferByteDTO
 * @see TopicResponseDTO
 * @see DescribeTopicRequest
 */
public class DescribeTopicDTO implements IBufferByteDTO {

    /**
     * Logger instance for this class to track response creation and serialization.
     */
    private static final Logger LOGGER = Logger.getLogger(DescribeTopicDTO.class.getName());

    /**
     * The correlation ID that matches this response to its corresponding request.
     * Used by clients to correlate asynchronous request-response pairs.
     */
    private final int correlationId;

    /**
     * List of topic response DTOs containing detailed information about each requested topic.
     * Each entry represents one topic's metadata including partitions, error codes, and permissions.
     */
    private final List<TopicResponseDTO> topicList = new ArrayList<>();

    /**
     * Constructs a new DescribeTopicDTO with the specified correlation ID.
     *
     * @param correlationId the correlation ID that matches this response to its request
     */
    public DescribeTopicDTO(int correlationId) {
        this.correlationId = correlationId;
    }

    /**
     * Adds a topic response to this DTO's topic list.
     * Each topic response contains metadata for a single topic including error codes,
     * partition information, and access permissions.
     *
     * @param topicResponse the topic response DTO to add to the response
     * @throws NullPointerException if topicResponse is null
     */
    public void addTopicResponse(TopicResponseDTO topicResponse) {
        topicList.add(topicResponse);
    }

    /**
     * Creates a complete DescribeTopicPartitions response from a given request.
     * This factory method processes each topic in the request and creates appropriate
     * response entries with default values and error codes.
     *
     * <p>For each topic in the request, this method:</p>
     * <ul>
     *   <li>Creates a TopicResponseDTO with a default empty UUID (16 zero bytes)</li>
     *   <li>Sets the error code to UNKNOWN_TOPIC_OR_PARTITION for all topics</li>
     *   <li>Preserves the original topic name and length information</li>
     *   <li>Logs the processing progress for debugging purposes</li>
     * </ul>
     *
     * @param request the DescribeTopicRequest containing the topics to describe
     * @return a new DescribeTopicDTO containing responses for all requested topics
     * @throws NullPointerException     if request is null
     * @throws IllegalArgumentException if request contains invalid topic information
     */
    public static DescribeTopicDTO createDescribeTopicPartitionsResponse(DescribeTopicRequest request) {
        LOGGER.info("Creating DescribeTopicPartitions Response...");
        LOGGER.info("Request correlation ID: " + request.correlationId());
        LOGGER.info("Number of topics in request: " + request.topics().size());
        // UUID default (empty) = 00000000-0000-0000-0000-000000000000
        final byte[] TOPIC_ID = new byte[16];

        DescribeTopicDTO response = new DescribeTopicDTO(request.correlationId());

        for (int i = 0; i < request.topics().size(); i++) {
            TopicInfo topic = request.topics().get(i);
            String topicName = new String(topic.topicName());
            LOGGER.info("Processing topic " + (i + 1) + ": '" + topicName + "'");
            TopicResponseDTO topicResponse = new TopicResponseDTO(
                    TOPIC_ID,
                    UNKNOWN_TOPIC_OR_PARTITION.getCode(),
                    topic.topicNameLength(),
                    topic.topicName(),
                    (byte) 0
            );
            response.addTopicResponse(topicResponse);
            LOGGER.info("Added topic response for: " + topicName + " with error code: " + UNKNOWN_TOPIC_OR_PARTITION.getCode());
        }
        LOGGER.info("Created response with " + request.topics().size() + " topics");
        return response;
    }

    /**
     * Converts this DTO to a ByteBuffer for network transmission according to Kafka's
     * DescribeTopicPartitions response format.
     *
     * <p>The binary format structure:</p>
     * <ul>
     *   <li>4 bytes: message body size</li>
     *   <li>4 bytes: correlation ID</li>
     *   <li>1 byte: response header tag buffer</li>
     *   <li>4 bytes: throttle time (milliseconds)</li>
     *   <li>1 byte: topics array length (compact array encoding)</li>
     *   <li>For each topic: variable bytes containing topic metadata</li>
     *   <li>1 byte: next cursor (null indicator)</li>
     *   <li>1 byte: final tag buffer</li>
     * </ul>
     *
     * <p>The buffer is flipped before return to prepare it for reading.</p>
     *
     * @return a ByteBuffer containing the serialized response data ready for transmission
     * @throws IllegalStateException if buffer allocation fails or serialization encounters an error
     */
    @Override
    public ByteBuffer toByteBuffer() {
        LOGGER.info("Creating DescribeTopicPartitions response for correlation ID: " + correlationId);
        int bodySize = getBodySize();
        int totalSize = 4 + bodySize; // message_size + body
        LOGGER.info("Calculated exact buffer size: " + totalSize + " bytes");
        ByteBuffer buffer = ByteBuffer.allocate(totalSize);
        // Message size
        buffer.putInt(bodySize);
        // Correlation ID
        buffer.putInt(correlationId);
        // Response header tag buffer
        buffer.put((byte) 0);
        // Throttle time ms
        buffer.putInt(0);

        // Topics array
        if (topicList.isEmpty()) {
            buffer.put((byte) 0);
        } else {
            buffer.put((byte) (topicList.size() + 1));
            listToBuff(buffer);
        }

        // Next cursor (null)
        buffer.put((byte) 0);

        // Final tag buffer
        buffer.put((byte) 0);
        buffer.flip();
        return buffer;
    }

    /**
     * Calculates the total size in bytes of the response body for buffer allocation.
     * This method computes the exact number of bytes needed to serialize all response
     * data according to the Kafka protocol specification.
     *
     * <p>The calculation includes:</p>
     * <ul>
     *   <li>Fixed header fields: correlation ID, tag buffers, throttle time</li>
     *   <li>Topics array length indicator</li>
     *   <li>For each topic: error code, name, ID, flags, partitions, operations, tags</li>
     *   <li>Cursor and final tag buffer</li>
     * </ul>
     *
     * <p>Topic names use compact string encoding (length + content bytes).</p>
     *
     * @return the calculated body size in bytes
     */
    private int getBodySize() {
        int bodySize = 4 + 1 + 4 + 1; // correlation_id + header_tag + throttle_time + topics_length

        for (TopicResponseDTO topic : topicList) {
            bodySize += 2; // error_code
            bodySize += 1 + topic.getTopicName().length; // compact string (length + content)
            bodySize += 16; // topic_id
            bodySize += 1; // is_internal
            bodySize += 1; // partitions (empty compact array)
            bodySize += 4; // authorized_operations
            bodySize += 1; // topic_tag_buffer
        }

        bodySize += 1; // next_cursor
        bodySize += 1; // final_tag_buffer
        return bodySize;
    }

    /**
     * Serializes the list of topics into the provided ByteBuffer using Kafka's binary format.
     * This method writes each topic's metadata in the order required by the protocol specification.
     *
     * <p>For each topic, the following fields are serialized:</p>
     * <ul>
     *   <li>2 bytes: error code (short)</li>
     *   <li>Variable bytes: topic name as compact string (length + content)</li>
     *   <li>16 bytes: topic ID (UUID as byte array, currently all zeros)</li>
     *   <li>1 byte: is_internal flag (always 0/false)</li>
     *   <li>1 byte: partitions array (empty compact array, value 1)</li>
     *   <li>4 bytes: authorized_operations (always 0)</li>
     *   <li>1 byte: topic tag buffer (always 0)</li>
     * </ul>
     *
     * @param buffer the ByteBuffer to write topic data into
     * @throws java.nio.BufferOverflowException if the buffer doesn't have enough remaining space
     * @throws NullPointerException             if buffer is null
     */
    private void listToBuff(ByteBuffer buffer) {
        for (TopicResponseDTO topic : topicList) {
            // Error code
            buffer.putShort(topic.getErrorCode());

            // Topic name (compact string)
            byte[] nameBytes = topic.getTopicName();
            buffer.put((byte) (nameBytes.length + 1));
            buffer.put(nameBytes);

            // Topic ID (16 bytes)
            buffer.put(topic.getTopicId());

            // Is internal
            buffer.put(topic.getIsInternal());

            // Partitions (empty compact array)
            buffer.put((byte) 1);

            // Authorized operations, TODO: check if this is correct...
            buffer.put(new byte[4]);

            // Topic tag buffer
//            buffer.put((byte) 0);
        }
    }
}