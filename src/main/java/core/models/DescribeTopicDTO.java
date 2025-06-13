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
     * Varint representation of -1 used to indicate a null cursor in the response.
     */
    private static final byte NULL_CURSOR = (byte) 0xFF;

    /**
     * Size of the topic ID field in bytes (UUID representation).
     */
    private static final int TOPIC_ID_SIZE = 16;

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
     * @throws NullPointerException if request is null
     */
    public static DescribeTopicDTO createDescribeTopicPartitionsResponse(DescribeTopicRequest request) {
        LOGGER.info("Creating DescribeTopicPartitions Response...");
        LOGGER.info("Request correlation ID: " + request.correlationId());
        LOGGER.info("Number of topics in request: " + request.topics().size());

        final byte[] topicId = new byte[TOPIC_ID_SIZE];
        DescribeTopicDTO response = new DescribeTopicDTO(request.correlationId());

        for (int i = 0; i < request.topics().size(); i++) {
            TopicInfo topic = request.topics().get(i);
            String topicName = new String(topic.topicName());
            LOGGER.info("Processing topic " + (i + 1) + ": '" + topicName + "'");

            TopicResponseDTO topicResponse = new TopicResponseDTO(
                    topicId,
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
     *   <li>1 byte: next cursor (0xFF for null)</li>
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
        int bodySize = calculateBodySize();
        int totalSize = 4 + bodySize;

        LOGGER.fine("Creating DescribeTopicPartitions response buffer with size: " + totalSize + " bytes");

        ByteBuffer buffer = ByteBuffer.allocate(totalSize);

        writeHeader(buffer, bodySize);
        writeTopicsArray(buffer);
        writeFooter(buffer);

        buffer.flip();
        return buffer;
    }

    /**
     * Calculates the total size in bytes of the response body for buffer allocation.
     * This method computes the exact number of bytes needed to serialize all response
     * data according to the Kafka protocol specification.
     *
     * @return the calculated body size in bytes
     */
    private int calculateBodySize() {
        // Fixed fields: correlation_id + header_tag + throttle_time + topics_length
        int bodySize = 4 + 1 + 4 + 1;
        // Variable fields for each topic
        for (TopicResponseDTO topic : topicList) {
            bodySize += 2; // error_code
            bodySize += 1 + topic.getTopicName().length; // compact string (length + content)
            bodySize += TOPIC_ID_SIZE; // topic_id
            bodySize += 1; // is_internal
            bodySize += 1; // partitions (empty compact array)
            bodySize += 4; // authorized_operations
            bodySize += 1; // topic_tag_buffer
        }
        // Footer fields: next_cursor + final_tag_buffer
        bodySize += 1 + 1;
        return bodySize;
    }

    /**
     * Writes the response header fields to the buffer.
     *
     * @param buffer   the ByteBuffer to write to
     * @param bodySize the calculated body size
     */
    private void writeHeader(ByteBuffer buffer, int bodySize) {
        buffer.putInt(bodySize);          // Message size
        buffer.putInt(correlationId);     // Correlation ID
        buffer.put((byte) 0);             // Response header tag buffer
        buffer.putInt(0);                 // Throttle time ms
    }

    /**
     * Writes the topics array to the buffer using compact array encoding.
     *
     * @param buffer the ByteBuffer to write to
     */
    private void writeTopicsArray(ByteBuffer buffer) {
        // Topics array length (compact array: actual_size + 1)
        buffer.put((byte) (topicList.size() + 1));

        for (TopicResponseDTO topic : topicList) {
            writeTopicData(buffer, topic);
        }
    }

    /**
     * Writes a single topic's data to the buffer.
     *
     * @param buffer the ByteBuffer to write to
     * @param topic  the topic response DTO to serialize
     */
    private void writeTopicData(ByteBuffer buffer, TopicResponseDTO topic) {
        // Error code
        buffer.putShort(topic.getErrorCode());

        // Topic name (compact string)
        byte[] nameBytes = topic.getTopicName();
        buffer.put((byte) (nameBytes.length + 1));
        buffer.put(nameBytes);

        // Topic ID (16 bytes, all zeros for default UUID)
        buffer.put(new byte[TOPIC_ID_SIZE]);

        // Is internal flag
        buffer.put((byte) 0);

        // Partitions (empty compact array)
        buffer.put((byte) 1);

        // Authorized operations
        buffer.putInt(0);

        // Topic tag buffer
        buffer.put((byte) 0);
    }

    /**
     * Writes the response footer fields to the buffer.
     *
     * @param buffer the ByteBuffer to write to
     */
    private void writeFooter(ByteBuffer buffer) {
        buffer.put(NULL_CURSOR);    // Next cursor (null represented as -1 varint)
        buffer.put((byte) 0);       // Final tag buffer
    }
}