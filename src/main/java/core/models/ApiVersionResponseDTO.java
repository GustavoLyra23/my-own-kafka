package core.models;

import java.nio.BufferOverflowException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.logging.Level;
import java.util.logging.Logger;

import static enums.API_KEYS.API_VERSIONS;
import static enums.API_KEYS.DESCRIBE_TOPIC_PARTITION;

/**
 * Data Transfer Object (DTO) representing an API Version response in a network protocol.
 * This class encapsulates the response data for API version requests, including supported
 * API keys and their version ranges.
 *
 * <p>The response includes information about which APIs are supported by the server
 * and what versions of each API are available. This is typically used during the
 * initial handshake phase of a protocol to negotiate compatible API versions.</p>
 *
 * <p>This class extends {@link AbstractResponseDTO} and implements {@link IBufferByteDTO}
 * to provide binary serialization capabilities.</p>
 *
 * @author Your Name
 * @version 1.0
 * @see AbstractResponseDTO
 * @see IBufferByteDTO
 * @see ApiVersion
 */
public class ApiVersionResponseDTO extends AbstractResponseDTO implements IBufferByteDTO {

    /**
     * Logger instance for this class.
     */
    private static final Logger LOGGER = Logger.getLogger(ApiVersionResponseDTO.class.getName());

    /**
     * List of supported API versions. Each entry contains an API key and its
     * supported version range.
     */
    private final List<ApiVersion> apiVersions = new ArrayList<>();

    /**
     * Constructs a new ApiVersionResponseDTO with the specified correlation ID and error code.
     * Initializes the response with default supported API versions including API_VERSIONS
     * and DESCRIBE_TOPIC_PARTITION APIs.
     *
     * @param correlationId the correlation ID that matches this response to its request
     * @param errorCode the error code indicating the status of the request (0 for success)
     */
    public ApiVersionResponseDTO(int correlationId, short errorCode) {
        super(correlationId, errorCode);
        apiVersions.add(new ApiVersion(API_VERSIONS.getApiKey(), 0, 4));
        //DescribeTopicPartitions api key.
        apiVersions.add(new ApiVersion(DESCRIBE_TOPIC_PARTITION.getApiKey(), 0, 0));
    }

    /**
     * Converts this DTO to a ByteBuffer for network transmission.
     * The buffer contains the complete binary representation of the API version response
     * according to the protocol specification.
     *
     * <p>The buffer structure includes:</p>
     * <ul>
     *   <li>4 bytes: message body size</li>
     *   <li>4 bytes: correlation ID</li>
     *   <li>2 bytes: error code</li>
     *   <li>1 byte: number of API versions + 1</li>
     *   <li>For each API version: 7 bytes (API key, min version, max version, tag buffer)</li>
     *   <li>4 bytes: throttle time</li>
     *   <li>1 byte: response tag buffer</li>
     * </ul>
     *
     * @return a ByteBuffer containing the serialized response data, or an empty buffer if serialization fails
     * @throws BufferOverflowException if the calculated buffer size is insufficient (handled internally)
     */
    @Override
    public ByteBuffer toByteBuffer() {
        try {
            var buff = createBuffer();
            LOGGER.fine("Response buffer created successfully, total size: " + buff.capacity());
            return buff;
        } catch (BufferOverflowException e) {
            LOGGER.log(Level.SEVERE, "Buffer overflow when creating response", e);
            return ByteBuffer.allocate(0); // Return an empty buffer on error
        }
    }

    /**
     * Calculates the total size in bytes of the response body.
     * This calculation is used to determine the buffer size needed for serialization.
     *
     * <p>The body size includes:</p>
     * <ul>
     *   <li>4 bytes: correlation_id (response header)</li>
     *   <li>2 bytes: error_code</li>
     *   <li>1 byte: COMPACT_ARRAY length for api_keys</li>
     *   <li>(apiVersions.size() * 7) bytes: each ApiKey (2+2+2+1 bytes)</li>
     *   <li>4 bytes: throttle_time_ms</li>
     *   <li>1 byte: TAG_BUFFER for response</li>
     * </ul>
     *
     * @return the calculated body size in bytes
     */
    private int calculateBodySize() {
        // Body size calculation:
        // 4 bytes: correlation_id (response header)
        // 2 bytes: error_code
        // 1 byte: COMPACT_ARRAY length for api_keys
        // (apiVersions.size() * 7) bytes: each ApiKey (2+2+2+1 bytes)
        // 4 bytes: throttle_time_ms
        // 1 byte: TAG_BUFFER for response
        int size = 4 + 2 + 1 + (apiVersions.size() * 7) + 4 + 1;
        LOGGER.fine("Calculated body size: " + size);
        return size;
    }

    /**
     * Creates and populates a ByteBuffer with the serialized response data.
     * This method handles the actual binary serialization of all response fields
     * according to the protocol specification.
     *
     * <p>The serialization process:</p>
     * <ol>
     *   <li>Allocates a buffer with the calculated size</li>
     *   <li>Writes the body size as the first field</li>
     *   <li>Writes the correlation ID and error code</li>
     *   <li>Writes the API versions array with compact array encoding</li>
     *   <li>Writes the throttle time and tag buffer</li>
     * </ol>
     *
     * @return a populated ByteBuffer ready for network transmission
     * @throws BufferOverflowException if the buffer is too small for the data
     */
    private ByteBuffer createBuffer() {
        int bodySize = calculateBodySize();
        LOGGER.fine("Creating response buffer with body size: " + bodySize);

        var buff = ByteBuffer.allocate(4 + bodySize);

        buff.putInt(bodySize);

        buff.putInt(correlationId);

        buff.putShort(errorCode);

        buff.put((byte) (apiVersions.size() + 1));

        for (ApiVersion apiVersion : apiVersions) {
            buff.putShort((short) apiVersion.apiKey());
            buff.putShort((short) apiVersion.minVersion());
            buff.putShort((short) apiVersion.maxVersion());
            buff.put((byte) 0);
        }

        buff.putInt(throttleTime);
        buff.put((byte) 0);
        return buff;
    }
}