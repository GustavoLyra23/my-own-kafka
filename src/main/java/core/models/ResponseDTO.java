package core.models;

import java.nio.BufferOverflowException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.logging.Level;
import java.util.logging.Logger;

public class ResponseDTO {
    private static final Logger LOGGER = Logger.getLogger(ResponseDTO.class.getName());

    private final int correlationId;
    private final short errorCode;
    private final List<ApiVersion> apiVersions = new ArrayList<>();
    private final int throttleTime;

    public ResponseDTO(int correlationId, short errorCode) {
        this.correlationId = correlationId;
        this.errorCode = errorCode;
        this.throttleTime = 0;
        apiVersions.add(new ApiVersion(18, 0, 4));
        //DescribeTopicPartitions api key.
        apiVersions.add(new ApiVersion(75, 0, 0));
    }

    public ByteBuffer toByteBuffer() {
        try {
            var buff = createBuffer();
            LOGGER.fine("Response buffer created successfully, total size: " + buff.capacity());
            return buff;

        } catch (BufferOverflowException e) {
            LOGGER.log(Level.SEVERE, "Buffer overflow when creating response", e);
            return createBuffer();
        }
    }

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