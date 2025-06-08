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
    }

    public ByteBuffer toByteBuffer() {
        try {
            int bodySize = calculateBodySize();
            LOGGER.fine("Creating response buffer with body size: " + bodySize);

            var buff = ByteBuffer.allocate(4 + bodySize);

            buff.putInt(bodySize);

            buff.putInt(correlationId);
            buff.putShort(errorCode);
            buff.putShort((short) apiVersions.size());

            for (ApiVersion apiVersion : apiVersions) {
                buff.putShort((short) apiVersion.apiKey());
                buff.putShort((short) apiVersion.minVersion());
                buff.putShort((short) apiVersion.maxVersion());
                buff.put((byte) 0);
            }

            buff.putInt(throttleTime);
            buff.put((byte) 0);

            LOGGER.fine("Response buffer created successfully, total size: " + buff.capacity());
            return buff;

        } catch (BufferOverflowException e) {
            LOGGER.log(Level.SEVERE, "Buffer overflow when creating response", e);

            int safeSize = 1024;
            var buff = ByteBuffer.allocate(safeSize);

            int bodySize = calculateBodySize();
            buff.putInt(bodySize);
            buff.putInt(correlationId);
            buff.putShort(errorCode);
            buff.putShort((short) apiVersions.size());

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

    private int calculateBodySize() {
        int size = 4 + 2 + 2 + (apiVersions.size() * 7) + 4 + 1;
        LOGGER.fine("Calculated body size: " + size);
        return size;
    }
}