package core.models;

import java.nio.BufferOverflowException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;

public class ResponseDTO {

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
            return buff;
        } catch (BufferOverflowException e) {
            return ByteBuffer.allocate(0);
        }
    }


    private int calculateBodySize() {
        return 4 + 2 + 1 + (apiVersions.size() * 7) + 4 + 1;
    }
}


