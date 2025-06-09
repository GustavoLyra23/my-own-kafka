package core.models;

import java.nio.ByteBuffer;

/**
 * Interface for Data Transfer Objects (DTOs) that can be converted to a ByteBuffer.
 * This is typically used for network communication where data needs to be serialized.
 */
public interface IBufferByteDTO {
    ByteBuffer toByteBuffer();
}
