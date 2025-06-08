package web.util;

import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;

import static enums.REQUEST_DATA.*;

/*
 * This utility class provides methods to read various request components from an InputStream.l
 */
public class RequestReaderUtil {

    private RequestReaderUtil() throws IllegalAccessException {
        throw new IllegalAccessException("Utility class cannot be instantiated");
    }

    public static Integer readMsgRequest(InputStream inputStream) throws IOException {
        byte[] msgBuffer = new byte[MSG_SIZE.getSize()];
        inputStream.read(msgBuffer);
        return ByteBuffer.wrap(msgBuffer).getInt();
    }

    public static Integer readCorrelationId(InputStream inputStream) throws IOException {
        byte[] correlationIdBuffer = new byte[CORRELATION_ID_SIZE.getSize()];
        inputStream.read(correlationIdBuffer);
        return ByteBuffer.wrap(correlationIdBuffer).getInt();
    }

    public static short readApiKey(InputStream inputStream) throws IOException {
        byte[] apiKeyBuffer = new byte[REQUEST_API_KEY_SIZE.getSize()];
        inputStream.read(apiKeyBuffer);
        return ByteBuffer.wrap(apiKeyBuffer).getShort();
    }

    public static short readApiVersion(InputStream inputStream) throws IOException {
        byte[] apiVersionBuffer = new byte[REQUEST_API_VERSION_SIZE.getSize()];
        inputStream.read(apiVersionBuffer);
        return ByteBuffer.wrap(apiVersionBuffer).getShort();
    }
}
