package web.util;

import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;

import static enums.REQUEST_DATA.*;

/*
 * This utility class provides methods to read various request components from an InputStream.l
 */
public class RequestReaderUtil {
    private static final int SKIP_MSG_SIZE = 4;

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

    // Reads the API key from the input stream...
    public static short readApiKey(InputStream inputStream, boolean skip) throws IOException {
        if (skip) inputStream.skip(SKIP_MSG_SIZE);
        byte[] apiKeyBuffer = new byte[REQUEST_API_KEY_SIZE.getSize()];
        inputStream.read(apiKeyBuffer);
        return ByteBuffer.wrap(apiKeyBuffer).getShort();
    }

    public static short readApiVersion(InputStream inputStream) throws IOException {
        byte[] apiVersionBuffer = new byte[REQUEST_API_VERSION_SIZE.getSize()];
        inputStream.read(apiVersionBuffer);
        return ByteBuffer.wrap(apiVersionBuffer).getShort();
    }

    public static short readClientIdLenght(InputStream inputStream) throws IOException {
        byte[] clientIdLengthBuff = new byte[CLIENTID_LENGTH.getSize()];
        inputStream.read(clientIdLengthBuff);
        return ByteBuffer.wrap(clientIdLengthBuff).getShort();
    }

    public static byte[] readClientIdContents(InputStream inputStream) throws IOException {
        byte[] clientIdContents = new byte[CLIENTID_CONTENTS.getSize()];
        inputStream.read(clientIdContents);
        return clientIdContents;
    }

    public static byte readTopicArrayLength(InputStream inputStream) throws IOException {
        byte[] topicArrayLengthBuff = new byte[TOPIC_ARRAY_LENGTH.getSize()];
        inputStream.read(topicArrayLengthBuff);
        return ByteBuffer.wrap(topicArrayLengthBuff).get();
    }


    public static byte readTopicNameLength(InputStream inputStream) throws IOException {
        byte[] topicNameLengthBuff = new byte[TOPIC_NAME_LENGTH.getSize()];
        inputStream.read(topicNameLengthBuff);
        return ByteBuffer.wrap(topicNameLengthBuff).get();
    }

    public static byte[] readTopicName(InputStream inputStream) throws IOException {
        byte[] topicNameBuff = new byte[TOPIC_NAME_SIZE.getSize()];
        inputStream.read(topicNameBuff);
        return topicNameBuff;
    }

    public static int readDescribeTopicPartitionLimit(InputStream inputStream) throws IOException {
        byte[] responsePartitionLimitBuff = new byte[RESPONSE_PARTITION_LIMIT_SIZE.getSize()];
        inputStream.read(responsePartitionLimitBuff);
        return ByteBuffer.wrap(responsePartitionLimitBuff).getInt();
    }


    public static String readCompactString(InputStream inputStream) throws IOException {
        int length = readVarint(inputStream);

        if (length == 0) {
            return null;
        }

        int actualLength = length - 1;

        if (actualLength == 0) {
            return "";
        }

        byte[] stringBytes = new byte[actualLength];
        int totalRead = 0;
        while (totalRead < actualLength) {
            int bytesRead = inputStream.read(stringBytes, totalRead, actualLength - totalRead);
            if (bytesRead == -1) {
                throw new IOException("EOF while reading compact string");
            }
            totalRead += bytesRead;
        }

        return new String(stringBytes, StandardCharsets.UTF_8);
    }

    public static int readVarint(InputStream inputStream) throws IOException {
        int result = 0;
        int shift = 0;

        while (true) {
            int b = inputStream.read();
            if (b == -1) {
                throw new IOException("EOF while reading varint");
            }

            result |= (b & 0x7F) << shift;

            if ((b & 0x80) == 0) {
                break;
            }

            shift += 7;
            if (shift >= 32) {
                throw new IOException("Varint too long");
            }
        }

        return result;
    }
}
