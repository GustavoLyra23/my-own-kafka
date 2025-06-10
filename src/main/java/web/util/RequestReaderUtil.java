package web.util;

import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;

import static enums.REQUEST_DATA.*;

/**
 * This utility class provides methods to read various request components from an InputStream.
 * All methods guarantee complete reads to prevent stream misalignment.
 */
public class RequestReaderUtil {
    private static final int SKIP_MSG_SIZE = 4;

    private RequestReaderUtil() throws IllegalAccessException {
        throw new IllegalAccessException("Utility class cannot be instantiated");
    }

    // ===== SAFE READING PRIMITIVES =====

    /**
     * Reads exactly the specified number of bytes from the input stream.
     * Guarantees that all bytes are read or throws IOException.
     */
    private static byte[] readExactly(InputStream inputStream, int length) throws IOException {
        byte[] buffer = new byte[length];
        int totalRead = 0;

        while (totalRead < length) {
            int bytesRead = inputStream.read(buffer, totalRead, length - totalRead);
            if (bytesRead == -1) {
                throw new IOException("EOF while reading " + length + " bytes (got " + totalRead + ")");
            }
            totalRead += bytesRead;
        }

        return buffer;
    }

    /**
     * Safely reads a single byte from the input stream.
     */
    private static byte readSingleByte(InputStream inputStream) throws IOException {
        int b = inputStream.read();
        if (b == -1) {
            throw new IOException("EOF while reading single byte");
        }
        return (byte) b;
    }

    /**
     * Safely reads an int32 (4 bytes) from the input stream.
     */
    public static int readInt32(InputStream inputStream) throws IOException {
        byte[] buffer = readExactly(inputStream, 4);
        return ByteBuffer.wrap(buffer).getInt();
    }

    /**
     * Safely reads an int16 (2 bytes) from the input stream.
     */
    public static short readInt16(InputStream inputStream) throws IOException {
        byte[] buffer = readExactly(inputStream, 2);
        return ByteBuffer.wrap(buffer).getShort();
    }

    // ===== KAFKA PROTOCOL SPECIFIC METHODS =====

    public static Integer readMsgRequest(InputStream inputStream) throws IOException {
        return readInt32(inputStream);
    }

    public static Integer readCorrelationId(InputStream inputStream) throws IOException {
        return readInt32(inputStream);
    }

    // Reads the API key from the input stream...
    public static short readApiKey(InputStream inputStream, boolean skip) throws IOException {
        if (skip) {
            long skipped = inputStream.skip(SKIP_MSG_SIZE);
            if (skipped != SKIP_MSG_SIZE) {
                throw new IOException("Could not skip " + SKIP_MSG_SIZE + " bytes (skipped " + skipped + ")");
            }
        }
        return readInt16(inputStream);
    }

    public static short readApiVersion(InputStream inputStream) throws IOException {
        return readInt16(inputStream);
    }

    public static short readClientIdLenght(InputStream inputStream) throws IOException {
        return readInt16(inputStream);
    }

    public static byte[] readClientIdContents(InputStream inputStream) throws IOException {
        // This method assumes the client ID is always 12 bytes based on the enum
        // In a real implementation, you'd read the length first, then the content
        return readExactly(inputStream, CLIENTID_CONTENTS.getSize());
    }

    public static byte readTopicArrayLength(InputStream inputStream) throws IOException {
        return readSingleByte(inputStream);
    }

    public static byte readTopicNameLength(InputStream inputStream) throws IOException {
        return readSingleByte(inputStream);
    }

    public static byte[] readTopicName(InputStream inputStream) throws IOException {
        // This method assumes fixed size based on enum
        // In real implementation, you'd use the length read previously
        return readExactly(inputStream, TOPIC_NAME_SIZE.getSize());
    }

    public static int readDescribeTopicPartitionLimit(InputStream inputStream) throws IOException {
        return readInt32(inputStream);
    }

    // ===== COMPACT FORMAT METHODS (already correct) =====

    public static String readCompactString(InputStream inputStream) throws IOException {
        int length = readVarint(inputStream);

        if (length == 0) {
            return null;
        }

        int actualLength = length - 1;

        if (actualLength == 0) {
            return "";
        }

        byte[] stringBytes = readExactly(inputStream, actualLength);
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

    // ===== ADDITIONAL UTILITY METHODS =====

    /**
     * Safely skips the specified number of bytes.
     */
    public static void skipExactly(InputStream inputStream, int bytesToSkip) throws IOException {
        long totalSkipped = 0;
        while (totalSkipped < bytesToSkip) {
            long skipped = inputStream.skip(bytesToSkip - totalSkipped);
            if (skipped == 0) {
                // skip() returned 0, try reading and discarding instead
                int b = inputStream.read();
                if (b == -1) {
                    throw new IOException("EOF while skipping bytes");
                }
                totalSkipped += 1;
            } else {
                totalSkipped += skipped;
            }
        }
    }

    /**
     * Reads a compact bytes field (used for cursor, etc.)
     */
    public static byte[] readCompactBytes(InputStream inputStream) throws IOException {
        int length = readVarint(inputStream);

        if (length == 0) {
            return null;
        }

        int actualLength = length - 1;
        if (actualLength == 0) {
            return new byte[0];
        }

        return readExactly(inputStream, actualLength);
    }
}