package utils;

import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;

/**
 * Utility class for reading binary data from input streams in network request processing.
 * This class provides methods to read various data types from input streams, typically used
 * for parsing binary protocol messages.
 *
 * <p>All methods in this class are static and the class cannot be instantiated.</p>
 *
 * @author Your Name
 * @version 1.0
 */
public class ByteNetReader {

    /**
     * Private constructor to prevent instantiation of this utility class.
     */
    private ByteNetReader() {
    }

    /**
     * Reads a 32-bit integer from the input stream.
     *
     * @param inputStream the input stream to read from
     * @return the 32-bit integer value read from the stream
     * @throws IOException if an I/O error occurs or if unable to read exactly 4 bytes
     */
    public static int readInt32(InputStream inputStream) throws IOException {
        byte[] buffer = new byte[4];
        if (inputStream.read(buffer) != 4) {
            throw new IOException("Failed to read 4 bytes");
        }
        return ByteBuffer.wrap(buffer).getInt();
    }

    /**
     * Reads a 16-bit short integer from the input stream.
     *
     * @param inputStream the input stream to read from
     * @return the 16-bit short value read from the stream
     * @throws IOException if an I/O error occurs or if unable to read exactly 2 bytes
     */
    public static short readInt16(InputStream inputStream) throws IOException {
        byte[] buffer = new byte[2];
        if (inputStream.read(buffer) != 2) {
            throw new IOException("Failed to read 2 bytes");
        }
        return ByteBuffer.wrap(buffer).getShort();
    }

    /**
     * Reads a message request identifier from the input stream.
     * This is typically the first field in a protocol message.
     *
     * @param inputStream the input stream to read from
     * @return the message request ID as an Integer
     * @throws IOException if an I/O error occurs during reading
     */
    public static Integer readMsgRequest(InputStream inputStream) throws IOException {
        return readInt32(inputStream);
    }

    /**
     * Reads a correlation ID from the input stream.
     * Correlation IDs are used to match requests with their corresponding responses.
     *
     * @param inputStream the input stream to read from
     * @return the correlation ID as an Integer
     * @throws IOException if an I/O error occurs during reading
     */
    public static Integer readCorrelationId(InputStream inputStream) throws IOException {
        return readInt32(inputStream);
    }

    /**
     * Reads an API key from the input stream with optional skip functionality.
     *
     * @param inputStream the input stream to read from
     * @param skip        if true, skips 4 bytes before reading the API key
     * @return the API key as a short value
     * @throws IOException if an I/O error occurs during reading or skipping
     */
    public static short readApiKey(InputStream inputStream, boolean skip) throws IOException {
        if (skip) {
            inputStream.skip(4);
        }
        return readInt16(inputStream);
    }

    /**
     * Reads an API version from the input stream.
     * API versions are used to determine the protocol version being used.
     *
     * @param inputStream the input stream to read from
     * @return the API version as a short value
     * @throws IOException if an I/O error occurs during reading
     */
    public static short readApiVersion(InputStream inputStream) throws IOException {
        return readInt16(inputStream);
    }
}