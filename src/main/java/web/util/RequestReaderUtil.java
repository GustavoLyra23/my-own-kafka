package web.util;

import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.logging.Level;
import java.util.logging.Logger;

import static enums.REQUEST_DATA.*;

/**
 * Utility class for reading various request components from InputStreams with guaranteed complete reads.
 *
 * <p>This class provides a comprehensive set of methods for safely reading binary data from InputStreams,
 * specifically designed for Kafka protocol parsing. All methods ensure complete reads to prevent stream
 * misalignment and provide robust error handling.</p>
 *
 * <p>The class includes methods for:</p>
 * <ul>
 *   <li>Safe reading of primitive data types (bytes, shorts, integers)</li>
 *   <li>Kafka protocol specific field reading</li>
 *   <li>Compact format data reading (strings, bytes, varints)</li>
 *   <li>Stream navigation utilities</li>
 * </ul>
 *
 * <p>All methods are thread-safe as they don't maintain any state and work directly with the provided
 * InputStream parameter.</p>
 */
public class RequestReaderUtil {

    /**
     * Logger instance for this utility class
     */
    private static final Logger LOGGER = Logger.getLogger(RequestReaderUtil.class.getName());

    /**
     * Size of message header to skip when reading API keys
     */
    private static final int SKIP_MSG_SIZE = 4;

    /**
     * Private constructor to prevent instantiation of this utility class.
     *
     * @throws IllegalAccessException always thrown to prevent instantiation
     */
    private RequestReaderUtil() throws IllegalAccessException {
        throw new IllegalAccessException("Utility class cannot be instantiated");
    }

    // ===== SAFE READING PRIMITIVES =====

    /**
     * Reads exactly the specified number of bytes from the input stream.
     *
     * <p>This method guarantees that all requested bytes are read or an IOException is thrown.
     * It handles partial reads by continuing to read until the complete buffer is filled.</p>
     *
     * @param inputStream the input stream to read from
     * @param length      the exact number of bytes to read
     * @return byte array containing exactly {@code length} bytes
     * @throws IOException if EOF is encountered before reading all bytes, or if an I/O error occurs
     */
    private static byte[] readExactly(InputStream inputStream, int length) throws IOException {
        LOGGER.log(Level.FINE, "Reading exactly {0} bytes from input stream", length);

        byte[] buffer = new byte[length];
        int totalRead = 0;

        while (totalRead < length) {
            int bytesRead = inputStream.read(buffer, totalRead, length - totalRead);
            if (bytesRead == -1) {
                LOGGER.log(Level.SEVERE, "EOF encountered after reading {0} of {1} bytes",
                        new Object[]{totalRead, length});
                throw new IOException("EOF while reading " + length + " bytes (got " + totalRead + ")");
            }
            totalRead += bytesRead;
            LOGGER.log(Level.FINEST, "Read {0} bytes, total: {1}/{2}",
                    new Object[]{bytesRead, totalRead, length});
        }

        LOGGER.log(Level.FINE, "Successfully read {0} bytes", length);
        return buffer;
    }

    /**
     * Safely reads a single byte from the input stream.
     *
     * @param inputStream the input stream to read from
     * @return the byte value read from the stream
     * @throws IOException if EOF is encountered or if an I/O error occurs
     */
    private static byte readSingleByte(InputStream inputStream) throws IOException {
        LOGGER.log(Level.FINEST, "Reading single byte from input stream");

        int b = inputStream.read();
        if (b == -1) {
            LOGGER.log(Level.SEVERE, "EOF encountered while reading single byte");
            throw new IOException("EOF while reading single byte");
        }

        LOGGER.log(Level.FINEST, "Read single byte: {0}", b);
        return (byte) b;
    }

    /**
     * Safely reads a 32-bit integer (4 bytes) from the input stream.
     *
     * <p>The integer is read in big-endian byte order as per Kafka protocol specification.</p>
     *
     * @param inputStream the input stream to read from
     * @return the 32-bit integer value
     * @throws IOException if EOF is encountered or if an I/O error occurs
     */
    public static int readInt32(InputStream inputStream) throws IOException {
        LOGGER.log(Level.FINE, "Reading 32-bit integer from input stream");

        byte[] buffer = readExactly(inputStream, 4);
        int value = ByteBuffer.wrap(buffer).getInt();

        LOGGER.log(Level.FINE, "Read 32-bit integer: {0}", value);
        return value;
    }

    /**
     * Safely reads a 16-bit short integer (2 bytes) from the input stream.
     *
     * <p>The short is read in big-endian byte order as per Kafka protocol specification.</p>
     *
     * @param inputStream the input stream to read from
     * @return the 16-bit short value
     * @throws IOException if EOF is encountered or if an I/O error occurs
     */
    public static short readInt16(InputStream inputStream) throws IOException {
        LOGGER.log(Level.FINE, "Reading 16-bit short from input stream");

        byte[] buffer = readExactly(inputStream, 2);
        short value = ByteBuffer.wrap(buffer).getShort();

        LOGGER.log(Level.FINE, "Read 16-bit short: {0}", value);
        return value;
    }

    // ===== KAFKA PROTOCOL SPECIFIC METHODS =====

    /**
     * Reads the message request size from the Kafka protocol stream.
     *
     * <p>This represents the total size of the request message in bytes.</p>
     *
     * @param inputStream the input stream to read from
     * @return the message request size as an Integer
     * @throws IOException if an I/O error occurs during reading
     */
    public static Integer readMsgRequest(InputStream inputStream) throws IOException {
        LOGGER.log(Level.FINE, "Reading message request size");
        Integer size = readInt32(inputStream);
        LOGGER.log(Level.FINE, "Message request size: {0}", size);
        return size;
    }

    /**
     * Reads the correlation ID from the Kafka protocol stream.
     *
     * <p>The correlation ID is used to match requests with their corresponding responses.</p>
     *
     * @param inputStream the input stream to read from
     * @return the correlation ID as an Integer
     * @throws IOException if an I/O error occurs during reading
     */
    public static Integer readCorrelationId(InputStream inputStream) throws IOException {
        LOGGER.log(Level.FINE, "Reading correlation ID");
        Integer correlationId = readInt32(inputStream);
        LOGGER.log(Level.FINE, "Correlation ID: {0}", correlationId);
        return correlationId;
    }

    /**
     * Reads the API key from the Kafka protocol stream.
     *
     * <p>The API key identifies the type of request being made. Optionally skips a specified
     * number of bytes before reading the API key.</p>
     *
     * @param inputStream the input stream to read from
     * @param skip        if true, skips {@link #SKIP_MSG_SIZE} bytes before reading
     * @return the API key as a short value
     * @throws IOException if an I/O error occurs during reading or skipping
     */
    public static short readApiKey(InputStream inputStream, boolean skip) throws IOException {
        LOGGER.log(Level.FINE, "Reading API key (skip: {0})", skip);

        if (skip) {
            long skipped = inputStream.skip(SKIP_MSG_SIZE);
            if (skipped != SKIP_MSG_SIZE) {
                LOGGER.log(Level.SEVERE, "Could not skip {0} bytes, only skipped {1}",
                        new Object[]{SKIP_MSG_SIZE, skipped});
                throw new IOException("Could not skip " + SKIP_MSG_SIZE + " bytes (skipped " + skipped + ")");
            }
            LOGGER.log(Level.FINE, "Skipped {0} bytes", SKIP_MSG_SIZE);
        }

        short apiKey = readInt16(inputStream);
        LOGGER.log(Level.FINE, "API key: {0}", apiKey);
        return apiKey;
    }

    /**
     * Reads the API version from the Kafka protocol stream.
     *
     * <p>The API version specifies which version of the API is being used for the request.</p>
     *
     * @param inputStream the input stream to read from
     * @return the API version as a short value
     * @throws IOException if an I/O error occurs during reading
     */
    public static short readApiVersion(InputStream inputStream) throws IOException {
        LOGGER.log(Level.FINE, "Reading API version");
        short version = readInt16(inputStream);
        LOGGER.log(Level.FINE, "API version: {0}", version);
        return version;
    }

    /**
     * Reads the client ID length from the Kafka protocol stream.
     *
     * <p>Note: Method name contains a typo ("Lenght" instead of "Length") but is preserved
     * for compatibility.</p>
     *
     * @param inputStream the input stream to read from
     * @return the client ID length as a short value
     * @throws IOException if an I/O error occurs during reading
     */
    public static short readClientIdLenght(InputStream inputStream) throws IOException {
        LOGGER.log(Level.FINE, "Reading client ID length");
        short length = readInt16(inputStream);
        LOGGER.log(Level.FINE, "Client ID length: {0}", length);
        return length;
    }

    /**
     * Reads the client ID contents from the Kafka protocol stream.
     *
     * <p>This method assumes the client ID is always a fixed size based on the
     * {@code CLIENTID_CONTENTS} enum value. In a real implementation, the length
     * should be read first, then the content.</p>
     *
     * @param inputStream the input stream to read from
     * @return byte array containing the client ID contents
     * @throws IOException if an I/O error occurs during reading
     */
    public static byte[] readClientIdContents(InputStream inputStream) throws IOException {
        LOGGER.log(Level.FINE, "Reading client ID contents");

        byte[] contents = readExactly(inputStream, CLIENTID_CONTENTS.getSize());
        LOGGER.log(Level.FINE, "Read client ID contents: {0} bytes", contents.length);
        return contents;
    }

    /**
     * Reads the topic array length from the Kafka protocol stream.
     *
     * @param inputStream the input stream to read from
     * @return the topic array length as a byte value
     * @throws IOException if an I/O error occurs during reading
     */
    public static byte readTopicArrayLength(InputStream inputStream) throws IOException {
        LOGGER.log(Level.FINE, "Reading topic array length");
        byte length = readSingleByte(inputStream);
        LOGGER.log(Level.FINE, "Topic array length: {0}", length);
        return length;
    }

    /**
     * Reads the topic name length from the Kafka protocol stream.
     *
     * @param inputStream the input stream to read from
     * @return the topic name length as a byte value
     * @throws IOException if an I/O error occurs during reading
     */
    public static byte readTopicNameLength(InputStream inputStream) throws IOException {
        LOGGER.log(Level.FINE, "Reading topic name length");
        byte length = readSingleByte(inputStream);
        LOGGER.log(Level.FINE, "Topic name length: {0}", length);
        return length;
    }

    /**
     * Reads the topic name from the Kafka protocol stream.
     *
     * <p>This method assumes a fixed size based on the {@code TOPIC_NAME_SIZE} enum value.
     * In a real implementation, the length should be read first, then the content.</p>
     *
     * @param inputStream the input stream to read from
     * @return byte array containing the topic name
     * @throws IOException if an I/O error occurs during reading
     */
    public static byte[] readTopicName(InputStream inputStream) throws IOException {
        LOGGER.log(Level.FINE, "Reading topic name");

        byte[] topicName = readExactly(inputStream, TOPIC_NAME_SIZE.getSize());
        LOGGER.log(Level.FINE, "Read topic name: {0} bytes", topicName.length);
        return topicName;
    }

    /**
     * Reads the describe topic partition limit from the Kafka protocol stream.
     *
     * @param inputStream the input stream to read from
     * @return the partition limit as an integer value
     * @throws IOException if an I/O error occurs during reading
     */
    public static int readDescribeTopicPartitionLimit(InputStream inputStream) throws IOException {
        LOGGER.log(Level.FINE, "Reading describe topic partition limit");
        int limit = readInt32(inputStream);
        LOGGER.log(Level.FINE, "Partition limit: {0}", limit);
        return limit;
    }

    // ===== COMPACT FORMAT METHODS =====

    /**
     * Reads a compact string from the Kafka protocol stream.
     *
     * <p>Compact strings are prefixed with a varint length field. A length of 0 indicates
     * a null string, while a length of 1 indicates an empty string.</p>
     *
     * @param inputStream the input stream to read from
     * @return the string value, or null if the length was 0
     * @throws IOException if an I/O error occurs during reading
     */
    public static String readCompactString(InputStream inputStream) throws IOException {
        LOGGER.log(Level.FINE, "Reading compact string");

        int length = readVarint(inputStream);
        LOGGER.log(Level.FINE, "Compact string length: {0}", length);

        if (length == 0) {
            LOGGER.log(Level.FINE, "Compact string is null");
            return null;
        }

        int actualLength = length - 1;

        if (actualLength == 0) {
            LOGGER.log(Level.FINE, "Compact string is empty");
            return "";
        }

        byte[] stringBytes = readExactly(inputStream, actualLength);
        String result = new String(stringBytes, StandardCharsets.UTF_8);
        LOGGER.log(Level.FINE, "Read compact string: \"{0}\"", result);
        return result;
    }

    /**
     * Reads a variable-length integer (varint) from the input stream.
     *
     * <p>Varints are encoded using a variable number of bytes where each byte contains
     * 7 bits of data and 1 continuation bit. The continuation bit indicates whether
     * more bytes follow.</p>
     *
     * @param inputStream the input stream to read from
     * @return the decoded integer value
     * @throws IOException if EOF is encountered, varint is too long, or an I/O error occurs
     */
    public static int readVarint(InputStream inputStream) throws IOException {
        LOGGER.log(Level.FINEST, "Reading varint");

        int result = 0;
        int shift = 0;

        while (true) {
            int b = inputStream.read();
            if (b == -1) {
                LOGGER.log(Level.SEVERE, "EOF encountered while reading varint");
                throw new IOException("EOF while reading varint");
            }

            result |= (b & 0x7F) << shift;

            if ((b & 0x80) == 0) {
                break;
            }

            shift += 7;
            if (shift >= 32) {
                LOGGER.log(Level.SEVERE, "Varint too long (shift >= 32)");
                throw new IOException("Varint too long");
            }
        }

        LOGGER.log(Level.FINEST, "Read varint: {0}", result);
        return result;
    }

    // ===== ADDITIONAL UTILITY METHODS =====

    /**
     * Safely skips exactly the specified number of bytes from the input stream.
     *
     * <p>This method ensures that the exact number of bytes is skipped, falling back
     * to reading and discarding bytes if the skip operation doesn't work as expected.</p>
     *
     * @param inputStream the input stream to skip bytes from
     * @param bytesToSkip the exact number of bytes to skip
     * @throws IOException if EOF is encountered before skipping all bytes, or if an I/O error occurs
     */
    public static void skipExactly(InputStream inputStream, int bytesToSkip) throws IOException {
        LOGGER.log(Level.FINE, "Skipping exactly {0} bytes", bytesToSkip);

        long totalSkipped = 0;
        while (totalSkipped < bytesToSkip) {
            long skipped = inputStream.skip(bytesToSkip - totalSkipped);
            if (skipped == 0) {
                // skip() returned 0, try reading and discarding instead
                LOGGER.log(Level.FINEST, "skip() returned 0, falling back to read and discard");
                int b = inputStream.read();
                if (b == -1) {
                    LOGGER.log(Level.SEVERE, "EOF encountered while skipping bytes");
                    throw new IOException("EOF while skipping bytes");
                }
                totalSkipped += 1;
            } else {
                totalSkipped += skipped;
                LOGGER.log(Level.FINEST, "Skipped {0} bytes, total: {1}/{2}",
                        new Object[]{skipped, totalSkipped, bytesToSkip});
            }
        }

        LOGGER.log(Level.FINE, "Successfully skipped {0} bytes", bytesToSkip);
    }

    /**
     * Reads a compact bytes field from the Kafka protocol stream.
     *
     * <p>Compact bytes are prefixed with a varint length field. A length of 0 indicates
     * null bytes, while a length of 1 indicates an empty byte array. This format is
     * commonly used for cursors and other binary data.</p>
     *
     * @param inputStream the input stream to read from
     * @return the byte array, or null if the length was 0
     * @throws IOException if an I/O error occurs during reading
     */
    public static byte[] readCompactBytes(InputStream inputStream) throws IOException {
        LOGGER.log(Level.FINE, "Reading compact bytes");

        int length = readVarint(inputStream);
        LOGGER.log(Level.FINE, "Compact bytes length: {0}", length);

        if (length == 0) {
            LOGGER.log(Level.FINE, "Compact bytes is null");
            return null;
        }

        int actualLength = length - 1;
        if (actualLength == 0) {
            LOGGER.log(Level.FINE, "Compact bytes is empty");
            return new byte[0];
        }

        byte[] result = readExactly(inputStream, actualLength);
        LOGGER.log(Level.FINE, "Read compact bytes: {0} bytes", result.length);
        return result;
    }
}