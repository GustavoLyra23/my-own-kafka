package core.models;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.logging.Logger;

/**
 * Data Transfer Object (DTO) for describing a topic in a messaging system.
 * This class encapsulates the topic's ID, name, and authorized operations.
 */
public class DescribeTopicDTO implements IBufferByteDTO {

    private static final Logger LOGGER = Logger.getLogger(DescribeTopicDTO.class.getName());
    private final int correlationId;
    private final List<TopicResponseDTO> topicList = new ArrayList<>();

    public DescribeTopicDTO(int correlationId) {
        this.correlationId = correlationId;
    }

    public void addTopicResponse(TopicResponseDTO topicResponse) {
        topicList.add(topicResponse);
    }

    /**
     * Calcula o tamanho correto do corpo da resposta baseado no conteúdo real
     */
    private int calculateBodySize() {
        int size = 0;

        // Correlation ID (4 bytes)
        size += 4;

        // Throttle time (4 bytes) - mesmo que não usado, precisa estar presente
        size += 4;

        // Topics array length (compact format - 1 byte para arrays pequenos)
        size += 1;

        // Para cada tópico
        for (TopicResponseDTO topic : topicList) {
            // Error code (2 bytes)
            size += 2;

            // Topic name (compact string format)
            // 1 byte para length + tamanho real do nome
            size += 1 + topic.getTopicName().length;

            // Topic ID (16 bytes - UUID)
            size += 16;

            // Is internal (1 byte)
            size += 1;

            // Partitions array (compact format - vazio, então 1 byte)
            size += 1;

            // Topic authorized operations (4 bytes)
            size += 4;

            // Tag buffer (1 byte)
            size += 1;
        }

        // Next cursor (compact nullable bytes - null, então 1 byte)
        size += 1;

        // Final tag buffer (1 byte)
        size += 1;

        LOGGER.fine("Calculated body size: " + size + " for " + topicList.size() + " topics");
        return size;
    }

    @Override
    public ByteBuffer toByteBuffer() {
        int bodySize = calculateBodySize();
        LOGGER.fine("Creating DescribeTopicDTO buffer with body size: " + bodySize);

        ByteBuffer buff = ByteBuffer.allocate(4 + bodySize);

        // Message size (excluding this field itself)
        buff.putInt(bodySize);

        // Correlation ID
        buff.putInt(correlationId);

        // Throttle time ms (0 = no throttling)
        buff.putInt(0);

        // Topics array (compact format)
        buff.put((byte) (topicList.size() + 1)); // +1 for compact array format

        for (TopicResponseDTO topic : topicList) {
            // Error code
            buff.putShort(topic.getErrorCode());

            // Topic name (compact string)
            byte[] topicNameBytes = topic.getTopicName();
            buff.put((byte) (topicNameBytes.length + 1)); // +1 for compact string format
            buff.put(topicNameBytes);

            // Topic ID (16 bytes UUID - usando o valor que você já tem)
            byte[] topicId = topic.getTopicId();
            if (topicId.length >= 16) {
                // Se o topicId tem pelo menos 16 bytes, usa os primeiros 16
                buff.put(topicId, 0, 16);
            } else {
                // Se é menor, completa com zeros
                buff.put(topicId);
                for (int i = topicId.length; i < 16; i++) {
                    buff.put((byte) 0);
                }
            }

            // Is internal
            buff.put(topic.getIsInternal());

            // Partitions array (empty - compact format)
            buff.put((byte) 1); // empty compact array = 1

            // Topic authorized operations (0 = no operations)
            buff.putInt(0);

            // Topic tag buffer
            buff.put((byte) 0);
        }

        // Next cursor (null - compact nullable bytes)
        buff.put((byte) 0); // 0 = null

        // Final tag buffer
        buff.put((byte) 0);

        LOGGER.fine("Created response buffer with " + buff.position() + " bytes");
        return buff;
    }
}