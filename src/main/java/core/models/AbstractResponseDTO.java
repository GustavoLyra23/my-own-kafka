package core.models;

/**
 * Abstract class representing a response DTO (Data Transfer Object) for Kafka requests.
 * It contains common fields such as correlationId, errorCode, and throttleTime.
 */
public abstract class AbstractResponseDTO {

    /**
     * The correlation ID is used to match responses with requests.
     */
    protected int correlationId;
    /**
     * The error code indicating the success or failure of the request.
     * If the request was successful, this will be 0.
     */
    protected short errorCode;
    /**
     * The time in milliseconds that the client should wait before retrying the request.
     */
    protected final int throttleTime;

    protected AbstractResponseDTO(int correlationId, short errorCode) {
        this.correlationId = correlationId;
        this.errorCode = errorCode;
        this.throttleTime = 0;
    }
}
