package core.models;

public abstract class AbstractResponseDTO {

    protected int correlationId;
    protected short errorCode;
    protected final int throttleTime;

    protected AbstractResponseDTO(int correlationId, short errorCode) {
        this.correlationId = correlationId;
        this.errorCode = errorCode;
        this.throttleTime = 0;
    }
}
