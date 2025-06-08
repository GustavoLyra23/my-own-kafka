package core.models;

public record KafkaRequest(int msgSize, short apiKey, short apiVersion, int correlationId) {
}
