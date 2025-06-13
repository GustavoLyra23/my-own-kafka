package core.models;

/**
 * Record ApiVersion represents the API version information for a Kafka request.
 * It contains apiKey, minVersion, and maxVersion fields that define the API version.
 */
public record ApiVersion(int apiKey, int minVersion, int maxVersion) {
}
