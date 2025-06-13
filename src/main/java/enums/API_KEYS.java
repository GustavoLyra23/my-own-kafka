package enums;

/**
 * Enum representing api keys in a Kafka-like system.
 * Each key corresponds to a specific action that will be performed by the system.
 */
public enum API_KEYS {

    API_VERSIONS(18),
    DESCRIBE_TOPIC_PARTITION(75);

    private final int apiKey;


    API_KEYS(int apiKey) {
        this.apiKey = apiKey;
    }

    public static API_KEYS apiKeyFromInt(int apiKey) {
        for (API_KEYS key : API_KEYS.values()) {
            if (key.getApiKey() == apiKey) {
                return key;
            }
        }
        throw new IllegalArgumentException("Invalid API key: " + apiKey);
    }


    public int getApiKey() {
        return apiKey;
    }
}
