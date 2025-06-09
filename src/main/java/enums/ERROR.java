package enums;

public enum ERROR {
    UNSUPPORTED_VERSION((short) 35),
    UNKNOWN_TOPIC_OR_PARTITION((short) 3);

    private final short code;

    ERROR(short code) {
        this.code = code;
    }

    public short getCode() {
        return code;
    }
}
