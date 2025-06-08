package enums;

public enum ERROR {
    UNSUPPORTED_VERSION((short) 35);

    private final short code;

    ERROR(short code) {
        this.code = code;
    }

    public short getCode() {
        return code;
    }
}
