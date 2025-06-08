package enums;

public enum REQUEST_DATA {

    MSG_SIZE(4),
    REQUEST_API_KEY_SIZE(2),
    REQUEST_API_VERSION_SIZE(2),
    CORRELATION_ID_SIZE(4);

    private final int size;

    REQUEST_DATA(int size) {
        this.size = size;
    }

    public int getSize() {
        return size;
    }
}
