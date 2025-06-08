package models;

public class ProtocolMsg {

    private Integer messageSize;
    private Header header;
    //    private Object body;

    public ProtocolMsg(Integer messageSize, Header header) {
        this.messageSize = messageSize;
        this.header = header;
    }

    public Integer getMessageSize() {
        return messageSize;
    }

    public void setMessageSize(Integer messageSize) {
        this.messageSize = messageSize;
    }

    public Header getHeader() {
        return header;
    }

    public void setHeader(Header header) {
        this.header = header;
    }
}


