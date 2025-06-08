package models;

public class ProtocolMsg {

    private Integer messageSize;
    private Header header;
    private Body body;

    public ProtocolMsg() {
    }

    public ProtocolMsg(Integer messageSize, Header header, Body body) {
        this.messageSize = messageSize;
        this.header = header;
        this.body = body;
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

    public Body getBody() {
        return body;
    }

    public void setBody(Body body) {
        this.body = body;
    }

    public static ProtocolMsgBuilder builder() {
        return new ProtocolMsgBuilder();
    }

    public static class ProtocolMsgBuilder {
        private Integer messageSize;
        private Header header;
        private Body body;

        ProtocolMsgBuilder() {
        }

        public ProtocolMsgBuilder messageSize(Integer messageSize) {
            this.messageSize = messageSize;
            return this;
        }

        public ProtocolMsgBuilder header(Header header) {
            this.header = header;
            return this;
        }

        public ProtocolMsgBuilder body(Body body) {
            this.body = body;
            return this;
        }

        public ProtocolMsg build() {
            return new ProtocolMsg(messageSize, header, body);
        }
    }
}


