package com.messaging.network.legacy.events;

import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Objects;

public class DataMessage extends Message {
    private final String contentType;
    private final String content;

    public DataMessage(String type, String key, String contentType, String content) {
        super(type, key);
        this.contentType = contentType;
        this.content = content;
    }

    public String getContentType() {
        return contentType;
    }

    public String getContent() {
        return content;
    }

    public void toOutputStream(DataOutputStream dos) throws IOException {
        dos.writeUTF(getType());
        dos.writeUTF(getKey());

        String outputContentType = this.contentType != null ? this.contentType : "application/json";
        dos.writeUTF(outputContentType);

        byte[] contents = this.content.getBytes(StandardCharsets.UTF_8);
        dos.writeInt(contents.length);
        dos.write(contents);
    }

    public int getContentLength() {
        return content.length();
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        if (!super.equals(o)) return false;
        DataMessage that = (DataMessage) o;
        return Objects.equals(contentType, that.contentType) &&
               Objects.equals(content, that.content);
    }

    @Override
    public int hashCode() {
        return Objects.hash(super.hashCode(), contentType, content);
    }
}
