package com.messaging.network.legacy.events;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.List;

public class DataMessageEvent extends MessageEvent<DataMessage> {

    public DataMessageEvent(DataMessage message) {
        super(EventType.MESSAGE, message, NO_OFFSET, NO_PUBLISHED_TIME, 0, 0);
    }

    public DataMessageEvent(DataMessage message, long offset, long publishedTime, long size, long createTime) {
        super(EventType.MESSAGE, message, offset, publishedTime, size, createTime);
    }

    @Override
    public void toWire(DataOutputStream dos) throws IOException {
        getMessage().toOutputStream(dos);
    }

    public static DataMessageEvent from(DataInputStream dataInputStream) throws IOException {
        String type = dataInputStream.readUTF();
        String key = dataInputStream.readUTF();
        String contentType = dataInputStream.readUTF();

        int contentLength = dataInputStream.readInt();

        byte[] content = new byte[contentLength];
        dataInputStream.readFully(content);
        String contentStr = new String(content, StandardCharsets.UTF_8);

        DataMessage message = new DataMessage(type, key, contentType, contentStr);
        return new DataMessageEvent(message, NO_OFFSET, NO_PUBLISHED_TIME, 0, 0);
    }

    @Override
    public Class<? extends Event> getResponseEventType() {
        return AckEvent.class;
    }

    @Override
    public Boolean isConsumable(String serviceName, List<String> messageTypes) {
        return messageTypes.contains(payloadType());
    }
}
