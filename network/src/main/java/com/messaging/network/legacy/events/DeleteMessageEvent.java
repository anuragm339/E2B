package com.messaging.network.legacy.events;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.util.List;

public class DeleteMessageEvent extends MessageEvent<DeleteMessage> {

    public DeleteMessageEvent(DeleteMessage message) {
        super(EventType.DELETE, message, NO_OFFSET, NO_PUBLISHED_TIME, 0, 0);
    }

    public DeleteMessageEvent(DeleteMessage message, long offset, long publishedTime, long size, long createTime) {
        super(EventType.DELETE, message, offset, publishedTime, size, createTime);
    }

    @Override
    public void toWire(DataOutputStream dos) throws IOException {
        dos.writeUTF(getMessage().getType());
        dos.writeUTF(getMessage().getKey());
    }

    public static DeleteMessageEvent from(DataInputStream dis) throws IOException {
        String type = dis.readUTF();
        String key = dis.readUTF();

        return new DeleteMessageEvent(new DeleteMessage(type, key), NO_OFFSET, NO_PUBLISHED_TIME, 0, 0);
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
