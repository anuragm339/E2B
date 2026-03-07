package com.messaging.network.legacy.events;

public class DeleteMessage extends Message {
    public DeleteMessage(String type, String key) {
        super(type, key);
    }

    public int getLength() {
        return 1;
    }
}
