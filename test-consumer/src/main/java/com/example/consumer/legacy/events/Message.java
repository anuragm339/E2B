package com.example.consumer.legacy.events;

import java.util.Objects;

public abstract class Message {
    private final String type;
    private final String key;

    public Message(String type, String key) {
        this.type = type;
        this.key = key;
    }

    public String getType() {
        return type;
    }

    public String getKey() {
        return key;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        Message message = (Message) o;
        return Objects.equals(type, message.type) &&
               Objects.equals(key, message.key);
    }

    @Override
    public int hashCode() {
        return Objects.hash(type, key);
    }
}
