package com.messaging.network.legacy.events;

import java.io.DataInputStream;
import java.io.IOException;

public class EventFactory {
    private static final int END_OF_STREAM = -1;

    public static Event create(DataInputStream dataInputStream) throws IOException {
        int typeIndex = dataInputStream.read();
        if (typeIndex == END_OF_STREAM) {
            return EOFEvent.INSTANCE;
        }

        EventType type = EventType.get(typeIndex);
        return create(type, dataInputStream);
    }

    private static <T extends Event> T create(EventType type, DataInputStream dataInputStream) throws IOException {
        switch (type) {
            case REGISTER:
                return (T) RegisterEvent.from(dataInputStream);
            case MESSAGE:
                return (T) DataMessageEvent.from(dataInputStream);
            case DELETE:
                return (T) DeleteMessageEvent.from(dataInputStream);
            case RESET:
                return (T) ResetEvent.from(dataInputStream);
            case READY:
                return (T) ReadyEvent.from(dataInputStream);
            case ACK:
                return (T) AckEvent.from(dataInputStream);
            case BATCH:
                return (T) BatchEvent.from(dataInputStream);
            case EOF:
                return (T) EOFEvent.from(dataInputStream);
            default:
                throw new IllegalArgumentException("Unknown event type: " + type);
        }
    }
}
