package com.messaging.network.legacy.events;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.util.Objects;

public class RegisterEvent extends Event {
    private final int version;
    private final String clientId;

    public RegisterEvent(int version, String clientId) {
        super(EventType.REGISTER);
        this.version = version;
        this.clientId = clientId;
    }

    public static RegisterEvent from(DataInputStream dataInputStream) throws IOException {
        int version = dataInputStream.readInt();
        String clientId = dataInputStream.readUTF();
        return new RegisterEvent(version, clientId);
    }

    @Override
    public void toWire(DataOutputStream dos) throws IOException {
        dos.writeInt(version);
        dos.writeUTF(clientId);
    }

    public int getVersion() {
        return version;
    }

    public String getClientId() {
        return clientId;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        if (!super.equals(o)) return false;
        RegisterEvent that = (RegisterEvent) o;
        return version == that.version && Objects.equals(clientId, that.clientId);
    }

    @Override
    public int hashCode() {
        return Objects.hash(super.hashCode(), version, clientId);
    }
}
