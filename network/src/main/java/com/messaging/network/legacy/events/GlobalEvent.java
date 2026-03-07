package com.messaging.network.legacy.events;

import java.io.DataOutputStream;
import java.io.IOException;
import java.util.List;
import java.util.Objects;

public abstract class GlobalEvent extends Event {
    private final String serviceName;

    GlobalEvent(EventType type, String serviceName) {
        super(type);
        if (serviceName == null) {
            this.serviceName = "";
        } else {
            this.serviceName = serviceName;
        }
    }

    public String getServiceName() {
        return serviceName;
    }

    @Override
    public Boolean isConsumable(String serviceName, List<String> messageTypes) {
        return this.serviceName.equals(serviceName);
    }

    @Override
    public void toWire(DataOutputStream dos) throws IOException {
        // No code - subclasses override if needed
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        if (!super.equals(o)) return false;
        GlobalEvent that = (GlobalEvent) o;
        return Objects.equals(serviceName, that.serviceName);
    }

    @Override
    public int hashCode() {
        return Objects.hash(super.hashCode(), serviceName);
    }
}
