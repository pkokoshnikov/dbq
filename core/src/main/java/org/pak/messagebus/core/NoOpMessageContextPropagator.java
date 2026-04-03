package org.pak.messagebus.core;

import java.util.Map;

public final class NoOpMessageContextPropagator implements MessageContextPropagator {
    private static final Scope NO_OP_SCOPE = () -> {
    };

    @Override
    public Map<String, String> injectCurrentContext(Map<String, String> headers) {
        return Map.copyOf(headers);
    }

    @Override
    public Scope extractToCurrentContext(Map<String, String> headers) {
        return NO_OP_SCOPE;
    }
}
