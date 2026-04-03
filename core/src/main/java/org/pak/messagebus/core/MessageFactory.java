package org.pak.messagebus.core;

import java.time.Instant;
import java.util.Map;

public interface MessageFactory {
    default <T> Message<T> createMessage(String key, Instant originatedTime, T payload) {
        return createMessage(key, originatedTime, payload, Map.of());
    }

    <T> Message<T> createMessage(String key, Instant originatedTime, T payload, Map<String, String> headers);
}
