package org.pak.qdb.api;

import java.time.Instant;
import java.util.Map;

public record Message<T>(
        String key,
        Instant originatedTime,
        T payload,
        Map<String, String> headers
) {
    public Message {
        headers = Map.copyOf(headers);
    }

    public Message(String key, Instant originatedTime, T payload) {
        this(key, originatedTime, payload, Map.of());
    }

    public Message<T> withHeaders(Map<String, String> headers) {
        return new Message<>(key, originatedTime, payload, headers);
    }
}
