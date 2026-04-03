package org.pak.qdb.model;

import org.pak.qdb.api.Message;

import java.time.Instant;
import java.util.Map;

public record SimpleMessage<T>(
        String key,
        Instant originatedTime,
        T payload,
        Map<String, String> headers
) implements Message<T> {
    public SimpleMessage {
        headers = Map.copyOf(headers);
    }

    public SimpleMessage(String key, Instant originatedTime, T payload) {
        this(key, originatedTime, payload, Map.of());
    }
}
