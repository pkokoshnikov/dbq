package org.pak.qdb.api;

import org.pak.qdb.model.SimpleMessage;

import java.time.Instant;
import java.util.Map;

public interface Message<T> {
    String key();

    Instant originatedTime();

    T payload();

    Map<String, String> headers();

    default Message<T> withHeaders(Map<String, String> headers) {
        return new SimpleMessage<>(key(), originatedTime(), payload(), headers);
    }
}
