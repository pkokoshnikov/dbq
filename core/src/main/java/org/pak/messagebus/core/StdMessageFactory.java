package org.pak.messagebus.core;

import java.time.Instant;
import java.util.Map;

public class StdMessageFactory implements MessageFactory {
    @Override
    public <T> Message<T> createMessage(String key, Instant originatedTime, T payload, Map<String, String> headers) {
        return new SimpleMessage<>(key, originatedTime, payload, headers);
    }
}
