package org.pak.dbq.internal.support;

import org.pak.dbq.api.Message;
import org.pak.dbq.spi.MessageFactory;

import java.time.Instant;
import java.util.Map;

public class SimpleMessageFactory implements MessageFactory {
    @Override
    public <T> Message<T> createMessage(String key, Instant originatedTime, T payload, Map<String, String> headers) {
        return new Message<>(key, originatedTime, payload, headers);
    }
}
