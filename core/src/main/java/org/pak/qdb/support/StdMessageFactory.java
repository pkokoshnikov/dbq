package org.pak.qdb.support;

import org.pak.qdb.api.Message;
import org.pak.qdb.spi.MessageFactory;

import java.time.Instant;
import java.util.Map;

public class StdMessageFactory implements MessageFactory {
    @Override
    public <T> Message<T> createMessage(String key, Instant originatedTime, T payload, Map<String, String> headers) {
        return new Message<>(key, originatedTime, payload, headers);
    }
}
